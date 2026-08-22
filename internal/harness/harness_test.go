package harness

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const expectedMigrationCount = 14

func requireDocker(t *testing.T) {
	t.Helper()
	sock := os.Getenv("DOCKER_HOST")
	if sock == "" || !strings.HasPrefix(sock, "unix://") {
		sock = "unix:///var/run/docker.sock"
	}
	path := strings.TrimPrefix(sock, "unix://")

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Skipf("docker ping request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Skipf("docker daemon unavailable at %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
}

func startHarness(t *testing.T) *Harness {
	t.Helper()
	requireDocker(t)
	ctx := t.Context()
	h, err := Start(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })
	return h
}

func harnessRepoDir(t *testing.T) string {
	t.Helper()
	repo, err := NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	return repo
}

func applyMigrations(t *testing.T, dsn, repoDir string) {
	t.Helper()
	if err := ApplyPrismaMigrations(t.Context(), repoDir, dsn); err != nil {
		t.Fatalf("prisma migrate deploy: %v", err)
	}
}

func TestMigrationsApplyCleanlyFromEmpty(t *testing.T) {
	h := startHarness(t)
	repoDir := harnessRepoDir(t)
	applyMigrations(t, h.PostgresDSN, repoDir)

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var applied int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM _prisma_migrations WHERE finished_at IS NOT NULL`,
	).Scan(&applied); err != nil {
		t.Fatalf("query applied migrations: %v", err)
	}
	if applied != expectedMigrationCount {
		t.Errorf("applied migrations = %d, want %d", applied, expectedMigrationCount)
	}

	var failed int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM _prisma_migrations WHERE finished_at IS NULL OR logs IS NOT NULL`,
	).Scan(&failed); err != nil {
		t.Fatalf("query failed migrations: %v", err)
	}
	if failed != 0 {
		t.Errorf("failed/incomplete migrations = %d, want 0", failed)
	}
}

func TestMigrationsIdempotentOnRerun(t *testing.T) {
	h := startHarness(t)
	repoDir := harnessRepoDir(t)

	for i := 0; i < 2; i++ {
		applyMigrations(t, h.PostgresDSN, repoDir)
	}

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var applied int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM _prisma_migrations WHERE finished_at IS NOT NULL`,
	).Scan(&applied); err != nil {
		t.Fatalf("query applied migrations: %v", err)
	}
	if applied != expectedMigrationCount {
		t.Errorf("after re-run applied migrations = %d, want %d (re-run must be a no-op)", applied, expectedMigrationCount)
	}
}

func TestRedisAcceptsAuthedPing(t *testing.T) {
	h := startHarness(t)

	conn, err := net.DialTimeout("tcp", h.RedisAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial redis: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	payload := fmt.Sprintf(
		"*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n*1\r\n$4\r\nPING\r\n",
		len(RedisPassword), RedisPassword,
	)
	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Fatalf("write auth/ping: %v", err)
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	reply := string(buf[:n])
	if reply != "+OK\r\n+PONG\r\n" {
		t.Errorf("redis reply = %q, want %q", reply, "+OK\r\n+PONG\r\n")
	}
}
