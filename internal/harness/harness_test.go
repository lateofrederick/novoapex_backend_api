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

func applyBaseline(t *testing.T, dsn string) {
	t.Helper()
	if err := ApplyBaselineSchema(t.Context(), dsn); err != nil {
		t.Fatalf("apply baseline schema: %v", err)
	}
}

func TestBaselineSchemaAppliesCleanlyFromEmpty(t *testing.T) {
	h := startHarness(t)
	applyBaseline(t, h.PostgresDSN)

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, table := range []string{"businesses", "conversations", "orders", "customers", "products"} {
		var exists bool
		if err := db.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`, table,
		).Scan(&exists); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("expected table %s to exist after baseline apply", table)
		}
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
