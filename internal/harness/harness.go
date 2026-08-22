package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	PostgresUser     = "postgres"
	PostgresPassword = "postgres"
	PostgresDB       = "novoapex"
	RedisPassword    = "testpass"

	pgStartupTimeout = 2 * time.Minute
)

type Harness struct {
	PostgresDSN string
	RedisAddr   string

	pg    testcontainers.Container
	redis testcontainers.Container
}

func Start(ctx context.Context) (*Harness, error) {
	h := &Harness{}
	var err error

	h.pg, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "pgvector/pgvector:pg15",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     PostgresUser,
				"POSTGRES_PASSWORD": PostgresPassword,
				"POSTGRES_DB":       PostgresDB,
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(pgStartupTimeout),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}

	pgHost, err := h.pg.Host(ctx)
	if err != nil {
		h.Terminate(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("postgres host: %w", err)
	}
	pgPort, err := h.pg.MappedPort(ctx, "5432/tcp")
	if err != nil {
		h.Terminate(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("postgres port: %w", err)
	}
	h.PostgresDSN = fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable",
		PostgresUser, PostgresPassword, pgHost, pgPort.Port(), PostgresDB)

	h.redis, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			Cmd:          []string{"redis-server", "--requirepass", RedisPassword},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	if err != nil {
		h.Terminate(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("start redis: %w", err)
	}

	redisHost, err := h.redis.Host(ctx)
	if err != nil {
		h.Terminate(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("redis host: %w", err)
	}
	redisPort, err := h.redis.MappedPort(ctx, "6379/tcp")
	if err != nil {
		h.Terminate(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("redis port: %w", err)
	}
	h.RedisAddr = net.JoinHostPort(redisHost, redisPort.Port())

	return h, nil
}

func (h *Harness) Terminate(ctx context.Context) {
	const shutdownTimeout = 10 * time.Second
	if h.redis != nil {
		tctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
		_ = h.redis.Terminate(tctx)
		cancel()
	}
	if h.pg != nil {
		tctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
		_ = h.pg.Terminate(tctx)
		cancel()
	}
}

func NovoApexRepoDir() (string, error) {
	if v := os.Getenv("NOVOAPEX_REPO"); v != "" {
		if ok, err := repoExists(v); err == nil && ok {
			return filepath.Abs(v)
		}
		return "", fmt.Errorf("NOVOAPEX_REPO=%s does not contain prisma/schema.prisma", v)
	}
	candidates := []string{"../../../novoapex", "../../novoapex"}
	for _, c := range candidates {
		ok, err := repoExists(c)
		if err == nil && ok {
			return filepath.Abs(c)
		}
	}
	return "", errors.New("novoapex repo not found; set NOVOAPEX_REPO")
}

func repoExists(dir string) (bool, error) {
	fi, err := os.Stat(filepath.Join(dir, "prisma", "schema.prisma"))
	if err != nil {
		return false, err
	}
	return !fi.IsDir(), nil
}

func ApplyPrismaMigrations(ctx context.Context, repoDir, dsn string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	var out bytes.Buffer
	cmd := exec.CommandContext(cmdCtx, "npx", "prisma", "migrate", "deploy",
		"--schema", filepath.Join(repoDir, "prisma", "schema.prisma"))
	cmd.Dir = repoDir
	cmd.Env = append(os.Environ(), "DATABASE_URL="+dsn)
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("prisma migrate deploy: %w\n%s", err, out.String())
	}
	return nil
}
