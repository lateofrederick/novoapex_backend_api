package harness

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/novoapex/novoapex-backend-api/internal/db"
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

// ApplyBaselineSchema brings a fresh test database to the current schema by
// running the same embedded migrations (internal/db.Migrate) that cmd/migrate
// applies in production. It is the schema-bootstrap mechanism for every test
// in this repo.
func ApplyBaselineSchema(ctx context.Context, dsn string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	conn, err := pgx.Connect(cmdCtx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(cmdCtx) }()

	if _, err := db.Migrate(cmdCtx, conn, slog.New(slog.DiscardHandler)); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
