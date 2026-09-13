// Command migrate brings a database to the current Go-owned schema by applying
// every pending migration embedded from internal/db/migrations. Idempotent and
// safe to run concurrently: applied versions are tracked in schema_migrations
// under an advisory lock.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/novoapex/novoapex-backend-api/internal/db"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" && len(os.Args) > 1 {
		dsn = os.Args[1]
	}
	if dsn == "" {
		logger.Error("DATABASE_URL required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		logger.Error("parse DATABASE_URL", slog.Any("error", err))
		os.Exit(1)
	}
	db.NormalizeRuntimeParams(connCfg.RuntimeParams)
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		logger.Error("connect", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	applied, err := db.Migrate(ctx, conn, logger)
	if err != nil {
		logger.Error("migrate failed", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("migrate complete", slog.Int("applied", len(applied)), slog.Any("versions", applied))
}
