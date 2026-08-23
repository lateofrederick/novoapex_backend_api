// Command migrate brings a fresh (or existing) database to the current Go-
// owned baseline schema. Idempotent: tracks applied versions in
// schema_migrations; re-runs are no-ops.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/novoapex/novoapex-backend-api/internal/db"
)

const baselineVersion = "0001_baseline"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		if len(os.Args) > 1 {
			dsn = os.Args[1]
		}
	}
	if dsn == "" {
		logger.Error("DATABASE_URL required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		logger.Error("connect", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := migrate(ctx, conn, logger); err != nil {
		logger.Error("migrate failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func migrate(ctx context.Context, conn *pgx.Conn, logger *slog.Logger) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	var applied bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
		baselineVersion).Scan(&applied); err != nil {
		return fmt.Errorf("check baseline: %w", err)
	}
	if applied {
		logger.Info("schema up to date", slog.String("version", baselineVersion))
		return tx.Commit(ctx)
	}

	logger.Info("applying baseline schema")
	if _, err := tx.Exec(ctx, string(db.BaselineSchema())); err != nil {
		return fmt.Errorf("apply baseline: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1)`, baselineVersion); err != nil {
		return fmt.Errorf("record baseline: %w", err)
	}

	return tx.Commit(ctx)
}
