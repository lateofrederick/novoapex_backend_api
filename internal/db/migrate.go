package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Migrations are plain SQL files embedded from internal/db/migrations, named
// NNNN_description.sql and applied in lexical order. Each file runs in its own
// transaction together with its schema_migrations bookkeeping row, so a
// failing migration leaves no partial state. Applied files must never be
// edited — add a new file instead (and re-run `sqlc generate`, which reads the
// same directory).
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrationLockKey serialises concurrent migrators (api/worker/migrate
// containers starting together) through a session advisory lock.
const migrationLockKey int64 = 0x6e6f766f61706578 // "novoapex"

var migrationNameRe = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)

// Migration is one embedded schema migration.
type Migration struct {
	Version string // file name without .sql, e.g. "0001_baseline"
	SQL     string
}

// Migrations returns every embedded migration in apply order.
func Migrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("db: read embedded migrations: %w", err)
	}
	var out []Migration
	seen := map[string]string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := migrationNameRe.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("db: migration %q must be named NNNN_description.sql", e.Name())
		}
		if prev, dup := seen[m[1]]; dup {
			return nil, fmt.Errorf("db: migrations %q and %q share number %s", prev, e.Name(), m[1])
		}
		seen[m[1]] = e.Name()
		body, err := migrationFiles.ReadFile(path.Join("migrations", e.Name()))
		if err != nil {
			return nil, fmt.Errorf("db: read migration %s: %w", e.Name(), err)
		}
		out = append(out, Migration{Version: strings.TrimSuffix(e.Name(), ".sql"), SQL: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Migrate applies every pending embedded migration and returns the versions
// it applied (empty when the schema is already current). Safe to run
// concurrently from several processes and idempotent across restarts.
func Migrate(ctx context.Context, conn *pgx.Conn, logger *slog.Logger) ([]string, error) {
	if logger == nil {
		logger = slog.Default()
	}
	migrations, err := Migrations()
	if err != nil {
		return nil, err
	}

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		return nil, fmt.Errorf("db: acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`); err != nil {
		return nil, fmt.Errorf("db: ensure schema_migrations: %w", err)
	}

	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return nil, err
	}
	if err := checkKnownVersions(applied, migrations); err != nil {
		return nil, err
	}

	var done []string
	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		logger.Info("applying migration", slog.String("version", m.Version))
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, m.Version)
			return err
		})
		if err != nil {
			return done, fmt.Errorf("db: apply migration %s: %w", m.Version, err)
		}
		done = append(done, m.Version)
	}
	if len(done) == 0 {
		logger.Info("schema up to date", slog.String("version", migrations[len(migrations)-1].Version))
	}
	return done, nil
}

func appliedVersions(ctx context.Context, conn *pgx.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("db: read schema_migrations: %w", err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("db: read schema_migrations: %w", err)
	}
	out := make(map[string]bool, len(versions))
	for _, v := range versions {
		out[v] = true
	}
	return out, nil
}

// checkKnownVersions refuses to run a binary older than the database: a
// recorded version this build does not embed means the schema has moved past
// what this code understands.
func checkKnownVersions(applied map[string]bool, migrations []Migration) error {
	known := make(map[string]bool, len(migrations))
	for _, m := range migrations {
		known[m.Version] = true
	}
	var unknown []string
	for v := range applied {
		if !known[v] {
			unknown = append(unknown, v)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return errors.New("db: database has migrations this build does not know: " + strings.Join(unknown, ", "))
	}
	return nil
}
