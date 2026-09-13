package db_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/novoapex/novoapex-backend-api/internal/db"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

func TestMigrations_EmbeddedAndOrdered(t *testing.T) {
	ms, err := db.Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}
	if len(ms) == 0 || ms[0].Version != "0001_baseline" {
		t.Fatalf("first migration = %+v, want 0001_baseline", ms)
	}
	for i := 1; i < len(ms); i++ {
		if ms[i-1].Version >= ms[i].Version {
			t.Errorf("migrations out of order: %s before %s", ms[i-1].Version, ms[i].Version)
		}
	}
}

func TestMigrate_FreshIdempotentConcurrentAndGuarded(t *testing.T) {
	if testing.Short() {
		t.Skip("needs docker")
	}
	ctx := t.Context()
	h, err := harness.Start(ctx)
	if err != nil {
		t.Skipf("docker harness unavailable: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })

	connect := func() *pgx.Conn {
		t.Helper()
		cfg, err := pgx.ParseConfig(h.PostgresDSN + "&schema=public")
		if err != nil {
			t.Fatal(err)
		}
		db.NormalizeRuntimeParams(cfg.RuntimeParams)
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("connect with Prisma-style ?schema= URL: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}
	quiet := slog.New(slog.DiscardHandler)
	all, _ := db.Migrations()

	// Several migrators racing on a fresh database (api/worker/migrate
	// containers booting together) apply each migration exactly once.
	var wg sync.WaitGroup
	results := make([][]string, 4)
	errs := make([]error, 4)
	for i := range results {
		conn := connect()
		wg.Add(1)
		go func(i int, conn *pgx.Conn) {
			defer wg.Done()
			results[i], errs[i] = db.Migrate(ctx, conn, quiet)
		}(i, conn)
	}
	wg.Wait()
	appliedTotal := 0
	for i, e := range errs {
		if e != nil {
			t.Fatalf("migrator %d: %v", i, e)
		}
		appliedTotal += len(results[i])
	}
	if appliedTotal != len(all) {
		t.Fatalf("applied %d migrations across racers, want exactly %d", appliedTotal, len(all))
	}

	conn := connect()
	var tz string
	if err := conn.QueryRow(ctx, `SHOW timezone`).Scan(&tz); err != nil || tz != "UTC" {
		t.Errorf("session timezone = %q (%v), want UTC", tz, err)
	}
	var recorded int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&recorded); err != nil || recorded != len(all) {
		t.Errorf("schema_migrations rows = %d (%v), want %d", recorded, err, len(all))
	}

	again, err := db.Migrate(ctx, conn, quiet)
	if err != nil || len(again) != 0 {
		t.Fatalf("re-run applied %v (%v), want nothing", again, err)
	}

	// A database migrated by a newer build is refused rather than silently used.
	if _, err := conn.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ('9999_from_the_future')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Migrate(ctx, conn, quiet); err == nil || !strings.Contains(err.Error(), "9999_from_the_future") {
		t.Fatalf("unknown applied version must be rejected, got %v", err)
	}
}
