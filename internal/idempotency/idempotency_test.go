package idempotency_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/idempotency"
)

func TestStore_ExactlyOnceClaims(t *testing.T) {
	ctx := t.Context()
	h, err := harness.Start(ctx)
	if err != nil {
		t.Skipf("docker harness unavailable: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })
	if err := harness.ApplyBaselineSchema(ctx, h.PostgresDSN); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, h.PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := idempotency.Store{Pool: pool}

	t.Run("record claims once; seen reflects the claim", func(t *testing.T) {
		if seen, err := store.Seen(ctx, "k1"); err != nil || seen {
			t.Fatalf("Seen before claim = %v, %v", seen, err)
		}
		if claimed, err := idempotency.Record(ctx, pool, "k1"); err != nil || !claimed {
			t.Fatalf("first Record = %v, %v", claimed, err)
		}
		if claimed, err := idempotency.Record(ctx, pool, "k1"); err != nil || claimed {
			t.Fatalf("second Record = %v, %v (want an idempotent no-op)", claimed, err)
		}
		if seen, err := store.Seen(ctx, "k1"); err != nil || !seen {
			t.Fatalf("Seen after claim = %v, %v", seen, err)
		}
	})

	t.Run("process-once skips duplicates and releases the claim when fn fails", func(t *testing.T) {
		boom := errors.New("enqueue failed")
		if processed, err := store.ProcessOnce(ctx, "k2", func(pgx.Tx) error { return boom }); !errors.Is(err, boom) || processed {
			t.Fatalf("failing fn = %v, %v", processed, err)
		}
		if seen, _ := store.Seen(ctx, "k2"); seen {
			t.Fatal("a failed process must not keep the claim, or the redelivery is lost")
		}
		if processed, err := store.ProcessOnce(ctx, "k2", func(pgx.Tx) error { return nil }); err != nil || !processed {
			t.Fatalf("redelivery = %v, %v", processed, err)
		}
		ran := false
		if processed, err := store.ProcessOnce(ctx, "k2", func(pgx.Tx) error { ran = true; return nil }); err != nil || processed || ran {
			t.Fatalf("duplicate = processed:%v ran:%v err:%v", processed, ran, err)
		}
	})

	t.Run("concurrent deliveries process exactly once", func(t *testing.T) {
		var runs atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = store.ProcessOnce(ctx, "k3", func(pgx.Tx) error { runs.Add(1); return nil })
			}()
		}
		wg.Wait()
		if n := runs.Load(); n != 1 {
			t.Fatalf("fn ran %d times, want 1", n)
		}
	})
}
