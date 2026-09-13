// Package idempotency ports WebhooksService (apps/api/src/webhooks/
// webhooks.service.ts): webhook deliveries are made exactly-once by claiming
// their idempotency key in webhook_events (unique idempotency_key).
package idempotency

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Execer is satisfied by *pgxpool.Pool, *pgxpool.Conn and pgx.Tx.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Store claims idempotency keys in webhook_events.
type Store struct {
	Pool *pgxpool.Pool
}

// Seen ports checkIdempotencyKey: whether the key was already claimed. A
// cheap read that lets callers skip a write transaction for known duplicates;
// the claim itself (Record/ProcessOnce) remains the authoritative guard.
func (s Store) Seen(ctx context.Context, key string) (bool, error) {
	var seen bool
	if err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM webhook_events WHERE idempotency_key = $1)`, key).Scan(&seen); err != nil {
		return false, fmt.Errorf("idempotency: check %q: %w", key, err)
	}
	return seen, nil
}

// Record ports recordIdempotencyKey: claim key on db (a pool, connection or
// open transaction). It reports whether this call made the claim; an existing
// claim is not an error, so a later stage can re-record a key an earlier stage
// already claimed.
func Record(ctx context.Context, db Execer, key string) (bool, error) {
	tag, err := db.Exec(ctx,
		`INSERT INTO webhook_events (id, idempotency_key) VALUES ($1, $2)
		 ON CONFLICT (idempotency_key) DO NOTHING`, uuid.NewString(), key)
	if err != nil {
		return false, fmt.Errorf("idempotency: record %q: %w", key, err)
	}
	return tag.RowsAffected() == 1, nil
}

// ProcessOnce ports processWithIdempotency: claim key and run fn in the same
// transaction. It returns false without running fn when the key was already
// claimed. When fn fails the transaction rolls back, releasing the claim so a
// redelivery can process the event — the source kept the claim and lost the
// event in that case.
func (s Store) ProcessOnce(ctx context.Context, key string, fn func(tx pgx.Tx) error) (bool, error) {
	processed := false
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		claimed, err := Record(ctx, tx, key)
		if err != nil || !claimed {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		processed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return processed, nil
}
