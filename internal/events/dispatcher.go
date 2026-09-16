// Package events
// The transactional-outbox dispatcher: publishes PENDING domain_events to
// their queue subscribers. Producers emit events inside their own transaction
// and signal via LISTEN/NOTIFY on commit (see events.Insert); this dispatcher
// wakes on the channel and drains, with a poll backstop so a missed
// notification can never strand an event.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// Subscription routes one event type to a target queue + task type.
type Subscription struct {
	Queue    string
	TaskType string
}

// Dispatcher publishes PENDING events. Consumers are at-least-once and must
// dedupe on their own natural keys, so re-delivery after a crash is safe.
type Dispatcher struct {
	Pool          *pgxpool.Pool
	Publisher     queue.Publisher
	Subscriptions map[string][]Subscription
	MaxAttempts   int // publish attempts before an event goes DEAD (0 = default)
	PollInterval  time.Duration
	BatchSize     int
}

const (
	defaultPollInterval = 5 * time.Second
	defaultMaxAttempts  = 5
	defaultBatchSize    = 100
)

func (d *Dispatcher) pollInterval() time.Duration {
	if d.PollInterval > 0 {
		return d.PollInterval
	}
	return defaultPollInterval
}

func (d *Dispatcher) maxAttempts() int {
	if d.MaxAttempts > 0 {
		return d.MaxAttempts
	}
	return defaultMaxAttempts
}

func (d *Dispatcher) batchSize() int {
	if d.BatchSize > 0 {
		return d.BatchSize
	}
	return defaultBatchSize
}

// Run blocks, LISTENing on the event channel and draining on every
// notification (plus a poll backstop) until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) error {
	conn, err := d.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("events: acquire notify connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+ChannelName); err != nil {
		return fmt.Errorf("events: listen %s: %w", ChannelName, err)
	}
	pgConn := conn.Conn()

	for {
		if _, err := d.Drain(ctx); err != nil {
			slog.Error("events: drain failed", "err", err)
		}

		// Wait for the next notification, bounded by the poll backstop so a
		// lost notification still gets drained on the next tick.
		waitCtx, cancel := context.WithTimeout(ctx, d.pollInterval())
		_, waitErr := pgConn.WaitForNotification(waitCtx)
		cancel()
		if waitErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Timeout: loop and drain again (backstop recovery).
		}
	}
}

// Drain publishes every currently-PENDING event to its subscribers, marking
// each PUBLISHED on success or recording a failed attempt (DEAD after the max).
// A single advisory lock makes the drain a singleton across worker replicas.
func (d *Dispatcher) Drain(ctx context.Context) (int, error) {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("events: begin drain tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('event-dispatcher'))`); err != nil {
		return 0, fmt.Errorf("events: advisory lock: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT id, event_type, payload::text, attempts
		  FROM domain_events
		 WHERE status = 'PENDING'
		 ORDER BY occurred_at
		 LIMIT $1`, d.batchSize())
	if err != nil {
		return 0, fmt.Errorf("events: query pending: %w", err)
	}

	type pending struct {
		id        string
		eventType string
		payload   string
		attempts  int
	}
	var events []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.eventType, &p.payload, &p.attempts); err != nil {
			rows.Close()
			return 0, fmt.Errorf("events: scan pending: %w", err)
		}
		events = append(events, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("events: iterate pending: %w", err)
	}
	rows.Close()

	if len(events) == 0 {
		return 0, tx.Commit(ctx)
	}

	published := 0
	for _, ev := range events {
		if err := d.publish(ctx, ev.eventType, ev.payload); err != nil {
			attempts := ev.attempts + 1
			status := "PENDING"
			if attempts >= d.maxAttempts() {
				status = "DEAD"
			}
			if _, uerr := tx.Exec(ctx,
				`UPDATE domain_events SET attempts = $2, last_error = $3, status = $4 WHERE id = $1`,
				ev.id, attempts, err.Error(), status); uerr != nil {
				return 0, fmt.Errorf("events: record failed publish: %w", uerr)
			}
			slog.Error("events: publish failed",
				"event", ev.eventType, "attempts", attempts, "status", status, "err", err)
			continue
		}
		if _, uerr := tx.Exec(ctx,
			`UPDATE domain_events SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP WHERE id = $1`,
			ev.id); uerr != nil {
			return 0, fmt.Errorf("events: mark published: %w", uerr)
		}
		published++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("events: commit drain: %w", err)
	}
	return published, nil
}

// publish fans one event out to every subscriber. An event with no subscribers
// is treated as handled (marked PUBLISHED), never retried.
func (d *Dispatcher) publish(ctx context.Context, eventType, payload string) error {
	subs := d.Subscriptions[eventType]
	if len(subs) == 0 {
		return nil
	}
	if d.Publisher == nil {
		return errors.New("events: dispatcher has no publisher")
	}
	v, err := decodePayload(payload)
	if err != nil {
		return fmt.Errorf("events: decode %s payload: %w", eventType, err)
	}
	for _, sub := range subs {
		if err := d.Publisher.Enqueue(ctx, sub.Queue, sub.TaskType, v, nil); err != nil {
			return fmt.Errorf("events: enqueue %s to %s: %w", eventType, sub.TaskType, err)
		}
	}
	return nil
}

// decodePayload round-trips the event payload through the JSON decoder with
// UseNumber so the value re-marshals exactly (no float64 coercion) when handed
// to Publisher.Enqueue.
func decodePayload(raw string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}
