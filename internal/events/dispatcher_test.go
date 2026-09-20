package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/events"
	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

type capturedJob struct {
	queue    string
	taskType string
	payload  []byte
}

type capturePublisher struct {
	jobs []capturedJob
	fail bool
}

func (p *capturePublisher) Enqueue(_ context.Context, q, taskType string, payload any, _ *queue.EnqueueOpts) error {
	if p.fail {
		return errors.New("publish boom")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	p.jobs = append(p.jobs, capturedJob{queue: q, taskType: taskType, payload: b})
	return nil
}

func evtStartDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := t.Context()
	env, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { env.Terminate(context.Background()) })
	if err := harness.ApplyBaselineSchema(ctx, env.PostgresDSN); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	pool, err := pgxpool.New(ctx, env.PostgresDSN)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func insertEvent(t *testing.T, pool *pgxpool.Pool, e events.Event) {
	t.Helper()
	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		_, err := events.Insert(context.Background(), tx, e)
		return err
	})
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}
}

func eventStatus(t *testing.T, pool *pgxpool.Pool, aggregateID string) (string, int) {
	t.Helper()
	var status string
	var attempts int
	if err := pool.QueryRow(context.Background(),
		`SELECT status::text, attempts FROM domain_events WHERE aggregate_id = $1`, aggregateID).
		Scan(&status, &attempts); err != nil {
		t.Fatalf("read event: %v", err)
	}
	return status, attempts
}

func TestDispatcherDrainPublishesAndMarks(t *testing.T) {
	pool := evtStartDB(t)
	pub := &capturePublisher{}
	d := &events.Dispatcher{
		Pool:      pool,
		Publisher: pub,
		Subscriptions: map[string][]events.Subscription{
			events.TypeOrderCreated: {
				{Queue: queue.QPaymentInit, TaskType: queue.TaskPaymentInit},
				{Queue: queue.QCRMMaterialiser, TaskType: queue.TaskProfileStats},
			},
		},
	}

	insertEvent(t, pool, events.Event{
		AggregateType: events.AggregateTypeOrder,
		AggregateID:   "order-1",
		Type:          events.TypeOrderCreated,
		Payload:       events.OrderCreated{OrderID: "order-1", TotalAmount: "51.00", Currency: "GHS"},
	})

	published, err := d.Drain(context.Background())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if published != 1 {
		t.Fatalf("published = %d, want 1", published)
	}
	if len(pub.jobs) != 2 {
		t.Fatalf("enqueued jobs = %d, want 2 (one per subscriber)", len(pub.jobs))
	}
	status, attempts := eventStatus(t, pool, "order-1")
	if status != "PUBLISHED" || attempts != 0 {
		t.Errorf("event status/attempts = %s/%d, want PUBLISHED/0", status, attempts)
	}
}

func TestDispatcherRetriesThenDead(t *testing.T) {
	pool := evtStartDB(t)
	pub := &capturePublisher{fail: true}
	d := &events.Dispatcher{
		Pool:      pool,
		Publisher: pub,
		Subscriptions: map[string][]events.Subscription{
			events.TypeOrderCreated: {{Queue: queue.QPaymentInit, TaskType: queue.TaskPaymentInit}},
		},
		MaxAttempts: 3,
	}

	insertEvent(t, pool, events.Event{
		AggregateType: events.AggregateTypeOrder,
		AggregateID:   "order-2",
		Type:          events.TypeOrderCreated,
		Payload:       events.OrderCreated{OrderID: "order-2"},
	})

	for i := 0; i < 3; i++ {
		if _, err := d.Drain(context.Background()); err != nil {
			t.Fatalf("Drain #%d: %v", i+1, err)
		}
		status, attempts := eventStatus(t, pool, "order-2")
		if i < 2 {
			if status != "PENDING" {
				t.Fatalf("drain #%d status = %s, want PENDING until max attempts", i+1, status)
			}
		}
		if attempts != i+1 {
			t.Fatalf("drain #%d attempts = %d, want %d", i+1, attempts, i+1)
		}
	}
	status, _ := eventStatus(t, pool, "order-2")
	if status != "DEAD" {
		t.Errorf("status after max attempts = %s, want DEAD", status)
	}
}

func TestDispatcherEventWithNoSubscribersStillPublished(t *testing.T) {
	pool := evtStartDB(t)
	pub := &capturePublisher{}
	d := &events.Dispatcher{Pool: pool, Publisher: pub, Subscriptions: map[string][]events.Subscription{}}

	insertEvent(t, pool, events.Event{
		AggregateType: events.AggregateTypeOrder,
		AggregateID:   "order-3",
		Type:          events.TypeOrderCancelled,
		Payload:       events.OrderCancelled{OrderID: "order-3", Reason: "expired"},
	})

	if _, err := d.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	status, _ := eventStatus(t, pool, "order-3")
	if status != "PUBLISHED" {
		t.Errorf("no-subscriber event status = %s, want PUBLISHED (never retried)", status)
	}
}

// F.28 — end-to-end: a committed producer NOTIFY wakes the dispatcher (no poll
// backstop needed) and the subscriber receives its job.
func TestDispatcherListenNotifyEndToEnd(t *testing.T) {
	pool := evtStartDB(t)
	pub := &capturePublisher{}
	d := &events.Dispatcher{
		Pool:      pool,
		Publisher: pub,
		Subscriptions: map[string][]events.Subscription{
			events.TypeOrderCreated: {{Queue: queue.QPaymentInit, TaskType: queue.TaskPaymentInit}},
		},
		PollInterval: time.Hour, // disable the backstop: only NOTIFY may publish
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	// Let Run LISTEN and finish its initial (empty) drain before the event
	// exists, so a timely publish can only come from the NOTIFY path.
	time.Sleep(300 * time.Millisecond)

	insertEvent(t, pool, events.Event{
		AggregateType: events.AggregateTypeOrder,
		AggregateID:   "order-notify",
		Type:          events.TypeOrderCreated,
		Payload:       events.OrderCreated{OrderID: "order-notify"},
	})

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("event was not published via NOTIFY within 5s")
		default:
		}
		if len(pub.jobs) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if len(pub.jobs) != 1 || pub.jobs[0].taskType != queue.TaskPaymentInit {
		t.Fatalf("published jobs = %+v, want exactly one TaskPaymentInit", pub.jobs)
	}
	status, _ := eventStatus(t, pool, "order-notify")
	if status != "PUBLISHED" {
		t.Errorf("event status = %s, want PUBLISHED", status)
	}
}
