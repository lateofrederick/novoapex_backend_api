package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

func TestS6RateLimitFixedWindow(t *testing.T) {
	h := s6Harness(t)
	l := newRateLimiter(h.RedisAddr, harness.RedisPassword, false, slog.Default())
	defer func() { _ = l.close() }()

	ctx := context.Background()
	const queue = "s6rl-window"
	const limit = 3
	window := time.Second

	for i := 0; i < limit; i++ {
		if _, ok, err := l.allow(ctx, queue, limit, window); err != nil || !ok {
			t.Fatalf("allow #%d: ok=%v err=%v", i+1, ok, err)
		}
	}

	wait, ok, err := l.allow(ctx, queue, limit, window)
	if err != nil || ok {
		t.Fatalf("expected denial after %d hits, got ok=%v err=%v", limit, ok, err)
	}
	if wait <= 0 || wait > window+250*time.Millisecond {
		t.Errorf("denial wait = %s, want within (0, window+slack]", wait)
	}

	// Different queue is an independent window.
	if _, ok, err := l.allow(ctx, "s6rl-other", limit, window); err != nil || !ok {
		t.Errorf("other queue should have its own window (ok=%v err=%v)", ok, err)
	}

	// Window rolls over.
	time.Sleep(wait + 100*time.Millisecond)
	if _, ok, _ := l.allow(ctx, queue, limit, window); !ok {
		t.Error("window did not roll over after wait")
	}
}

// T6.3 core behaviour: exceeding the limit makes the JOB WAIT (BullMQ
// semantics), not fail — all enqueued tasks eventually succeed without retry.
func TestS6RateLimitedTasksWaitNotFail(t *testing.T) {
	h := s6Harness(t)
	ctx := context.Background()

	const (
		taskType = "s6rl:probe"
		queue    = "s6rl"
		total    = 6
		limit    = 2
	)
	server := NewServer(ServerConfig{
		RedisAddr:       h.RedisAddr,
		RedisPass:       harness.RedisPassword,
		Concurrency:     total, // all tasks active at once; limiter serialises
		GracefulTimeout: 5 * time.Second,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Policies: map[string]QueuePolicy{
			queue: {MaxRetry: 1, RateLimit: limit, RateWindow: 700 * time.Millisecond},
		},
	})
	defer func() { _ = server.Close() }()

	var executed atomic.Int64
	done := make(chan struct{}, total)
	server.Register(taskType, func(ctx context.Context, payload []byte) error {
		executed.Add(1)
		done <- struct{}{}
		return nil
	})

	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	client := NewClient(h.RedisAddr, harness.RedisPassword)
	defer func() { _ = client.Close() }()
	for i := 0; i < total; i++ {
		if err := client.Enqueue(ctx, queue, taskType, map[string]any{"i": i},
			&EnqueueOpts{TaskID: fmt.Sprintf("s6rl-%d", i)}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	timeout := time.After(20 * time.Second)
	for n := 0; n < total; n++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatalf("only %d/%d tasks executed within timeout", executed.Load(), total)
		}
	}
	if got := executed.Load(); got != total {
		t.Errorf("executed = %d, want %d (throttled jobs must still run)", got, total)
	}

	// BullMQ-parity invariant: throttled tasks WAIT — none of them may have
	// consumed a retry or landed in the archive. Verified against broker stats
	// instead of wall-clock (window-boundary luck makes elapsed time flaky).
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: h.RedisAddr, Password: harness.RedisPassword})
	defer func() { _ = inspector.Close() }()
	info, err := inspector.GetQueueInfo(queue)
	if err != nil {
		t.Fatalf("queue info: %v", err)
	}
	if info.Retry != 0 || info.Archived != 0 || info.FailedTotal != 0 {
		t.Errorf("broker stats = retry:%d archived:%d failed_total:%d, want all 0 (jobs must wait, not fail)",
			info.Retry, info.Archived, info.FailedTotal)
	}
	if info.ProcessedTotal != total {
		t.Errorf("processed total = %d, want %d", info.ProcessedTotal, total)
	}
}

// A cancelled context while blocked must NOT lose the job: it surfaces as a
// RateLimitedError so the scheduler retries it after its window.
func TestS6RateLimitCtxCancelReturnsRequeueError(t *testing.T) {
	h := s6Harness(t)
	l := newRateLimiter(h.RedisAddr, harness.RedisPassword, false, slog.Default())
	defer func() { _ = l.close() }()

	server := NewServer(ServerConfig{
		RedisAddr: h.RedisAddr,
		RedisPass: harness.RedisPassword,
		Policies: map[string]QueuePolicy{
			"s6rl-cancel": {MaxRetry: 1, RateLimit: 1, RateWindow: time.Hour},
		},
		Logger: slog.Default(),
	})
	defer func() { _ = server.Close() }()

	ctx := context.Background()
	// Exhaust the hourly window.
	if _, ok, _ := l.allow(ctx, "s6rl-cancel", 1, time.Hour); !ok {
		t.Fatal("could not fill window")
	}

	handler := server.rateLimited("s6rl-cancel", QueuePolicy{RateLimit: 1, RateWindow: time.Hour})(func(ctx context.Context, payload []byte) error {
		return nil
	})

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	err := handler(cancelledCtx, []byte(`{}`))
	var rle *RateLimitedError
	if !errors.As(err, &rle) {
		t.Fatalf("err = %v, want *RateLimitedError", err)
	}
	if !rle.Deadline {
		t.Errorf("Deadline flag = false, want true for ctx cancellation")
	}

	// The RetryDelayFunc must map this sentinel to a flat small delay.
	delay := retryDelay(server.policies)(0, &RateLimitedError{RetryIn: time.Second}, asynqTaskOf("x:y"))
	if delay != time.Second {
		t.Errorf("retryDelay(RateLimitedError) = %s, want flat 1s", delay)
	}
}

func TestS6RetryDelayExponentialPerB2(t *testing.T) {
	policies := map[string]QueuePolicy{
		QWebhookProcessing: builtinPolicies[QWebhookProcessing],
		"no-base-queue":    {},
	}
	fn := retryDelay(policies)
	task := func(tt string) *asynq.Task { return asynq.NewTask(tt, nil) }

	if d := fn(0, errors.New("boom"), task("webhook-processing:process")); d != 1*time.Second {
		t.Errorf("first retry delay = %s, want 1s base", d)
	}
	if d := fn(1, errors.New("boom"), task("webhook-processing:process")); d != 2*time.Second {
		t.Errorf("second retry delay = %s, want 2s (base*2^1)", d)
	}
	if d := fn(4, errors.New("boom"), task("webhook-processing:process")); d != 16*time.Second {
		t.Errorf("fifth retry delay = %s, want 16s", d)
	}
	// Queues without backoff fall through to asynq default (>0).
	if d := fn(0, errors.New("boom"), task("no-base-queue:x")); d <= 0 {
		t.Errorf("fallback delay = %s, want positive", d)
	}
}

func asynqTaskOf(tt string) *asynq.Task { return asynq.NewTask(tt, nil) }
