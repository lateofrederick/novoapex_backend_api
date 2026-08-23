package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

// T6.4 end-to-end: the failure handler is EXPLICITLY registered (asynq
// Config.ErrorHandler) and provably fires — hook lands exactly once when
// retries are exhausted against a real broker.
func TestS6FailureHookFiresExactlyOnceOnExhaustedRetries(t *testing.T) {
	h := s6Harness(t)
	ctx := context.Background()

	const taskType = "s6fail:boom"
	var hookCalls atomic.Int64
	orig := SentryHook
	defer func() { SentryHook = orig }()
	SentryHook = func(tt string, err error) {
		if tt != taskType {
			t.Errorf("hook taskType = %q, want %q", tt, taskType)
		}
		hookCalls.Add(1)
	}

	server := NewServer(ServerConfig{
		RedisAddr:       h.RedisAddr,
		RedisPass:       harness.RedisPassword,
		Concurrency:     2,
		GracefulTimeout: 5 * time.Second,
		Logger:          slog.New(slog.NewTextHandler(&discardWriter{}, nil)),
		Policies: map[string]QueuePolicy{
			"s6fail": {MaxRetry: 2, BackoffBase: 10 * time.Millisecond}, // 3 attempts total, fast
		},
	})
	defer func() { _ = server.Close() }()

	var attempts atomic.Int64
	server.Register(taskType, func(ctx context.Context, payload []byte) error {
		attempts.Add(1)
		return errors.New("simulated permanent business failure")
	})

	if err := server.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	client := NewClient(h.RedisAddr, harness.RedisPassword)
	defer func() { _ = client.Close() }()
	// Explicit MaxRetry: "s6fail" is a test queue with no §B.2 row, so the
	// client's nil-opts default would be zero retries.
	if err := client.Enqueue(ctx, "s6fail", taskType, map[string]any{"k": "v"}, &EnqueueOpts{MaxRetry: 2}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if hookCalls.Load() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // let any spurious extra fires surface

	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3 (MaxRetry 2 => first try + 2 retries)", got)
	}
	if got := hookCalls.Load(); got != 1 {
		t.Errorf("Sentry hook fired %d times, want exactly once on exhaustion", got)
	}
}

func TestS6ServerRoutesPayloadsByTaskType(t *testing.T) {
	h := s6Harness(t)
	ctx := context.Background()

	server := NewServer(ServerConfig{
		RedisAddr:       h.RedisAddr,
		RedisPass:       harness.RedisPassword,
		GracefulTimeout: 5 * time.Second,
		Policies: map[string]QueuePolicy{
			"s6route-a": {MaxRetry: 1},
			"s6route-b": {MaxRetry: 1},
		},
	})
	defer func() { _ = server.Close() }()

	got := make(chan string, 4)
	server.Register("s6route-a:t1", func(ctx context.Context, payload []byte) error {
		got <- "a:" + string(payload)
		return nil
	})
	server.Register("s6route-b:t2", func(ctx context.Context, payload []byte) error {
		got <- "b:" + string(payload)
		return nil
	})

	if err := server.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	client := NewClient(h.RedisAddr, harness.RedisPassword)
	defer func() { _ = client.Close() }()
	for _, c := range []struct{ q, tt string }{
		{"s6route-a", "s6route-a:t1"},
		{"s6route-b", "s6route-b:t2"},
	} {
		if err := client.Enqueue(ctx, c.q, c.tt, map[string]any{"hello": c.q}, nil); err != nil {
			t.Fatalf("enqueue %s: %v", c.tt, err)
		}
	}

	// Arrival order across two queues is not deterministic; compare as sets.
	received := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case g := <-got:
			received[g] = true
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for handler messages (have %v)", received)
		}
	}
	for _, want := range []string{`a:{"hello":"s6route-a"}`, `b:{"hello":"s6route-b"}`} {
		if !received[want] {
			t.Errorf("handler messages = %v, missing %q", received, want)
		}
	}
}

func TestS6StopIsIdempotentAndFast(t *testing.T) {
	h := s6Harness(t)
	server := NewServer(ServerConfig{
		RedisAddr: h.RedisAddr,
		RedisPass: harness.RedisPassword,
		Policies:  map[string]QueuePolicy{"s6stop": {MaxRetry: 1}},
	})
	defer func() { _ = server.Close() }()

	if err := server.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first stop: %v", err)
		}
	case <-time.After(DefaultGracefulTimeout + 5*time.Second):
		t.Fatal("stop blocked beyond graceful timeout")
	}
	// Second stop must not panic or hang.
	_ = server.Stop()
}

type discardWriter struct{}

func (*discardWriter) Write(p []byte) (int, error) { return len(p), nil }
