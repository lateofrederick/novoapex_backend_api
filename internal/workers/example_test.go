package workers_test

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestExampleProcessorLogsEveryJobOnTheQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	h, err := harness.Start(ctx)
	if err != nil {
		t.Skipf("docker harness unavailable: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })

	var logs syncBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	server := queue.NewServer(queue.ServerConfig{RedisAddr: h.RedisAddr, RedisPass: harness.RedisPassword, GracefulTimeout: 5 * time.Second, Logger: logger})
	t.Cleanup(func() { _ = server.Close() })
	workers.RegisterExample(server, logger)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	client := queue.NewClient(h.RedisAddr, harness.RedisPassword)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Enqueue(ctx, queue.QExample, queue.TaskExamplePrefix+"greet", map[string]any{"hello": "world"}, &queue.EnqueueOpts{TaskID: "job-1"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Enqueue(ctx, queue.QExample, queue.TaskExamplePrefix+"other", nil, &queue.EnqueueOpts{TaskID: "job-2"}); err != nil {
		t.Fatal(err)
	}

	want := []string{
		`Processing job job-1 (greet): {\"hello\":\"world\"}`,
		`Processing job job-2 (other): null`,
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if out := logs.String(); strings.Contains(out, want[0]) && strings.Contains(out, want[1]) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	out := logs.String()
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("log missing %s\n%s", w, out)
		}
	}
	if !strings.Contains(out, `"processor":"ExampleProcessor"`) {
		t.Errorf("log lines are not tagged with the processor context\n%s", out)
	}

	t.Run("every queue is declared for the dashboard", func(t *testing.T) {
		opt := asynq.RedisClientOpt{Addr: h.RedisAddr, Password: harness.RedisPassword}
		if err := queue.DeclareQueues(ctx, opt); err != nil {
			t.Fatal(err)
		}
		inspector := asynq.NewInspector(opt)
		defer func() { _ = inspector.Close() }()
		queues, err := inspector.Queues()
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queue.AllQueues() {
			if !slices.Contains(queues, q) {
				t.Errorf("queue %q not listed (have %v)", q, queues)
			}
		}
	})
}
