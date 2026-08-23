package queue

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

// --- shared helpers ----------------------------------------------------------

func requireDockerDaemon(t *testing.T) {
	t.Helper()
	sock := os.Getenv("DOCKER_HOST")
	if sock == "" || !strings.HasPrefix(sock, "unix://") {
		sock = "unix:///var/run/docker.sock"
	}
	path := strings.TrimPrefix(sock, "unix://")

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Skipf("docker ping request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Skipf("docker daemon unavailable at %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
}

// s6Harness boots the shared pgvector/redis containers (redis is what these
// substrate tests exercise; postgres comes along for later parity work).
func s6Harness(t *testing.T) *harness.Harness {
	t.Helper()
	requireDockerDaemon(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	h, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })
	return h
}

// --- T6.2/T6.3: EnqueueOpts -> asynq option mapping --------------------------

func optStrings(opts []asynq.Option) []string {
	out := make([]string, len(opts))
	for i, o := range opts {
		out[i] = o.String()
	}
	return out
}

func TestS6EnqueueOptsMapping(t *testing.T) {
	tests := []struct {
		name  string
		queue string
		opts  *EnqueueOpts
		want  []string
	}{
		{
			name:  "nil opts falls back to B.2 policy defaults",
			queue: QWebhookProcessing,
			opts:  nil,
			want:  []string{`Queue("webhook-processing")`, "MaxRetry(2)"},
		},
		{
			name:  "orchestrator terminal policy",
			queue: QOrchestrator,
			opts:  nil,
			want:  []string{`Queue("orchestrator-queue")`, "MaxRetry(0)"},
		},
		{
			name:  "payment events five attempts => four retries",
			queue: QPaymentEvents,
			opts:  nil,
			want:  []string{`Queue("payment-events")`, "MaxRetry(4)"},
		},
		{
			name:  "explicit maxretry wins over policy",
			queue: QPaymentEvents,
			opts:  &EnqueueOpts{MaxRetry: 7},
			want:  []string{`Queue("payment-events")`, "MaxRetry(7)"},
		},
		{
			name:  "processin maps delay",
			queue: QFollowUp,
			opts:  &EnqueueOpts{ProcessIn: 3 * time.Second},
			want:  []string{`Queue("follow-up")`, "MaxRetry(2)", "ProcessIn(3s)"},
		},
		{
			name:  "processat ignored when processin set",
			queue: QFollowUp,
			opts:  &EnqueueOpts{ProcessIn: time.Second, ProcessAt: time.Now().Add(time.Hour)},
			want:  []string{`Queue("follow-up")`, "MaxRetry(2)", "ProcessIn(1s)"},
		},
		{
			name:  "taskid fixed-id dedup",
			queue: QWebhookProcessing,
			opts:  &EnqueueOpts{TaskID: "wa-msg-42"},
			want:  []string{`Queue("webhook-processing")`, "MaxRetry(2)", `TaskID("wa-msg-42")`},
		},
		{
			name:  "unique ttl",
			queue: QOutbound,
			opts:  &EnqueueOpts{UniqueTTL: time.Minute},
			want:  []string{`Queue("outbound-queue")`, "MaxRetry(0)", "Unique(1m0s)"},
		},
		{
			name:  "retain ttl retention",
			queue: QCRMMaterialiser,
			opts:  &EnqueueOpts{RetainTTL: time.Hour},
			want:  []string{`Queue("crm-materialiser")`, "MaxRetry(2)", "Retention(1h0m0s)"},
		},
		{
			name:  "full mapping together",
			queue: QEmbedding,
			opts: &EnqueueOpts{
				TaskID: "prod-9", UniqueTTL: time.Minute,
				RetainTTL: time.Minute, ProcessAt: time.Unix(1700000000, 0).UTC(),
			},
			want: []string{
				`Queue("embedding")`, "MaxRetry(2)",
				fmt.Sprintf("ProcessAt(%s)", time.Unix(1700000000, 0).UTC().Format(time.UnixDate)),
				`TaskID("prod-9")`, "Unique(1m0s)", "Retention(1m0s)",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := optStrings(buildAsynqOptions(tt.queue, tt.opts))
			if strings.Join(got, " ") != strings.Join(tt.want, " ") {
				t.Errorf("opts mismatch\n got: %v\nwant: %v", got, tt.want)
			}
		})
	}
}

func TestS6PolicyTableMatchesB2(t *testing.T) {
	cases := []struct {
		queue    string
		maxRetry int
		base     time.Duration
		limit    int
		window   time.Duration
	}{
		{QWebhookProcessing, 2, time.Second, 100, 10 * time.Second},
		{QOrchestrator, 0, 0, 0, 0},
		{QOutbound, 0, 0, 50, time.Second},
		{QCRMMaterialiser, 2, 2 * time.Second, 50, time.Second},
		{QPaymentEvents, 4, 3 * time.Second, 20, time.Second},
		{QFollowUp, 2, 5 * time.Second, 10, time.Second},
		{QEmbedding, 2, 2 * time.Second, 0, 0},
	}
	for _, c := range cases {
		p := PolicyFor(c.queue)
		if p.MaxRetry != c.maxRetry || p.BackoffBase != c.base ||
			p.RateLimit != c.limit || p.RateWindow != c.window {
			t.Errorf("%s policy = %+v, want retry=%d base=%s rl=%d/%s",
				c.queue, p, c.maxRetry, c.base, c.limit, c.window)
		}
	}
	if p := PolicyFor("unknown-queue"); p.MaxRetry != 0 {
		t.Errorf("unknown queue fallback = %+v, want zero-value", p)
	}
}

// --- real broker round trip ---------------------------------------------------

func TestS6ClientEnqueueRoundTripAndDedupSwallow(t *testing.T) {
	h := s6Harness(t)
	ctx := context.Background()

	c := NewClient(h.RedisAddr, harness.RedisPassword)
	defer func() { _ = c.Close() }()

	if err := c.Enqueue(ctx, QEmbedding, TaskEmbedProduct,
		map[string]any{"productId": "p-roundtrip"}, &EnqueueOpts{TaskID: "s6-fixed-id"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: h.RedisAddr, Password: harness.RedisPassword})
	defer func() { _ = inspector.Close() }()

	pending, err := inspector.ListPendingTasks(QEmbedding, asynq.PageSize(100))
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	var match *asynq.TaskInfo
	for _, ti := range pending {
		if ti.Type == TaskEmbedProduct {
			match = ti
			break
		}
	}
	if match == nil {
		t.Fatalf("enqueued task not found in %q pending list (%d tasks)", QEmbedding, len(pending))
	}
	if match.ID != "s6-fixed-id" {
		t.Errorf("task id = %q, want s6-fixed-id (TaskID mapping)", match.ID)
	}
	if string(match.Payload) != `{"productId":"p-roundtrip"}` {
		t.Errorf("payload = %s, want json-encoded map", match.Payload)
	}
	if match.Queue != QEmbedding {
		t.Errorf("queue = %q, want %q", match.Queue, QEmbedding)
	}

	// BullMQ parity: duplicate fixed id is silently dropped, not an error.
	before := len(pending)
	if err := c.Enqueue(ctx, QEmbedding, TaskEmbedProduct,
		map[string]any{"productId": "p-roundtrip"}, &EnqueueOpts{TaskID: "s6-fixed-id"}); err != nil {
		t.Errorf("duplicate TaskID must be swallowed (BullMQ jobId semantics), got %v", err)
	}
	pending2, err := inspector.ListPendingTasks(QEmbedding, asynq.PageSize(100))
	if err != nil {
		t.Fatalf("re-list pending: %v", err)
	}
	if len(pending2) != before {
		t.Errorf("pending count after duplicate = %d, want %d", len(pending2), before)
	}

	// Unique dedup also swallows.
	uerr := c.Enqueue(ctx, QEmbedding, "s6:unique-probe", map[string]any{"k": 1}, &EnqueueOpts{UniqueTTL: time.Minute})
	if uerr != nil {
		t.Fatalf("unique enqueue: %v", uerr)
	}
	if err := c.Enqueue(ctx, QEmbedding, "s6:unique-probe", map[string]any{"k": 1}, &EnqueueOpts{UniqueTTL: time.Minute}); err != nil {
		t.Errorf("duplicate Unique must be swallowed, got %v", err)
	}
}

func TestS6PublisherInterfaceSatisfaction(t *testing.T) {
	var _ Publisher = (*AsynqClient)(nil)
	var _ Registrar = (*Server)(nil)
}
