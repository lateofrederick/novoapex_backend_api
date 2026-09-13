package workers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

// ---------------------------------------------------------------------------
// DB bootstrap (real Postgres via testcontainers + prisma migrations)
// ---------------------------------------------------------------------------

type s7bDB struct {
	pool    *pgxpool.Pool
	db      *sql.DB
	factory *harness.Factory
}

func s7bDockerAlive(t *testing.T) {
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

func s7bStartDB(t *testing.T) *s7bDB {
	t.Helper()
	s7bDockerAlive(t)
	ctx := t.Context()

	env, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness postgres: %v", err)
	}
	t.Cleanup(func() { env.Terminate(context.Background()) })

	if err := harness.ApplyBaselineSchema(ctx, env.PostgresDSN); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	pool, err := pgxpool.New(ctx, env.PostgresDSN)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	db, err := sql.Open("pgx", env.PostgresDSN)
	if err != nil {
		t.Fatalf("open stdlib db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return &s7bDB{pool: pool, db: db, factory: harness.NewFactory(t, db)}
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type s7bEnqueuedJob struct {
	Queue    string
	TaskType string
	Payload  map[string]any
}

type s7bPublisher struct {
	mu       sync.Mutex
	enqueued []s7bEnqueuedJob
}

func (p *s7bPublisher) Enqueue(_ context.Context, q, taskType string, payload any, _ *queue.EnqueueOpts) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Decode the wire JSON, as a real consumer would see it, rather than
	// type-asserting the producer's Go value.
	var m map[string]any
	if raw, err := json.Marshal(payload); err == nil {
		_ = json.Unmarshal(raw, &m)
	}
	p.enqueued = append(p.enqueued, s7bEnqueuedJob{Queue: q, TaskType: taskType, Payload: m})
	return nil
}

func (p *s7bPublisher) snapshot() []s7bEnqueuedJob {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]s7bEnqueuedJob, len(p.enqueued))
	copy(out, p.enqueued)
	return out
}

var _ queue.Publisher = (*s7bPublisher)(nil)

type s7bPaystack struct {
	mu       sync.Mutex
	requests []workers.PaymentRequest
	fail     bool
}

func (f *s7bPaystack) InitiatePayment(_ context.Context, req workers.PaymentRequest) (workers.PaymentLink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if f.fail {
		return workers.PaymentLink{Status: "failed"}, errors.New("paystack transport down")
	}
	return workers.PaymentLink{
		Status:            "initiated",
		ProviderReference: "ref-" + req.Reference,
		PaymentURL:        "https://checkout.paystack.test/pay/" + req.Reference,
	}, nil
}

func (f *s7bPaystack) snapshot() []workers.PaymentRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]workers.PaymentRequest, len(f.requests))
	copy(out, f.requests)
	return out
}

var _ workers.PaystackInitiator = (*s7bPaystack)(nil)

// ---------------------------------------------------------------------------
// shared env + job builders
// ---------------------------------------------------------------------------

type s7bEnv struct {
	dbw       *s7bDB
	publisher *s7bPublisher
	paystack  *s7bPaystack
	deps      workers.CRMDeps
}

func s7bNewEnv(t *testing.T) *s7bEnv {
	t.Helper()
	dbw := s7bStartDB(t)
	e := &s7bEnv{
		dbw:       dbw,
		publisher: &s7bPublisher{},
		paystack:  &s7bPaystack{},
	}
	e.deps = workers.CRMDeps{
		Pool:      dbw.pool,
		Publisher: e.publisher,
		Paystack:  e.paystack,
	}
	return e
}

func s7bPtr(s string) *string { return &s }

// env is the current test's fixture bundle; each test assigns it via
// env = s7bNewEnv(t) so the assertion helpers can reach the DB.
var env *s7bEnv

func ctx() context.Context { return context.Background() }

func s7bBaseJob(bizID, custID, convID, phone, sourceMsg string) workers.CRMSignalJob {
	return workers.CRMSignalJob{
		BusinessID:           bizID,
		CustomerID:           custID,
		ConversationID:       convID,
		CustomerPhone:        phone,
		SourceMessageID:      sourceMsg,
		LLMIntent:            "product_inquiry",
		ReferencedProductIDs: []string{},
		Timestamp:            time.Now().UTC().Format(time.RFC3339),
		CRMSignals: workers.CrmSignals{
			DetectedItems:       []workers.DetectedItem{},
			DetectedPreferences: []string{},
			Sentiment:           s7bPtr("neutral"),
		},
	}
}
