package observability_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/observability"
)

// ingest is a fake Sentry envelope endpoint.
type ingest struct {
	mu    sync.Mutex
	items []envelopeItem
}

type envelopeItem struct {
	EventID string
	Type    string
	Payload map[string]any
}

func (in *ingest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	rd := bufio.NewReader(bytes.NewReader(body))
	headerLine, _ := rd.ReadBytes('\n')
	var header struct {
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(headerLine, &header)
	for {
		line, err := rd.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) == 0 {
			if err != nil {
				break
			}
			continue
		}
		var ih struct {
			Type   string `json:"type"`
			Length *int   `json:"length"`
		}
		if json.Unmarshal(line, &ih) != nil {
			break
		}
		var payload []byte
		if ih.Length != nil {
			payload = make([]byte, *ih.Length)
			if _, err := io.ReadFull(rd, payload); err != nil {
				break
			}
			_, _ = rd.ReadByte() // trailing newline
		} else {
			payload, _ = rd.ReadBytes('\n')
		}
		var decoded map[string]any
		_ = json.Unmarshal(payload, &decoded)
		in.mu.Lock()
		in.items = append(in.items, envelopeItem{EventID: header.EventID, Type: ih.Type, Payload: decoded})
		in.mu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
}

func (in *ingest) ofType(t string) []envelopeItem {
	in.mu.Lock()
	defer in.mu.Unlock()
	var out []envelopeItem
	for _, it := range in.items {
		if it.Type == t {
			out = append(out, it)
		}
	}
	return out
}

func startSentry(t *testing.T) *ingest {
	t.Helper()
	in := &ingest{}
	srv := httptest.NewServer(in)
	t.Cleanup(srv.Close)

	defaultTransport := http.DefaultTransport
	one := 1.0
	ok, err := observability.InitObservability(observability.Config{
		DSN:                strings.Replace(srv.URL, "http://", "http://publickey@", 1) + "/1",
		Environment:        "test",
		TracesSampleRate:   &one,
		ProfilesSampleRate: &one,
	})
	if err != nil || !ok {
		t.Fatalf("init sentry: %v %v", ok, err)
	}
	t.Cleanup(func() {
		sentry.Flush(2 * time.Second)
		sentry.CurrentHub().BindClient(nil)
		http.DefaultTransport = defaultTransport
	})
	return in
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func dig(m map[string]any, path ...string) any {
	var cur any = m
	for _, p := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[p]
	}
	return cur
}

func TestSentryInstrumentation(t *testing.T) {
	in := startSentry(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(upstream.Close)

	kernel := httpx.New(observability.HTTPMiddleware)
	kernel.MountHealth(httpx.HealthDeps{DB: failingPinger{}, Redis: failingPinger{}})
	kernel.Router.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		tracer := observability.PGXTracer{}
		qctx := tracer.TraceQueryStart(r.Context(), nil, pgx.TraceQueryStartData{SQL: "SELECT *\n  FROM orders WHERE id = $1"})
		tracer.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{CommandTag: pgconn.NewCommandTag("SELECT 1")})
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/meta", nil)
		if resp, err := (&http.Client{}).Do(req); err == nil {
			_ = resp.Body.Close()
		}
		profiledWork(120 * time.Millisecond)
		_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"id": chi.URLParam(r, "id")})
	})
	kernel.Router.Post("/products", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		httpx.WriteZodValidationError(w, []httpx.FieldIssue{{Code: "too_small", Path: "price", Message: "Too small"}})
	})
	kernel.Router.With(auth.Middleware("secret", nil)).Get("/secure", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	kernel.Router.Get("/boom", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, errors.New("database exploded"))
	})
	kernel.Router.Get("/panic", func(http.ResponseWriter, *http.Request) {
		panic("handler panicked")
	})
	api := httptest.NewServer(kernel.Handler())
	t.Cleanup(api.Close)

	do := func(method, path, body string, headers map[string]string) int {
		req, _ := http.NewRequest(method, api.URL+path, strings.NewReader(body))
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := do("GET", "/orders/o-1", "", map[string]string{"sentry-trace": "0123456789abcdef0123456789abcdef-89abcdef01234567-1"}); code != 200 {
		t.Fatalf("orders = %d", code)
	}
	if code := do("POST", "/products?draft=1", `{"name":"Shea","price":-1,"phone":"+233200000001"}`, map[string]string{"Content-Type": "application/json", "X-User-Id": "u-7"}); code != 400 {
		t.Fatalf("products = %d", code)
	}
	if code := do("GET", "/secure", "", nil); code != 401 {
		t.Fatalf("secure = %d", code)
	}
	if code := do("GET", "/boom", "", nil); code != 500 {
		t.Fatalf("boom = %d", code)
	}
	if code := do("GET", "/panic", "", nil); code != 500 {
		t.Fatalf("panic = %d", code)
	}
	if code := do("GET", "/health", "", nil); code != 503 {
		t.Fatalf("health = %d", code)
	}

	err := observability.TraceTask(context.Background(), "outbound-queue:send-message", func(ctx context.Context) error {
		done := make(chan struct{})
		go func() { profiledWork(120 * time.Millisecond); close(done) }()
		<-done
		return errors.New("meta rejected")
	})
	if err == nil || err.Error() != "meta rejected" {
		t.Fatalf("TraceTask must return the task's error, got %v", err)
	}

	sentry.Flush(5 * time.Second)

	// Issues keyed "<exception type>: <value>", or the message for a
	// non-error panic value.
	exceptions := map[string]envelopeItem{}
	for _, ev := range in.ofType("event") {
		values, _ := ev.Payload["exception"].([]any)
		if len(values) == 0 {
			exceptions[str(ev.Payload["message"])] = ev
			continue
		}
		last, _ := values[len(values)-1].(map[string]any)
		exceptions[str(last["type"])+": "+str(last["value"])] = ev
	}

	t.Run("4xx exceptions are captured under their Nest names with body, tags and user", func(t *testing.T) {
		zod, ok := exceptions["ZodValidationException: Validation failed"]
		if !ok {
			t.Fatalf("validation failure not captured; have %v", keys(exceptions))
		}
		if got := str(dig(zod.Payload, "tags", "path")); got != "/products?draft=1" {
			t.Errorf("path tag = %q (request.url, with query)", got)
		}
		if got := str(dig(zod.Payload, "tags", "method")); got != "POST" {
			t.Errorf("method tag = %q", got)
		}
		if got := str(dig(zod.Payload, "user", "id")); got != "u-7" {
			t.Errorf("user = %q, want the X-User-Id", got)
		}
		if got := str(dig(zod.Payload, "contexts", "extra", "body", "name")); got != "Shea" {
			t.Errorf("body extra name = %q", got)
		}
		if got := str(dig(zod.Payload, "contexts", "extra", "body", "phone")); got != observability.RedactedValue {
			t.Errorf("body extra phone = %q, want it scrubbed", got)
		}
		if _, ok := exceptions["UnauthorizedException: Invalid or missing session token"]; !ok {
			t.Errorf("auth guard rejection not captured; have %v", keys(exceptions))
		}
	})

	t.Run("server errors and panics are captured; health paths are not", func(t *testing.T) {
		if _, ok := exceptions["*errors.errorString: database exploded"]; !ok {
			t.Errorf("handler error not captured; have %v", keys(exceptions))
		}
		panicked := false
		for k := range exceptions {
			if strings.Contains(k, "handler panicked") {
				panicked = true
			}
			if strings.Contains(k, "Service Unavailable") || strings.Contains(k, "health") {
				t.Errorf("health check failure reported: %s", k)
			}
		}
		if !panicked {
			t.Errorf("panic not captured; have %v", keys(exceptions))
		}
		if len(exceptions) != 4 {
			t.Errorf("captured %d exceptions, want 4: %v", len(exceptions), keys(exceptions))
		}
	})

	transactions := map[string]envelopeItem{}
	for _, tx := range in.ofType("transaction") {
		transactions[str(tx.Payload["transaction"])] = tx
	}
	profiles := map[string]envelopeItem{}
	for _, p := range in.ofType("profile") {
		profiles[p.EventID] = p
	}

	t.Run("requests are route-named transactions with db and http spans, continuing the trace", func(t *testing.T) {
		tx, ok := transactions["GET /orders/{id}"]
		if !ok {
			t.Fatalf("no route-named transaction; have %v", keys(transactions))
		}
		if got := str(dig(tx.Payload, "transaction_info", "source")); got != "route" {
			t.Errorf("source = %q", got)
		}
		if got := str(dig(tx.Payload, "contexts", "trace", "trace_id")); got != "0123456789abcdef0123456789abcdef" {
			t.Errorf("trace_id = %q, want the incoming sentry-trace", got)
		}
		if got := str(dig(tx.Payload, "contexts", "trace", "op")); got != "http.server" {
			t.Errorf("op = %q", got)
		}
		ops := map[string]string{}
		spans, _ := tx.Payload["spans"].([]any)
		for _, s := range spans {
			sm, _ := s.(map[string]any)
			ops[str(sm["op"])] = str(sm["description"])
		}
		if ops["db.sql.query"] != "SELECT * FROM orders WHERE id = $1" {
			t.Errorf("db span = %q (spans %v)", ops["db.sql.query"], ops)
		}
		if !strings.HasPrefix(ops["http.client"], "GET "+upstream.URL+"/meta") {
			t.Errorf("http.client span = %q (spans %v)", ops["http.client"], ops)
		}
	})

	t.Run("profiled transactions carry a sample-format profile in the same envelope", func(t *testing.T) {
		for _, name := range []string{"GET /orders/{id}", "outbound-queue:send-message"} {
			tx, ok := transactions[name]
			if !ok {
				t.Fatalf("transaction %q missing; have %v", name, keys(transactions))
			}
			p, ok := profiles[tx.EventID]
			if !ok {
				t.Fatalf("%s: no profile in its envelope", name)
			}
			if got := str(dig(p.Payload, "transaction", "id")); got != tx.EventID {
				t.Errorf("%s: profile transaction.id = %q, want %q", name, got, tx.EventID)
			}
			if got := str(dig(p.Payload, "transaction", "name")); got != name {
				t.Errorf("%s: profile transaction.name = %q", name, got)
			}
			if got := str(dig(tx.Payload, "contexts", "profile", "profile_id")); got == "" || got != str(p.Payload["event_id"]) {
				t.Errorf("%s: contexts.profile.profile_id = %q, profile event_id = %q", name, got, str(p.Payload["event_id"]))
			}
			if p.Payload["version"] != "1" || p.Payload["platform"] != "go" {
				t.Errorf("%s: version/platform = %v/%v", name, p.Payload["version"], p.Payload["platform"])
			}
			samples, _ := dig(p.Payload, "profile", "samples").([]any)
			if len(samples) < 2 {
				t.Errorf("%s: %d samples, want >= 2", name, len(samples))
			}
			frames, _ := dig(p.Payload, "profile", "frames").([]any)
			found := false
			for _, f := range frames {
				fm, _ := f.(map[string]any)
				if strings.Contains(str(fm["function"]), "profiledWork") && fm["in_app"] == true {
					found = true
				}
			}
			if !found {
				t.Errorf("%s: profile frames do not show the in-app work function: %v", name, frames)
			}
		}
		task := transactions["outbound-queue:send-message"]
		if got := str(dig(task.Payload, "contexts", "trace", "op")); got != "queue.process" {
			t.Errorf("task op = %q", got)
		}
		if got := str(dig(task.Payload, "contexts", "trace", "status")); got != "internal_error" {
			t.Errorf("failed task status = %q", got)
		}
		threads := map[string]bool{}
		samples, _ := dig(profiles[task.EventID].Payload, "profile", "samples").([]any)
		for _, s := range samples {
			threads[str(s.(map[string]any)["thread_id"])] = true
		}
		if len(threads) < 2 {
			t.Errorf("task profile sampled %d goroutines, want the task's and the one it started", len(threads))
		}
	})
}

//go:noinline
func profiledWork(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}

type failingPinger struct{}

func (failingPinger) Ping(context.Context) error { return errors.New("down") }

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
