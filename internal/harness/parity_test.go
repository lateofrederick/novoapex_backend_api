package harness

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParDiffJSONTable(t *testing.T) {
	tests := []struct {
		name    string
		want    string
		got     string
		equal   bool
		exact   []string
		contain []string
	}{
		{
			name:  "object key order irrelevant",
			want:  `{"b":1,"a":{"y":2,"x":1}}`,
			got:   `{"a":{"x":1,"y":2},"b":1}`,
			equal: true,
		},
		{
			name:  "nested arrays of objects with scrambled keys equal",
			want:  `{"rows":[{"id":2,"tags":["b","a"]},{"id":1,"tags":["z"]}]}`,
			got:   `{"rows":[{"tags":["b","a"],"id":2},{"id":1,"tags":["z"]}]}`,
			equal: true,
		},
		{
			name:  "numeric int vs float vs exponent representations",
			want:  `{"t":51,"u":5.1e1,"v":[51.0]}`,
			got:   `{"t":51.0,"u":51,"v":[510e-1]}`,
			equal: true,
		},
		{
			name:  "decimal money precision beyond float64",
			want:  `{"amount":9007199254740993,"price":0.300000000000000000004}`,
			got:   `{"amount":9007199254740993,"price":3.00000000000000000004e-1}`,
			equal: true,
		},
		{
			name:  "array order is significant",
			want:  `[1,2]`,
			got:   `[2,1]`,
			equal: false,
			exact: []string{"$[0]: want 1, got 2", "$[1]: want 2, got 1"},
		},
		{
			name:  "divergence inside nested array of objects pinpointed",
			want:  `{"data":{"rows":[{"id":1},{"id":2}]}}`,
			got:   `{"data":{"rows":[{"id":1},{"id":3}]}}`,
			equal: false,
			exact: []string{`$.data.rows[1].id: want 2, got 3`},
		},
		{
			name:  "number mismatch at $.data.total",
			want:  `{"data":{"total":100}}`,
			got:   `{"data":{"total":101}}`,
			equal: false,
			exact: []string{"$.data.total: want 100, got 101"},
		},
		{
			name:  "string never equals number even if same digits",
			want:  `{"price":"51"}`,
			got:   `{"price":51}`,
			equal: false,
			exact: []string{`$.price: want "51", got 51`},
		},
		{
			name:  "null vs missing key distinguished",
			want:  `{"a":null}`,
			got:   `{}`,
			equal: false,
			exact: []string{"$.a: want null, got <missing>"},
		},
		{
			name:  "missing vs unexpected extra key",
			want:  `{}`,
			got:   `{"b":1}`,
			equal: false,
			exact: []string{"$.b: want <missing>, got 1"},
		},
		{
			name:  "null equals null",
			want:  `{"a":null,"l":[null]}`,
			got:   `{"l":[null],"a":null}`,
			equal: true,
		},
		{
			name:  "type mismatch collapses to one line",
			want:  `{"a":[1]}`,
			got:   `{"a":{"0":1}}`,
			equal: false,
			exact: []string{`$.a: want [1], got {"0":1}`},
		},
		{
			name:  "array length mismatch reported once",
			want:  `[1]`,
			got:   `[1,2,3]`,
			equal: false,
			exact: []string{"$: array length want 1, got 3"},
		},
		{
			name:  "bool and string leaf mismatches",
			want:  `{"ok":true,"name":"Ama","n":1.50}`,
			got:   `{"ok":false,"name":"Kofi","n":1.5000001}`,
			equal: false,
			exact: []string{"$.n: want 1.50, got 1.5000001", "$.name: want \"Ama\", got \"Kofi\"", "$.ok: want true, got false"},
		},
		{
			name:    "invalid want json",
			want:    `{"a":`,
			got:     `{"a":1}`,
			equal:   false,
			contain: []string{"$: want is not valid JSON"},
		},
		{
			name:    "trailing data rejected on got side",
			want:    `{"a":1}`,
			got:     "{} {}",
			equal:   false,
			contain: []string{"$: got is not valid JSON (trailing data after JSON value)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diffs := DiffJSON([]byte(tt.want), []byte(tt.got))
			if tt.equal {
				if len(diffs) != 0 {
					t.Fatalf("expected equality, got diffs:\n%s", strings.Join(diffs, "\n"))
				}
				if !EqualJSON([]byte(tt.want), []byte(tt.got)) {
					t.Fatal("EqualJSON must agree with empty DiffJSON")
				}
				return
			}
			if len(diffs) == 0 {
				t.Fatal("expected diffs, got none")
			}
			if EqualJSON([]byte(tt.want), []byte(tt.got)) {
				t.Fatal("EqualJSON must report inequality")
			}
			if tt.exact != nil && !reflect.DeepEqual(diffs, tt.exact) {
				t.Fatalf("diffs mismatch:\n got:  %#v\n want: %#v", diffs, tt.exact)
			}
			for _, frag := range tt.contain {
				found := false
				for _, d := range diffs {
					if strings.Contains(d, frag) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("diffs %v missing fragment %q", diffs, frag)
				}
			}
		})
	}
}

func TestParDiffDeterministicOrder(t *testing.T) {
	want := []byte(`{"z":1,"a":{"m":2,"c":[true,null]},"k":"v"}`)
	got := []byte(`{"k":"other","a":{"c":[false,0],"m":9},"z":1}`)
	first := DiffJSON(want, got)
	for range 5 {
		again := DiffJSON(want, got)
		if !reflect.DeepEqual(first, again) {
			t.Fatalf("diff order unstable:\n%s\nvs\n%s",
				strings.Join(first, "\n"), strings.Join(again, "\n"))
		}
	}
}

func TestParEffectiveClientTimeout(t *testing.T) {
	if got := parEffectiveClient(nil); got.Timeout != 240*time.Second {
		t.Fatalf("nil client timeout = %v, want >=240000ms", got.Timeout)
	}
	if got := parEffectiveClient(&http.Client{}); got.Timeout != 240*time.Second {
		t.Fatalf("zero-timeout client not bumped: %v", got.Timeout)
	}
	custom := &http.Client{Timeout: 5 * time.Second}
	if got := parEffectiveClient(custom); got != custom {
		t.Fatal("explicit timeout client must be used as-is")
	}
}

func TestParStripVolatile(t *testing.T) {
	tests := []struct {
		name    string
		nodeRaw string
		goRaw   string
		strip   []string
		expect  []string
	}{
		{
			name:    "volatile timestamps stripped recursively incl arrays of objects",
			nodeRaw: `{"updatedAt":"2026-08-22T10:00:00Z","items":[{"updatedAt":"A","id":1}],"total":7}`,
			goRaw:   `{"items":[{"id":1,"updatedAt":"B"}],"total":7,"updatedAt":"1999-01-01T00:00:00Z"}`,
			strip:   []string{"updatedAt"},
			expect:  nil,
		},
		{
			name:    "one-sided volatile field still surfaces as missing",
			nodeRaw: `{"ts":"2026-08-22T10:00:00Z","id":1}`,
			goRaw:   `{"id":1}`,
			strip:   []string{"ts"},
			expect:  []string{"$.ts: want \"2026-08-22T10:00:00Z\", got <missing>"},
		},
		{
			name:    "non-stripped divergence untouched by stripping",
			nodeRaw: `{"meta":{"generatedAt":1,"total":100}}`,
			goRaw:   `{"meta":{"total":101,"generatedAt":2}}`,
			strip:   []string{"generatedAt"},
			expect:  []string{"$.meta.total: want 100, got 101"},
		},
		{
			name:    "empty strip list is a no-op passthrough",
			nodeRaw: `{"ts":1}`,
			goRaw:   `{"ts":2}`,
			strip:   nil,
			expect:  []string{"$.ts: want 1, got 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeOut, goOut := parStripVolatile([]byte(tt.nodeRaw), []byte(tt.goRaw), tt.strip)
			diffs := DiffJSON(nodeOut, goOut)
			if tt.expect == nil {
				if len(diffs) != 0 {
					t.Fatalf("expected no diffs after strip:\n%s", strings.Join(diffs, "\n"))
				}
				return
			}
			if !reflect.DeepEqual(diffs, tt.expect) {
				t.Fatalf("diffs mismatch:\n got:  %#v\n want: %#v", diffs, tt.expect)
			}
		})
	}
}

func TestParReplayCorpus(t *testing.T) {
	nodeBody := `{"data":{"currency":"GHS","total":100},"meta":{"page":1}}`
	goBody := `{"meta":{"page":1},"data":{"total":101,"currency":"GHS"}}`

	var nodeHits, goHits atomic.Int32
	nodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nodeHits.Add(1)
		payload, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/orders/summary":
			_, _ = w.Write([]byte(nodeBody))
		case r.Method == http.MethodPost && r.URL.Path == "/webhooks/whatsapp":
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeSrv.Close()

	goSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goHits.Add(1)
		payload, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/orders/summary":
			_, _ = w.Write([]byte(goBody))
		case r.Method == http.MethodPost && r.URL.Path == "/webhooks/whatsapp":
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer goSrv.Close()

	corpusPath := filepath.Join(t.TempDir(), "corpus.jsonl")
	lines := []Exchange{
		{
			Method:      http.MethodGet,
			Path:        "/orders/summary",
			Query:       "window=30d",
			Status:      http.StatusOK,
			RequestBody: Body{Encoding: "utf8"},
		},
		{
			Method:      http.MethodPost,
			Path:        "/webhooks/whatsapp",
			Status:      http.StatusOK,
			RequestBody: Body{Encoding: "utf8", Data: `{"event":"order.created","total":12500}`},
		},
	}
	var corpus strings.Builder
	enc := json.NewEncoder(&corpus)
	for _, ex := range lines {
		if err := enc.Encode(ex); err != nil {
			t.Fatalf("encode corpus line: %v", err)
		}
	}
	if err := os.WriteFile(corpusPath, []byte(corpus.String()), 0o600); err != nil {
		t.Fatalf("write corpus: %v", err)
	}

	t.Run("pairwise diff pinpoints divergent field path", func(t *testing.T) {
		results, err := ReplayCorpus(corpusPath, nodeSrv.URL, goSrv.URL, nil, ReplayOptions{})
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("results = %d, want 2", len(results))
		}
		summary := results[0]
		if summary.Path != "/orders/summary" || summary.Status != http.StatusOK || summary.Err != nil {
			t.Fatalf("unexpected result header: %+v", summary)
		}
		wantDiffs := []string{"$.data.total: want 100, got 101"}
		if !reflect.DeepEqual(summary.Diffs, wantDiffs) {
			t.Fatalf("diffs = %#v, want %#v", summary.Diffs, wantDiffs)
		}
		webhook := results[1]
		if webhook.Err != nil || len(webhook.Diffs) != 0 {
			t.Fatalf("echo exchange must match: %+v", webhook)
		}
	})

	t.Run("path prefix restricts replayed exchanges", func(t *testing.T) {
		beforeNode, beforeGo := nodeHits.Load(), goHits.Load()
		results, err := ReplayCorpus(corpusPath, nodeSrv.URL, goSrv.URL, nil,
			ReplayOptions{PathPrefix: "/webhooks"})
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if len(results) != 1 || results[0].Path != "/webhooks/whatsapp" {
			t.Fatalf("filtered results = %+v, want only /webhooks/whatsapp", results)
		}
		if nodeHits.Load() != beforeNode+1 || goHits.Load() != beforeGo+1 {
			t.Fatal("prefix filter replayed the wrong number of exchanges")
		}
	})

	t.Run("strip fields silences known volatility", func(t *testing.T) {
		volatilePath := filepath.Join(t.TempDir(), "volatile.jsonl")
		volatile := map[string]string{
			"/orders/summary": `{"data":{"total":100,"updatedAt":"A"},"meta":{"page":1}}`,
		}
		nodeV := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(volatile[r.URL.Path]))
		}))
		defer nodeV.Close()
		goV := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"meta":{"page":1},"data":{"updatedAt":"B","total":100}}`))
		}))
		defer goV.Close()

		if err := os.WriteFile(volatilePath, []byte(
			jsonLine(t, Exchange{Method: http.MethodGet, Path: "/orders/summary", Status: 200})), 0o600); err != nil {
			t.Fatalf("write corpus: %v", err)
		}
		results, err := ReplayCorpus(volatilePath, nodeV.URL, goV.URL, nil,
			ReplayOptions{StripFields: []string{"updatedAt"}})
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if len(results) != 1 || len(results[0].Diffs) != 0 || results[0].Err != nil {
			t.Fatalf("stripped replay should be clean: %+v", results)
		}
	})

	t.Run("fail fast stops at first divergence", func(t *testing.T) {
		results, err := ReplayCorpus(corpusPath, nodeSrv.URL, goSrv.URL, nil,
			ReplayOptions{FailFast: true})
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("failfast ran %d exchanges, want exactly 1", len(results))
		}
		if len(results[0].Diffs) == 0 {
			t.Fatal("stopped exchange must carry its diffs")
		}
	})

	t.Run("unreachable stack surfaces per-exchange error", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		deadURL := dead.URL
		dead.Close()

		results, err := ReplayCorpus(corpusPath, deadURL, goSrv.URL, nil, ReplayOptions{})
		if err != nil {
			t.Fatalf("per-exchange failures must not abort the run: %v", err)
		}
		if len(results) == 0 || results[0].Err == nil {
			t.Fatalf("expected transport error recorded, got %+v", results)
		}
		if !strings.Contains(results[0].Err.Error(), "node:") {
			t.Fatalf("error should attribute failing stack: %v", results[0].Err)
		}
	})
}

func TestParSummarize(t *testing.T) {
	results := []ReplayResult{
		{Path: "/health/live", Status: 200},
		{Path: "/orders/summary", Status: 200, Diffs: []string{"$.data.total: want 100, got 101"}},
		{Path: "/customers", Status: 0, Err: errors.New("dial tcp: connection refused")},
	}
	out := Summarize(results)
	for _, frag := range []string{
		"parity: 1/3 exchanges matched",
		"FAIL [1] /orders/summary (status 200): 1 diff(s)",
		"$.data.total: want 100, got 101",
		"FAIL [2] /customers",
	} {
		if !strings.Contains(out, frag) {
			t.Fatalf("summary %q missing fragment %q", out, frag)
		}
	}
	if out := Summarize(nil); out != "parity: 0/0 exchanges matched" {
		t.Fatalf("empty summary = %q", out)
	}
}

func TestParReplayUsesRecordedRequestBodyAndQuery(t *testing.T) {
	received := make(chan string, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case received <- r.Method + "|" + r.URL.RawQuery + "|" + string(body):
		default:
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	corpusPath := filepath.Join(t.TempDir(), "echo.jsonl")
	ex := Exchange{
		Method:         http.MethodPost,
		Path:           "/webhooks/whatsapp",
		Query:          "hub_mode=subscribe",
		RequestHeaders: map[string][]string{"Content-Type": {"application/json"}},
		RequestBody:    Body{Encoding: "utf8", Data: `{"from":"scrub_abc","event":"message"}`},
		Status:         http.StatusOK,
	}
	if err := os.WriteFile(corpusPath, []byte(jsonLine(t, ex)), 0o600); err != nil {
		t.Fatalf("write corpus: %v", err)
	}

	if _, err := ReplayCorpus(corpusPath, srv.URL, srv.URL, nil, ReplayOptions{}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	want := `POST|hub_mode=subscribe|{"from":"scrub_abc","event":"message"}`
	for range 2 {
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("server saw %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("server did not receive both replays")
		}
	}
}

func jsonLine(t *testing.T, ex Exchange) string {
	t.Helper()
	b, err := json.Marshal(ex)
	if err != nil {
		t.Fatalf("marshal exchange: %v", err)
	}
	return string(b)
}
