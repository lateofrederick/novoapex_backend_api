package workers_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/google"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

func sqlOpen(dsn string) (*sql.DB, error) { return sql.Open("pgx", dsn) }

type sqlDB = sql.DB

// --- local fakes -------------------------------------------------------------

type embedProbe struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dims       int      `json:"dimensions"`
	EncodingFm string   `json:"encoding_format"`
}

// deterministicEmbedStub serves POST /embeddings exactly like the harness
// OpenAI stub: vector := harness.Embedding(inputText, 768). Sharing the same
// function makes cross-stub results comparable bit-for-bit.
type embedStub struct {
	server *http.Server
	mu     sync.Mutex
	reqs   []embedProbe
	failOn func(body embedProbe) bool
}

func (s *embedStub) probes() []embedProbe {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]embedProbe(nil), s.reqs...)
}

func startOpenAIEmbedStub(t *testing.T) (*embedStub, string) {
	t.Helper()
	stub := &embedStub{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		var probe embedProbe
		_ = json.NewDecoder(r.Body).Decode(&probe)
		stub.mu.Lock()
		stub.reqs = append(stub.reqs, probe)
		fail := stub.failOn != nil && stub.failOn(probe)
		stub.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"message":"stub outage"}}`))
			return
		}
		text := ""
		if len(probe.Input) > 0 {
			text = probe.Input[0]
		}
		writeJSON(w, map[string]any{
			"object": "list",
			"data": []any{map[string]any{
				"object":    "embedding",
				"index":     0,
				"embedding": harness.Embedding(text, 768),
			}},
			"model": probe.Model,
		})
	})
	return stub, serveOn(t, mux, "/v1")
}

func startGoogleEmbedStub(t *testing.T) (*embedStub, string) {
	t.Helper()
	stub := &embedStub{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1beta/models/gemini-embedding-001:embedContent", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model                string `json:"model"`
			OutputDimensionality int    `json:"outputDimensionality"`
			Content              struct {
				Parts []struct {
					InlineData *struct {
						MimeType string `json:"mimeType"`
						Data     string `json:"data"`
					} `json:"inlineData"`
				} `json:"parts"`
			} `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		var mime string
		if len(body.Content.Parts) > 0 && body.Content.Parts[0].InlineData != nil {
			mime = body.Content.Parts[0].InlineData.MimeType
		}
		seed := fmt.Sprintf("%s|%s", mime, body.Model)
		stub.mu.Lock()
		stub.reqs = append(stub.reqs, embedProbe{Model: body.Model, Input: []string{seed}, Dims: body.OutputDimensionality})
		stub.mu.Unlock()
		writeJSON(w, map[string]any{"embedding": map[string]any{"values": harness.Embedding(seed, 768)}})
	})
	return stub, serveOn(t, mux, "/v1beta")
}

func serveOn(t *testing.T, handler http.Handler, prefix string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("stub listen: %v", err)
	}
	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String() + prefix
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func newPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newEmbeddingSvc(t *testing.T, h *harness.Harness) (*workers.EmbeddingService, *embedStub, *embedStub, *pgxpool.Pool) {
	t.Helper()
	oaiStub, oaiBase := startOpenAIEmbedStub(t)
	gStub, gBase := startGoogleEmbedStub(t)
	pool := newPool(t, h.PostgresDSN)
	svc := workers.NewEmbeddingService(workers.EmbeddingDeps{
		Pool:   pool,
		OpenAI: openai.New(openai.Config{APIKey: "sk-test-dummy", BaseURL: oaiBase}),
		Google: google.New(google.Config{APIKey: "g-test-dummy", BaseURL: gBase}),
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return svc, oaiStub, gStub, pool
}

// --- T6.15: byte-exact search document ---------------------------------------

func TestS6BuildSearchDocumentByteExact(t *testing.T) {
	strPtr := func(s string) *string { return &s }
	tests := []struct {
		name        string
		productName string
		description *string
		want        string
	}{
		{"name only", "Handwoven Basket", nil, "Handwoven Basket"},
		{"nil description", "Kente Cloth", nil, "Kente Cloth"},
		{"joined with dot-space", "Shea Butter", strPtr("Raw organic grade A"), "Shea Butter. Raw organic grade A"},
		{"description trimmed", "Shea Butter", strPtr("   Raw organic\tgrade A \n"), "Shea Butter. Raw organic\tgrade A"},
		{"whitespace-only description dropped", "Cocoa Pod", strPtr(" \n\t "), "Cocoa Pod"},
		{"empty description dropped", "Cocoa Pod", strPtr(""), "Cocoa Pod"},
		{"JS trims ZWNBSP like JS trim", "Beads", strPtr("\ufefffancy beads\u2003"), "Beads. fancy beads"},
		{"NEL is NOT trimmed (JS semantics)", "Yam", strPtr("\u0085puna yam"), "Yam. \u0085puna yam"},
		{"name never trimmed", "  Spaced Name  ", nil, "  Spaced Name  "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workers.BuildSearchDocument(tt.productName, tt.description); got != tt.want {
				t.Errorf("BuildSearchDocument = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- T6.20: processor switch (hermetic — no DB touched on these paths) --------

func TestS6DispatchUnknownJobTypeError(t *testing.T) {
	svc := workers.NewEmbeddingService(workers.EmbeddingDeps{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	err := svc.DispatchTask(context.Background(), "embedding:embed-product-video", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for unknown job type")
	}
	// Exact TS message: `Unknown embedding job type: ${job.name}`.
	if want := "Unknown embedding job type: embed-product-video"; err.Error() != want {
		t.Errorf("err = %q, want %q", err.Error(), want)
	}
	if err := svc.DispatchTask(context.Background(), queue.TaskEmbedProduct, []byte(`{}`)); err == nil ||
		err.Error() != "embedding: payload missing productId" {
		t.Errorf("missing productId err = %v", err)
	}
	if err := svc.DispatchTask(context.Background(), queue.TaskEmbedProductImage, []byte(`{}`)); err == nil ||
		err.Error() != "embedding: payload missing productImageId" {
		t.Errorf("missing productImageId err = %v", err)
	}
}

// --- DB-backed behaviour --------------------------------------------------------

func startQueueHarness(t *testing.T) *harness.Harness {
	t.Helper()
	h, err := harness.Start(context.Background())
	if err != nil {
		t.Skipf("docker unavailable for queue harness: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })
	repoDir, err := harness.NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	if err := harness.ApplyPrismaMigrations(context.Background(), repoDir, h.PostgresDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return h
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %s: %v", sql, err)
	}
}

func readOptionalVector(t *testing.T, pool *pgxpool.Pool, query string, arg any) ([]float32, bool) {
	t.Helper()
	var txt *string
	if err := pool.QueryRow(context.Background(), query, arg).Scan(&txt); err != nil {
		t.Fatalf("read vector: %v", err)
	}
	if txt == nil {
		return nil, false
	}
	return parseVectorText(*txt), true
}

func parseVectorText(s string) []float32 {
	inner := strings.TrimSuffix(strings.TrimPrefix(s, "["), "]")
	parts := strings.Split(inner, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		f, _ := strconv.ParseFloat(strings.TrimSpace(p), 32)
		out[i] = float32(f)
	}
	return out
}

func floatBitsEqual(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if math.Float32bits(a[i]) != math.Float32bits(b[i]) {
			return false
		}
	}
	return true
}

func vectorLiteral(v []float32) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = fmt.Sprintf("%v", f)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func serveFakeJPEG(t *testing.T) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/a.jpg", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write(bytes.Repeat([]byte{0xFF}, 64))
	})
	return serveOn(t, mux, "")
}

func TestS6EmbedProductWritesExpectedVector(t *testing.T) {
	h := startQueueHarness(t)
	svc, oaiStub, _, pool := newEmbeddingSvc(t, h)
	ctx := context.Background()

	name := "Handwoven Basket"
	desc := "  Large bolga basket \n"
	mustExec(t, pool,
		`INSERT INTO businesses (id, name, whatsapp_phone_number_id, owner_phone, currency, updated_at)
		 VALUES ('ec000001','Biz','wni','+23320000001','GHS',now())`)
	mustExec(t, pool,
		`INSERT INTO products (id, business_id, name, description, price, stock, updated_at)
		 VALUES ('ec000010','ec000001',$1,$2,25.50,2,now())`, name, desc)

	wantDoc := workers.BuildSearchDocument(name, &desc)
	if err := svc.DispatchTask(ctx, queue.TaskEmbedProduct, []byte(`{"productId":"ec000010"}`)); err != nil {
		t.Fatalf("DispatchTask: %v", err)
	}

	// Request contract (T6.16): model/dimensions/encoding identical to AI SDK.
	probes := oaiStub.probes()
	if len(probes) != 1 {
		t.Fatalf("embedding requests = %d, want 1", len(probes))
	}
	if probes[0].Model != "text-embedding-3-small" || probes[0].Dims != 768 || probes[0].EncodingFm != "float" {
		t.Errorf("request = %+v, want text-embedding-3-small @768 float", probes[0])
	}
	if len(probes[0].Input) != 1 || probes[0].Input[0] != wantDoc {
		t.Errorf("input = %q, want search doc %q", probes[0].Input, wantDoc)
	}

	got, ok := readOptionalVector(t, pool, `SELECT embedding::text FROM products WHERE id=$1`, "ec000010")
	if !ok {
		t.Fatal("embedding column still NULL")
	}
	if len(got) != 768 {
		t.Fatalf("dims = %d, want 768", len(got))
	}
	want := harness.Embedding(wantDoc, 768)
	if !floatBitsEqual(got, want) {
		t.Fatal("stored vector differs bit-wise from deterministic expectation")
	}
}

func TestS6EmbedProductNotFoundMessage(t *testing.T) {
	h := startQueueHarness(t)
	svc, _, _, _ := newEmbeddingSvc(t, h)
	err := svc.DispatchTask(context.Background(), queue.TaskEmbedProduct, []byte(`{"productId":"nope"}`))
	if err == nil || err.Error() != "Product not found: nope" {
		t.Errorf("err = %v, want exact TS message", err)
	}
}

func TestS6EmbedImageRowSkipForceAndNotFound(t *testing.T) {
	h := startQueueHarness(t)
	svc, _, gStub, pool := newEmbeddingSvc(t, h)
	ctx := context.Background()

	mustExec(t, pool,
		`INSERT INTO businesses (id, name, whatsapp_phone_number_id, owner_phone, currency, updated_at)
		 VALUES ('ec000002','Biz','wni2','+23320000002','GHS',now())`)
	mustExec(t, pool,
		`INSERT INTO products (id, business_id, name, price, stock, updated_at)
		 VALUES ('ec000020','ec000002','Frame','10.00',1,now())`)

	// Not found carries the exact TS message.
	if err := svc.EmbedProductImageRow(ctx, "missing", false); err == nil ||
		err.Error() != "Product image not found: missing" {
		t.Errorf("err = %v, want exact TS message", err)
	}

	imgURL := serveFakeJPEG(t) + "/a.jpg"
	seedVec := harness.Embedding("seed-a", 768)
	mustExec(t, pool, `INSERT INTO product_images (id, product_id, url, position, embedding, content_hash)
		VALUES ('img-1','ec000020',$1,0,$2,'hash')`, imgURL, vectorLiteral(seedVec))

	// Already-embedded row is skipped without any Google call.
	if err := svc.EmbedProductImageRow(ctx, "img-1", false); err != nil {
		t.Errorf("skip path returned %v, want nil", err)
	}
	if n := len(gStub.probes()); n != 0 {
		t.Fatalf("google called %d times on skip path, want 0", n)
	}

	// force re-embeds through the Google client and overwrites.
	if err := svc.EmbedProductImageRow(ctx, "img-1", true); err != nil {
		t.Fatalf("force path: %v", err)
	}
	probes := gStub.probes()
	if len(probes) != 1 || probes[0].Model != "models/gemini-embedding-001" && probes[0].Model != "gemini-embedding-001" || probes[0].Dims != 768 {
		t.Errorf("google probes = %+v, want gemini @768", probes)
	}
	got, ok := readOptionalVector(t, pool, `SELECT embedding::text FROM product_images WHERE id=$1`, "img-1")
	if !ok {
		t.Fatal("image embedding NULL after force")
	}
	if floatBitsEqual(got, seedVec) {
		t.Error("force did not overwrite the stored vector")
	}
}

func TestS6EmbedAllForBusinessPartialFailureReporting(t *testing.T) {
	h := startQueueHarness(t)
	svc, oaiStub, _, pool := newEmbeddingSvc(t, h)
	ctx := context.Background()

	mustExec(t, pool,
		`INSERT INTO businesses (id, name, whatsapp_phone_number_id, owner_phone, currency, updated_at)
		 VALUES ('ec000003','Biz','wni3','+23320000003','GHS',now())`)
	for i, id := range []string{"ec000031", "ec000032"} {
		mustExec(t, pool,
			`INSERT INTO products (id, business_id, name, price, stock, updated_at)
			 VALUES ($1,'ec000003',$2,'5.00',0,now())`, id, fmt.Sprintf("Item %d", i))
	}

	// Fail exactly one call mid-batch to exercise partial-failure accounting.
	calls := 0
	oaiStub.mu.Lock()
	oaiStub.failOn = func(embedProbe) bool { calls++; return calls == 2 }
	oaiStub.mu.Unlock()

	res, err := svc.EmbedAllForBusiness(ctx, "ec000003")
	if err != nil {
		t.Fatalf("bulk returned hard error: %v", err)
	}
	if res.Total != 2 || res.Succeeded != 1 || res.Failed != 1 || len(res.Errors) != 1 {
		t.Fatalf("bulk result = %+v, want 1 succeeded / 1 failed", res)
	}
	if !strings.Contains(res.Errors[0].Error, "stub outage") {
		t.Errorf("error detail = %q, want upstream message", res.Errors[0].Error)
	}
}

// --- integration client wire shapes (coverage lives here by design) -------------

func TestS6OpenAIClientWireShapeAndErrors(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]any
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		writeJSON(w, map[string]any{"data": []any{map[string]any{"index": 0, "embedding": []float32{1.5, -2.25}}}})
	})
	base := serveOn(t, handler, "/v1") // mirrors OPENAI_BASE_URL=".../v1"

	c := openai.New(openai.Config{APIKey: "sk-x", BaseURL: base})
	vec, err := c.Embedding(context.Background(), "text-embedding-3-small", 768, "hello")
	if err != nil {
		t.Fatalf("Embedding: %v", err)
	}
	if len(vec) != 2 || vec[0] != 1.5 || vec[1] != -2.25 {
		t.Errorf("vec = %v", vec)
	}
	if gotPath != "/v1/embeddings" || gotAuth != "Bearer sk-x" {
		t.Errorf("path/auth = %s / %q", gotPath, gotAuth)
	}
	if gotBody["encoding_format"] != "float" || gotBody["dimensions"] != float64(768) {
		t.Errorf("body = %v, want float encoding + 768 dims", gotBody)
	}

	rateLimited := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`rate limited`))
	})
	ce := openai.New(openai.Config{APIKey: "sk-x", BaseURL: serveOn(t, rateLimited, "")})
	_, err = ce.Embedding(context.Background(), "m", 0, "x")
	var oe *openai.Error
	if !errors.As(err, &oe) || oe.Status != 429 {
		t.Errorf("err = %v, want *openai.Error{429}", err)
	}
}

func TestS6GoogleClientWireShape(t *testing.T) {
	var gotPath, gotKey string
	var gotBody map[string]any
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.Header.Get("x-goog-api-key")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		writeJSON(w, map[string]any{"embedding": map[string]any{"values": []float32{0.5}}})
	})
	base := serveOn(t, handler, "")

	c := google.New(google.Config{APIKey: "gk", BaseURL: base})
	vec, err := c.EmbedContent(context.Background(), "gemini-embedding-001", 768,
		[]google.Part{{InlineData: &google.InlineData{MimeType: "image/jpeg", Data: "QUJD"}}})
	if err != nil || len(vec) != 1 || vec[0] != 0.5 {
		t.Fatalf("vec=%v err=%v", vec, err)
	}
	if gotPath != "/models/gemini-embedding-001:embedContent" || gotKey != "gk" {
		t.Errorf("path/key = %s / %q", gotPath, gotKey)
	}
	if gotBody["outputDimensionality"] != float64(768) || gotBody["model"] != "models/gemini-embedding-001" {
		t.Errorf("body = %v", gotBody)
	}
}

// --- T6.21: VECTOR PARITY ------------------------------------------------------
//
// 1. boot the Node stack (api + worker) against shared pgvector/redis with the
//    deterministic OpenAI stub;
// 2. create a product through the NODE API (signup → business → POST
//    /products), which enqueues BullMQ 'embed-product';
// 3. wait for the Node worker to fill products.embedding;
// 4. run the GO TaskEmbedProduct handler for the SAME productId against the
//    SAME database, pointed at a stub sharing the node stub's vector function;
// 5. assert the pgvector float32 slices are BIT-EXACT.
//
// Because the stub's vector is a deterministic pure function of the input
// text, bit-equality proves the search document was assembled byte-exactly
// (T6.15) AND that both stacks' write paths preserve full precision.

func s6ParityStack(t *testing.T) (*harness.Harness, *harness.NodeStack, *sqlDB) {
	t.Helper()

	sock := os.Getenv("DOCKER_HOST")
	if sock == "" || !strings.HasPrefix(sock, "unix://") {
		sock = "unix:///var/run/docker.sock"
	}
	h, err := harness.Start(context.Background())
	if err != nil {
		t.Skipf("docker unavailable (%s): %v", sock, err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })

	repoDir, err := harness.NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	if err := harness.ApplyPrismaMigrations(context.Background(), repoDir, h.PostgresDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, bin := range []string{
		filepath.Join(repoDir, "dist/apps/api/apps/api/src/main.js"),
		filepath.Join(repoDir, "dist/apps/worker/apps/worker/src/main.js"),
	} {
		if _, err := os.Stat(bin); err != nil {
			t.Skipf("node dist missing (%s) — run `npm run build:all` in %s first", bin, repoDir)
		}
	}

	stack := harness.StartNodeStack(t, h)
	db, err := sqlOpen(h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return h, stack, db
}

func TestS6VectorParityNodeVsGoBitExact(t *testing.T) {
	h, stack, db := s6ParityStack(t)

	productName := "Parity Widget"
	productDesc := "  Hand-carved wooden comb \n"

	token := paritySignupVendor(t, stack, h, "+23359990001", "parity-vendor@test.example")
	biz := parityCreateBusiness(t, stack, token, "Parity Goods")
	created := parityAPIRequest(t, stack.BaseURL, http.MethodPost, "/products", map[string]any{
		"name":        productName,
		"description": productDesc,
		"price":       49.99,
		"stock":       3,
	}, token)
	if created.Status != http.StatusOK && created.Status != http.StatusCreated {
		t.Fatalf("POST /products status=%d body=%s", created.Status, created.Body)
	}
	var product struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body, &product); err != nil || product.ID == "" {
		t.Fatalf("product id missing in response: %s", created.Body)
	}
	_ = biz

	// Wait for the NODE worker to embed (BullMQ embed-product job).
	deadline := time.Now().Add(120 * time.Second)
	nodeVecText := ""
	for time.Now().Before(deadline) {
		var txt *string
		if err := db.QueryRow(
			`SELECT embedding::text FROM products WHERE id = $1`, product.ID).Scan(&txt); err != nil {
			t.Fatalf("query embedding: %v", err)
		}
		if txt != nil {
			nodeVecText = *txt
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if nodeVecText == "" {
		t.Fatalf("node worker never filled products.embedding within timeout\n--- logs ---\n%s", stack.DumpLogs())
	}

	wantDoc := workers.BuildSearchDocument(productName, &productDesc)
	wantVec := harness.Embedding(wantDoc, 768)
	nodeVec := parseVectorText(nodeVecText)
	if len(nodeVec) != 768 {
		t.Fatalf("node vector dims = %d, want 768", len(nodeVec))
	}
	if !floatBitsEqual(nodeVec, wantVec) {
		t.Fatal("node-written vector differs from deterministic expectation — input text or write path diverged")
	}

	// GO side: same DB row, own pool + client over a stub sharing the vector fn.
	svc, _, _, pool := newEmbeddingSvc(t, h)
	ctx := context.Background()
	if err := svc.DispatchTask(ctx, queue.TaskEmbedProduct,
		[]byte(fmt.Sprintf(`{"productId":%q}`, product.ID))); err != nil {
		t.Fatalf("go handler: %v", err)
	}

	goVec, ok := readOptionalVector(t, pool,
		`SELECT embedding::text FROM products WHERE id=$1`, product.ID)
	if !ok {
		t.Fatal("go handler left embedding NULL")
	}

	if !floatBitsEqual(nodeVec, goVec) {
		diffIdx := -1
		for i := range nodeVec {
			if math.Float32bits(nodeVec[i]) != math.Float32bits(goVec[i]) {
				diffIdx = i
				break
			}
		}
		t.Fatalf("VECTORS NOT BIT-EXACT (first diff at [%d]: node=%v go=%v)", diffIdx, nodeVec[diffIdx], goVec[diffIdx])
	}
	t.Logf("PARITY OK: 768 float32 values bit-exact for product %s (search doc %q)", product.ID, wantDoc)
}

// --- minimal local API helpers (harness test-only helpers can't be imported) ---

type parityResponse struct {
	Status int
	Body   []byte
}

func parityAPIRequest(t *testing.T, baseURL, method, path string, body any, token string) parityResponse {
	t.Helper()
	var rdr io.Reader = bytes.NewReader(nil)
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, baseURL+path, rdr)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return parityResponse{Status: resp.StatusCode, Body: raw}
}

func paritySignupVendor(t *testing.T, stack *harness.NodeStack, h *harness.Harness, phone, email string) string {
	t.Helper()
	r := parityAPIRequest(t, stack.BaseURL, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": phone, "deliveryMethod": "email", "email": email}, "")
	if r.Status != http.StatusOK {
		t.Fatalf("request-otp status=%d body=%s", r.Status, r.Body)
	}
	stack.SMTP.WaitForMessage(t, email, 20*time.Second)
	code := parityRedisGet(t, h.RedisAddr, "otp:"+phone)
	if len(code) != 6 {
		t.Fatalf("OTP for %s missing/invalid: %q", phone, code)
	}
	vr := parityAPIRequest(t, stack.BaseURL, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": phone, "code": code}, "")
	if vr.Status != http.StatusOK {
		t.Fatalf("verify-otp status=%d body=%s", vr.Status, vr.Body)
	}
	var parsed map[string]any
	if err := json.Unmarshal(vr.Body, &parsed); err != nil {
		t.Fatalf("verify-otp non-json: %s", vr.Body)
	}
	token, _ := parsed["token"].(string)
	if token == "" {
		t.Fatalf("no token: %s", vr.Body)
	}
	return token
}

func parityCreateBusiness(t *testing.T, stack *harness.NodeStack, token, name string) map[string]any {
	t.Helper()
	r := parityAPIRequest(t, stack.BaseURL, http.MethodPost, "/businesses",
		map[string]any{"name": name, "currency": "GHS",
			"whatsappPhoneNumberId": fmt.Sprintf("wni_parity_%d", time.Now().UnixNano()%1e9)}, token)
	if r.Status != http.StatusOK && r.Status != http.StatusCreated {
		t.Fatalf("POST /businesses status=%d body=%s", r.Status, r.Body)
	}
	var biz map[string]any
	_ = json.Unmarshal(r.Body, &biz)
	return biz
}

func parityRedisGet(t *testing.T, addr, key string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial redis: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	auth := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(harness.RedisPassword), harness.RedisPassword)
	get := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
	if _, err := conn.Write([]byte(auth + get)); err != nil {
		t.Fatalf("redis write: %v", err)
	}
	rd := bufio.NewReader(conn)
	var reply strings.Builder
	for {
		line, err := rd.ReadString('\n')
		reply.WriteString(line)
		if err != nil || strings.Count(reply.String(), "\r\n") >= 3 {
			break
		}
	}
	parts := strings.Split(strings.TrimRight(reply.String(), "\r\n"), "\r\n")
	if len(parts) < 3 {
		t.Fatalf("unexpected redis reply %q", reply.String())
	}
	if !strings.HasPrefix(parts[0], "+OK") {
		t.Fatalf("redis auth failed: %q", parts[0])
	}
	valLine := parts[1]
	if valLine == "$-1" {
		return ""
	}
	var n int
	if _, err := fmt.Sscanf(valLine, "$%d", &n); err != nil || n < 0 || len(parts) < 3 {
		t.Fatalf("parse bulk length %q: %v", valLine, err)
	}
	value := parts[2]
	if len(value) != n {
		t.Fatalf("bulk value length = %d, want %d", len(value), n)
	}
	return value
}
