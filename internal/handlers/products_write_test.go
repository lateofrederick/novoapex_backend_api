package handlers

// products_write_test.go — Stage 4A write-surface coverage (T4.4–T4.7,
// T4.23c) plus the s4p_* harness shared by businesses_test.go and
// images_test.go: real Postgres via the shared containers, production auth
// middleware for claim injection, and stubbed StorageProvider/EmbedPublisher
// seams whose call counters double as behavioral assertions.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/storage"
)

const s4pJWTSecret = "0123456789abcdef0123456789abcdef" // == harness Node stack JWT_SECRET

type s4pEnv struct {
	T    *testing.T
	H    *harness.Harness
	DB   *sql.DB
	Pool *pgxpool.Pool
	F    *harness.Factory
}

func s4p_start(t *testing.T) *s4pEnv {
	t.Helper()
	ctx := context.Background()

	h, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })

	repoDir, err := harness.NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	if err := harness.ApplyPrismaMigrations(ctx, repoDir, h.PostgresDSN); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open fixtures db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	pool, err := pgxpool.New(ctx, h.PostgresDSN)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return &s4pEnv{T: t, H: h, DB: db, Pool: pool, F: harness.NewFactory(t, db)}
}

// s4p_withClaims wraps a handler tree with the PRODUCTION auth.Middleware;
// requests carry claims through the real context mechanism.
func s4p_withClaims(h http.Handler) http.Handler {
	return auth.Middleware(s4pJWTSecret, nil)(h)
}

// s4p_mint mints an HS256 token with the Node claim set ({phone, businessId}).
func s4p_mint(t *testing.T, claims auth.Claims) string {
	t.Helper()
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"phone":      claims.Phone,
		"businessId": claims.BusinessID,
		"exp":        time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(s4pJWTSecret))
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	return signed
}

// s4p_do performs an authenticated request and returns the recorder.
func s4p_do(t *testing.T, h http.Handler, method, target string, claims *auth.Claims, contentType string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if claims != nil {
		req.Header.Set("Authorization", "Bearer "+s4p_mint(t, *claims))
	}
	rec := httptest.NewRecorder()
	s4p_withClaims(h).ServeHTTP(rec, req)
	return rec
}

// s4p_json marshals body and performs a JSON request.
func s4p_json(t *testing.T, h http.Handler, method, target string, claims *auth.Claims, body any) *httptest.ResponseRecorder {
	t.Helper()
	if body == nil {
		return s4p_do(t, h, method, target, claims, "application/json", nil)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return s4p_do(t, h, method, target, claims, "application/json", raw)
}

// s4p_decode decodes a JSON response preserving json.Number.
func s4p_decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var v map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("decode status=%d: %v\nbody: %s", rec.Code, err, rec.Body.String())
	}
	return v
}

// ---- stubbed seams ---------------------------------------------------------

// s4p_stubStorage implements storage.StorageProvider with call counters; it
// stands in for Cloudinary so dedup/race behavior is observable without
// network I/O.
type s4p_stubStorage struct {
	mu       sync.Mutex
	uploads  int
	deletes  int
	folders  []string
	failNext bool // fail exactly one upload when true
}

func (s *s4p_stubStorage) ProviderName() string { return "stub" }

func (s *s4p_stubStorage) UploadImage(_ context.Context, _ []byte, folder string) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads++
	n := s.uploads
	s.folders = append(s.folders, folder)
	if s.failNext {
		s.failNext = false
		return "", "", fmt.Errorf("cloudinary exploded")
	}
	return fmt.Sprintf("https://res.example.com/stub/%d.png", n),
		fmt.Sprintf("stub/%d", n), nil
}

func (s *s4p_stubStorage) DeleteImage(_ context.Context, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	return nil
}

func (s *s4p_stubStorage) snapshot() (uploads, deletes int, folders []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploads, s.deletes, append([]string(nil), s.folders...)
}

// s4p_recordingEmbedder captures the T4.1 enqueue hook points.
type s4p_recordingEmbedder struct {
	mu         sync.Mutex
	productIDs []string
	imageIDs   []string
}

func (e *s4p_recordingEmbedder) PublishProductEmbed(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.productIDs = append(e.productIDs, id)
	return nil
}

func (e *s4p_recordingEmbedder) PublishImageEmbed(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.imageIDs = append(e.imageIDs, id)
	return nil
}

// ---- mounting ---------------------------------------------------------------

func s4p_productsHandler(e *s4pEnv, sp storage.StorageProvider, emb storage.EmbedPublisher) http.Handler {
	r := chi.NewRouter()
	r.Route("/products", func(pr chi.Router) { MountProductsWrite(pr, ProductsDeps{Pool: e.Pool, Storage: sp, Embeds: emb}) })
	return s4p_withClaims(r)
}

func s4p_businessesHandler(e *s4pEnv) http.Handler {
	r := chi.NewRouter()
	r.Route("/businesses", func(b chi.Router) { MountBusinesses(b, BusinessesDeps{Pool: e.Pool}) })
	return s4p_withClaims(r)
}

// ---- seeding helpers --------------------------------------------------------

func s4p_sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

var s4pSeq int

func s4p_pngBytes(tag string) []byte {
	s4pSeq++
	return append([]byte("\x89PNG\r\n\x1a\n"), fmt.Appendf(nil, "%s-%d", tag, s4pSeq)...)
}

// s4p_seedImage inserts a product_images row directly, with explicit control
// over storage_key/content_hash ("" -> NULL, i.e. legacy-shaped rows).
func s4p_seedImage(t *testing.T, e *s4pEnv, id, productID, url, storageKey, contentHash string, position int) {
	t.Helper()
	if _, err := e.DB.Exec(`INSERT INTO product_images
		(id, product_id, url, storage_key, content_hash, position)
		VALUES ($1, $2, $3, NULLIF($4,''), NULLIF($5,''), $6)`,
		id, productID, url, storageKey, contentHash, position); err != nil {
		t.Fatalf("seed image %s: %v", id, err)
	}
}

func s4p_imageCount(t *testing.T, e *s4pEnv, productID string) int {
	t.Helper()
	var n int
	if err := e.DB.QueryRow(`SELECT COUNT(*) FROM product_images WHERE product_id = $1`,
		productID).Scan(&n); err != nil {
		t.Fatalf("count images: %v", err)
	}
	return n
}

// s4p_positions returns the stored positions oldest-first; they MUST be dense.
func s4p_positions(t *testing.T, e *s4pEnv, productID string) []int {
	t.Helper()
	rows, err := e.DB.Query(`SELECT position FROM product_images WHERE product_id = $1 ORDER BY position`, productID)
	if err != nil {
		t.Fatalf("query positions: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []int
	for rows.Next() {
		var p int
		if err := rows.Scan(&p); err != nil {
			t.Fatalf("scan position: %v", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func s4p_assertDensePositions(t *testing.T, e *s4pEnv, productID string) {
	t.Helper()
	ps := s4p_positions(t, e, productID)
	for i, p := range ps {
		if p != i {
			t.Fatalf("positions not dense for %s: %v", productID, ps)
		}
	}
}

// ---- shared assertion sugar --------------------------------------------------

func s4p_status(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d\nbody: %s", rec.Code, want, rec.Body.String())
	}
}

func s4p_message(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	v := s4p_decode(t, rec)
	msg, ok := v["message"].(string)
	if !ok {
		t.Fatalf("envelope without string message: %s", rec.Body.String())
	}
	return msg
}

func s4p_issues(t *testing.T, rec *httptest.ResponseRecorder) []map[string]any {
	t.Helper()
	v := s4p_decode(t, rec)
	raw, ok := v["errors"].([]any)
	if !ok {
		t.Fatalf("expected zod errors array: %s", rec.Body.String())
	}
	out := make([]map[string]any, 0, len(raw))
	for _, i := range raw {
		m, ok := i.(map[string]any)
		if !ok {
			t.Fatalf("issue not object: %v", i)
		}
		out = append(out, m)
	}
	return out
}

func s4p_str(m map[string]any, key string) string {
	s, ok := m[key].(string)
	if !ok {
		panic(fmt.Sprintf("key %q not a string: %v", key, m[key]))
	}
	return s
}

// ---- CRUD tests (T4.4–T4.7, T4.23c) -----------------------------------------

// sorted to match ep_keys' output order
var s4pProductKeys = []string{
	"businessId", "category", "createdAt", "deliveryNote", "description", "id",
	"imageUrl", "images", "isAvailable", "name", "price", "requestCount",
	"sku", "stock", "stockNote", "updatedAt",
}

func TestS4pProductsCRUDLifecycle(t *testing.T) {
	e := s4p_start(t)
	biz := e.F.Business()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	h := s4p_productsHandler(e, &s4p_stubStorage{}, &s4p_recordingEmbedder{})

	// CREATE — full Prisma payload, defaults filled, bare money number.
	rec := s4p_json(t, h, http.MethodPost, "/products", &claims, map[string]any{
		"name": "Kelewele", "price": json.Number("12.50"), "stock": 4, "description": "spicy",
	})
	s4p_status(t, rec, http.StatusCreated)
	body := s4p_decode(t, rec)
	if got, want := strings.Join(ep_keys(t, body), ","), strings.Join(s4pProductKeys, ","); got != want {
		t.Fatalf("create keys:\n got %s\nwant %s", got, want)
	}
	id := s4p_str(body, "id")
	ep_assertMoney(t, body["price"], "12.50")
	if body["isAvailable"] != true || body["requestCount"] != json.Number("0") ||
		body["images"] == nil || body["imageUrl"] != nil ||
		body["sku"] != nil || body["category"] != nil {
		t.Fatalf("unexpected create defaults: %v", body)
	}

	// PATCH — partial spread: untouched columns survive verbatim.
	rec = s4p_json(t, h, http.MethodPatch, "/products/"+id, &claims, map[string]any{"name": "Kelewele X"})
	s4p_status(t, rec, http.StatusOK)
	patched := s4p_decode(t, rec)
	if s4p_str(patched, "name") != "Kelewele X" || s4p_str(patched, "description") != "spicy" {
		t.Fatalf("patch clobbered fields: %v", patched)
	}
	ep_assertMoney(t, patched["price"], "12.50")

	// 404s: unknown id AND another tenant's product.
	other := e.F.Business()
	otherProd := e.F.Product(other.ID)
	for _, pid := range []string{"prod-does-not-exist", otherProd.ID} {
		rec = s4p_json(t, h, http.MethodPatch, "/products/"+pid, &claims, map[string]any{"name": "Nope"})
		s4p_status(t, rec, http.StatusNotFound)
		if msg := s4p_message(t, rec); msg != "Product not found" {
			t.Fatalf("patch 404 message = %q", msg)
		}
	}

	// OUT OF STOCK — stock 0 + isAvailable false in one statement.
	rec = s4p_json(t, h, http.MethodPost, "/products/"+id+"/out-of-stock", &claims, nil)
	s4p_status(t, rec, http.StatusCreated)
	oos := s4p_decode(t, rec)
	if oos["stock"] != json.Number("0") || oos["isAvailable"] != false {
		t.Fatalf("out-of-stock state wrong: %v", oos)
	}

	// DELETE — cascade + best-effort asset cleanup, message-only body.
	rec = s4p_json(t, h, http.MethodDelete, "/products/"+id, &claims, nil)
	s4p_status(t, rec, http.StatusOK)
	if msg := s4p_message(t, rec); msg != "Product deleted successfully" {
		t.Fatalf("delete message = %q", msg)
	}
	var n int
	if err := e.DB.QueryRow(`SELECT COUNT(*) FROM products WHERE id = $1`, id).Scan(&n); err != nil || n != 0 {
		t.Fatalf("product not deleted (n=%d err=%v)", n, err)
	}
	rec = s4p_json(t, h, http.MethodDelete, "/products/"+id, &claims, nil)
	s4p_status(t, rec, http.StatusNotFound)
}

func TestS4pProductsCreateValidationZodShapes(t *testing.T) {
	e := s4p_start(t)
	biz := e.F.Business()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	h := s4p_productsHandler(e, &s4p_stubStorage{}, &s4p_recordingEmbedder{})

	cases := []struct {
		name    string
		body    map[string]any
		code    string
		path    string
		message string
	}{
		{"price string", map[string]any{"name": "Ok", "price": "12.5", "stock": 1},
			"invalid_type", "price", "Invalid input: expected number, received string"},
		{"price zero", map[string]any{"name": "Ok", "price": 0, "stock": 1},
			"too_small", "price", "Too small: expected number to be >0"},
		{"price negative", map[string]any{"name": "Ok", "price": -2, "stock": 1},
			"too_small", "price", "Too small: expected number to be >0"},
		{"stock float", map[string]any{"name": "Ok", "price": 1, "stock": 1.5},
			"invalid_type", "stock", "Invalid input: expected int, received number"},
		{"stock negative", map[string]any{"name": "Ok", "price": 1, "stock": -1},
			"too_small", "stock", "Too small: expected number to be >=0"},
		{"name short", map[string]any{"name": "K", "price": 1, "stock": 1},
			"too_small", "name", "Too small: expected string to have >=2 characters"},
		{"name number", map[string]any{"name": 7, "price": 1, "stock": 1},
			"invalid_type", "name", "Invalid input: expected string, received number"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := s4p_json(t, h, http.MethodPost, "/products", &claims, tc.body)
			s4p_status(t, rec, http.StatusBadRequest)
			v := s4p_decode(t, rec)
			if v["message"] != "Validation failed" || v["statusCode"] != json.Number("400") {
				t.Fatalf("bad envelope: %s", rec.Body.String())
			}
			issues := s4p_issues(t, rec)
			if len(issues) != 1 {
				t.Fatalf("want 1 issue, got %s", rec.Body.String())
			}
			is := issues[0]
			if s4p_str(is, "code") != tc.code || s4p_str(is, "message") != tc.message {
				t.Fatalf("issue mismatch: %s", rec.Body.String())
			}
			path, ok := is["path"].([]any)
			if !ok || len(path) != 1 || path[0] != tc.path {
				t.Fatalf("path mismatch: %s", rec.Body.String())
			}
		})
	}

	// Missing required fields accumulate ALL issues in schema-key order.
	rec := s4p_json(t, h, http.MethodPost, "/products", &claims, map[string]any{})
	s4p_status(t, rec, http.StatusBadRequest)
	issues := s4p_issues(t, rec)
	if len(issues) != 3 {
		t.Fatalf("want 3 issues, got %s", rec.Body.String())
	}
	wantPaths := []string{"name", "price", "stock"}
	for i, w := range wantPaths {
		p, _ := issues[i]["path"].([]any)
		if len(p) != 1 || p[0] != w || s4p_str(issues[i], "code") != "invalid_type" ||
			s4p_str(issues[i], "message") != "Invalid input: expected "+map[string]string{
				"name": "string", "price": "number", "stock": "number",
			}[w]+", received undefined" {
			t.Fatalf("issue %d mismatch: %s", i, rec.Body.String())
		}
	}

	// No businessId claim -> the @BusinessId guard's 403.
	noBiz := auth.Claims{Phone: "+233200000001"}
	rec = s4p_json(t, h, http.MethodPost, "/products", &noBiz, map[string]any{"name": "Xx", "price": 1, "stock": 0})
	s4p_status(t, rec, http.StatusForbidden)
	if msg := s4p_message(t, rec); msg != "No business is associated with this account. Create a business first." {
		t.Fatalf("403 message = %q", msg)
	}
}

func TestS4pProductsUpdateEmbedHooksAndConcurrency(t *testing.T) {
	e := s4p_start(t)
	biz := e.F.Business()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	emb := &s4p_recordingEmbedder{}
	h := s4p_productsHandler(e, &s4p_stubStorage{}, emb)

	rec := s4p_json(t, h, http.MethodPost, "/products", &claims, map[string]any{
		"name": "Embedme", "price": 5, "stock": 2,
	})
	s4p_status(t, rec, http.StatusCreated)
	id := s4p_str(s4p_decode(t, rec), "id")

	expectProductEmbeds := func(want int) {
		t.Helper()
		emb.mu.Lock()
		defer emb.mu.Unlock()
		if len(emb.productIDs) != want {
			t.Fatalf("product embeds = %d, want %d (%v)", len(emb.productIDs), want, emb.productIDs)
		}
	}
	expectProductEmbeds(1) // create

	s4p_status(t, s4p_json(t, h, http.MethodPatch, "/products/"+id, &claims,
		map[string]any{"stock": 9}), http.StatusOK)
	expectProductEmbeds(2) // every update re-enqueues

	s4p_status(t, s4p_json(t, h, http.MethodPost, "/products/"+id+"/out-of-stock", &claims, nil),
		http.StatusCreated)
	expectProductEmbeds(3) // markOutOfStock routes through update

	s4p_status(t, s4p_json(t, h, http.MethodDelete, "/products/"+id, &claims, nil), http.StatusOK)
	expectProductEmbeds(3) // deletion enqueues nothing

	// T4.23c — simultaneous product updates: both succeed, per-field
	// last-writer-wins, no lost updates or 500s.
	prod := e.F.Product(biz.ID)
	const rounds = 10
	errCh := make(chan int, rounds*2)
	var wg sync.WaitGroup
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			rec := s4p_json(t, h, http.MethodPatch, "/products/"+prod.ID, &claims, map[string]any{"name": "Writer-A"})
			errCh <- rec.Code
		}()
		go func() {
			defer wg.Done()
			rec := s4p_json(t, h, http.MethodPatch, "/products/"+prod.ID, &claims, map[string]any{"description": "Writer-B"})
			errCh <- rec.Code
		}()
	}
	wg.Wait()
	close(errCh)
	for code := range errCh {
		if code != http.StatusOK {
			t.Fatalf("concurrent patch returned %d", code)
		}
	}
	var name, desc string
	if err := e.DB.QueryRow(`SELECT name, COALESCE(description,'') FROM products WHERE id = $1`,
		prod.ID).Scan(&name, &desc); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if name != "Writer-A" || desc != "Writer-B" {
		t.Fatalf("final state inconsistent: name=%q desc=%q", name, desc)
	}
}
