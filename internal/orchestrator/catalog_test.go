package orchestrator_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pgvector/pgvector-go"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
)

// ---------------------------------------------------------------------------
// shared DB harness: ONE postgres container for every cat_ test (one per test
// would dominate runtime); isolation comes from per-test businesses.
// ---------------------------------------------------------------------------

type catEnv struct {
	pool *pgxpool.Pool
	db   *sql.DB
}

var (
	catOnce   sync.Once
	catShared *catEnv
	catErr    error
)

func catDockerAlive(t *testing.T) {
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

func catStart(t *testing.T) *catEnv {
	t.Helper()
	catDockerAlive(t)
	catOnce.Do(func() {
		bg := context.Background()
		env, err := harness.Start(bg)
		if err != nil {
			catErr = err
			return
		}
		if err := harness.ApplyBaselineSchema(bg, env.PostgresDSN); err != nil {
			catErr = err
			return
		}
		pool, err := pgxpool.New(bg, env.PostgresDSN)
		if err != nil {
			catErr = err
			return
		}
		db, err := sql.Open("pgx", env.PostgresDSN)
		if err != nil {
			catErr = err
			return
		}
		catShared = &catEnv{pool: pool, db: db}
	})
	if catErr != nil {
		t.Fatalf("cat harness: %v", catErr)
	}
	return catShared
}

func (e *catEnv) exec(t *testing.T, label, query string, args ...any) {
	t.Helper()
	if _, err := e.db.Exec(query, args...); err != nil {
		t.Fatalf("seed %s: %v", label, err)
	}
}

func (e *catEnv) newBusiness(t *testing.T, currency string) string {
	t.Helper()
	id := uuid.NewString()
	e.exec(t, "business", `INSERT INTO businesses
		(id, name, whatsapp_phone_number_id, owner_phone, currency, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id, "Cat Test Biz "+id[:8], uuid.NewString(), "+233"+id[:9], currency, time.Now().UTC())
	return id
}

type catProductSeed struct {
	name        string
	price       string
	stock       int
	description *string
	hidden      bool      // true ⇒ is_available=false (excluded from every section)
	embedding   []float32 // nil ⇒ column left NULL
	withImage   bool      // adds one product_images row WITHOUT embedding
}

func strPtr(s string) *string { return &s }

func (e *catEnv) seedProduct(t *testing.T, businessID string, s catProductSeed) string {
	t.Helper()
	id := uuid.NewString()
	e.exec(t, "product", `INSERT INTO products
		(id, business_id, name, price, stock, description, is_available, embedding, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		id, businessID, s.name, s.price, s.stock, s.description, !s.hidden,
		nilIfEmpty(s.embedding), time.Now().UTC())
	if s.withImage {
		e.seedImage(t, id, "https://res.example.com/"+id+".jpg", 0, nil)
	}
	return id
}

func (e *catEnv) seedFAQ(t *testing.T, businessID, question, answer string, embedding []float32) string {
	t.Helper()
	id := uuid.NewString()
	e.exec(t, "faq", `INSERT INTO faqs
		(id, business_id, question, answer, policy_tag, embedding, updated_at)
		VALUES ($1, $2, $3, $4, 'delivery', $5, $6)`,
		id, businessID, question, answer, nilIfEmpty(embedding), time.Now().UTC())
	return id
}

func (e *catEnv) seedImage(t *testing.T, productID, url string, position int, embedding []float32) string {
	t.Helper()
	id := uuid.NewString()
	e.exec(t, "product_image", `INSERT INTO product_images
		(id, product_id, url, position, content_hash, embedding)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id, productID, url, position, uuid.NewString(), nilIfEmpty(embedding))
	return id
}

func nilIfEmpty(v []float32) any {
	if len(v) == 0 {
		return nil
	}
	return pgvector.NewVector(v)
}

// ---------------------------------------------------------------------------
// deterministic OpenAI stub: vector := harness.Embedding(input[0], 768)
// ---------------------------------------------------------------------------

type catEmbedProbe struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dims       int      `json:"dimensions"`
	EncodingFM string   `json:"encoding_format"`
}

type catOpenAIStub struct {
	mu     sync.Mutex
	probes []catEmbedProbe
}

func (s *catOpenAIStub) recorded() []catEmbedProbe {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]catEmbedProbe(nil), s.probes...)
}

func catStartOpenAIStub(t *testing.T) (*catOpenAIStub, *openai.Client) {
	t.Helper()
	stub := &catOpenAIStub{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var probe catEmbedProbe
		_ = json.Unmarshal(body, &probe)
		stub.mu.Lock()
		stub.probes = append(stub.probes, probe)
		stub.mu.Unlock()
		text := ""
		if len(probe.Input) > 0 {
			text = probe.Input[0]
		}
		resp, _ := json.Marshal(map[string]any{
			"object": "list",
			"data": []any{map[string]any{
				"object":    "embedding",
				"index":     0,
				"embedding": harness.Embedding(text, 768),
			}},
			"model": probe.Model,
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(resp)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("stub listen: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	client := openai.New(openai.Config{
		APIKey:  "cat-test-key",
		BaseURL: "http://" + ln.Addr().String() + "/v1",
	})
	return stub, client
}

// ---------------------------------------------------------------------------
// T8.7 formatProduct golden strings (catalog-retriever.service.spec.ts:81-175
// plus currency/locale/edge cases). Pure — no DB.
// ---------------------------------------------------------------------------

func TestCatFormatProductGolden(t *testing.T) {
	cases := []struct {
		name     string
		row      orchestrator.ProductRow
		currency string
		want     string
	}{
		{
			name: "cat_golden_full_line_ghs",
			row: orchestrator.ProductRow{
				ID: "abcdefgh-1234-5678-9012-ijklmnopqrst", Name: "Nike Air Max",
				Price: strPtr("45000.00"), Stock: 12, Description: strPtr("Comfortable running shoes"),
			},
			currency: "GHS",
			want:     "[ID: abcdefgh] Nike Air Max — GH₵45,000 (12 available): Comfortable running shoes",
		},
		{
			name: "cat_golden_stock_zero_suffix_absent",
			row: orchestrator.ProductRow{
				ID: "abcdefgh-0000", Name: "Canvas Bag",
				Price: strPtr("3500.00"), Stock: 0, Description: strPtr("Eco-friendly bag"),
			},
			currency: "GHS",
			want:     "[ID: abcdefgh] Canvas Bag — GH₵3,500: Eco-friendly bag",
		},
		{
			name: "cat_golden_description_null_omitted",
			row: orchestrator.ProductRow{
				ID: "abcdefgh-0000", Name: "Mystery Box",
				Price: strPtr("10000.00"), Stock: 5,
			},
			currency: "GHS",
			want:     "[ID: abcdefgh] Mystery Box — GH₵10,000 (5 available)",
		},
		{
			name: "cat_golden_description_whitespace_only_omitted",
			row: orchestrator.ProductRow{
				ID: "abcdefgh-0000", Name: "Item",
				Price: strPtr("100.00"), Stock: 1, Description: strPtr("   "),
			},
			currency: "GHS",
			want:     "[ID: abcdefgh] Item — GH₵100 (1 available)",
		},
		{
			name: "cat_golden_free_product_renders_symbol_zero",
			row: orchestrator.ProductRow{
				ID: "freeitem-0000", Name: "Free Sticker",
				Price: strPtr("0.00"), Stock: 100, Description: strPtr("Promotional sticker"),
			},
			currency: "GHS",
			want:     "[ID: freeitem] Free Sticker — GH₵0 (100 available): Promotional sticker",
		},
		{
			name: "cat_golden_short_id_slice_is_first_eight_chars",
			row: orchestrator.ProductRow{
				ID: "x1y2z3w4-rest-of-uuid", Name: "Test",
				Price: strPtr("1.00"),
			},
			currency: "GHS",
			want:     "[ID: x1y2z3w4] Test — GH₵1",
		},
		{
			name:     "cat_golden_ngn_symbol",
			row:      orchestrator.ProductRow{ID: "123", Name: "Item", Price: strPtr("5000.00")},
			currency: "NGN",
			want:     "[ID: 123] Item — ₦5,000",
		},
		{
			name:     "cat_golden_usd_symbol",
			row:      orchestrator.ProductRow{ID: "123", Name: "Item", Price: strPtr("1500.00")},
			currency: "USD",
			want:     "[ID: 123] Item — $1,500",
		},
		{
			name:     "cat_golden_unknown_currency_falls_back_to_ghs",
			row:      orchestrator.ProductRow{ID: "123", Name: "Item", Price: strPtr("7000.00")},
			currency: "EUR",
			want:     "[ID: 123] Item — GH₵7,000",
		},
		{
			name:     "cat_golden_decimal_price_drops_trailing_zeros_like_js_number",
			row:      orchestrator.ProductRow{ID: "dec-0001", Name: "Soap", Price: strPtr("25.50"), Stock: 3},
			currency: "GHS",
			want:     "[ID: dec-0001] Soap — GH₵25.5 (3 available)",
		},
		{
			name: "cat_golden_has_image_marker_between_stock_and_description",
			row: orchestrator.ProductRow{
				ID: "img-00001", Name: "Shea Butter",
				Price: strPtr("90.00"), Stock: 4, HasImage: true, Description: strPtr("Raw organic"),
			},
			currency: "GHS",
			want:     "[ID: img-0000] Shea Butter — GH₵90 (4 available) [HAS_IMAGE]: Raw organic",
		},
		{
			name: "cat_golden_has_image_marker_after_price_when_no_stock",
			row: orchestrator.ProductRow{
				ID: "img-00002", Name: "Black Soap", Price: strPtr("15.00"), HasImage: true,
			},
			currency: "GHS",
			want:     "[ID: img-0000] Black Soap — GH₵15 [HAS_IMAGE]",
		},
		{
			name: "cat_golden_long_description_preserved_verbatim_no_truncation",
			row: orchestrator.ProductRow{
				ID: "longdesc1", Name: "Bundle",
				Price: strPtr("120.00"), Stock: 2,
				Description: strPtr(strings.Repeat("very moisturizing ", 40) + "end"),
			},
			currency: "GHS",
			want: "[ID: longdesc] Bundle — GH₵120 (2 available): " +
				strings.Repeat("very moisturizing ", 40) + "end",
		},
		{
			name: "cat_golden_description_trimmed_both_ends_only",
			row: orchestrator.ProductRow{
				ID: "trim-0001", Name: "Trim",
				Price: strPtr("5.00"), Stock: 1, Description: strPtr("  padded text  "),
			},
			currency: "GHS",
			want:     "[ID: trim-000] Trim — GH₵5 (1 available): padded text",
		},
		{
			name:     "cat_golden_null_price_renders_symbol_zero",
			row:      orchestrator.ProductRow{ID: "noprice1", Name: "Ghost"},
			currency: "GHS",
			want:     "[ID: noprice1] Ghost — GH₵0",
		},
		{
			name:     "cat_golden_million_grouping",
			row:      orchestrator.ProductRow{ID: "big-00001", Name: "Land", Price: strPtr("1000000.00"), Stock: 1},
			currency: "GHS",
			want:     "[ID: big-0000] Land — GH₵1,000,000 (1 available)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := orchestrator.FormatProduct(tc.row, tc.currency); got != tc.want {
				t.Errorf("FormatProduct mismatch:\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T8.8 getFullCatalog against real Postgres: ordering, header wording,
// availability filter, empty branch.
// ---------------------------------------------------------------------------

func TestCatFullCatalogSection(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()

	t.Run("cat_full_catalog_orders_by_name_and_formats_all", func(t *testing.T) {
		biz := env.newBusiness(t, "GHS")
		zeta := env.seedProduct(t, biz, catProductSeed{name: "Zeta", price: "3000.00", stock: 1})
		alpha := env.seedProduct(t, biz, catProductSeed{
			name: "Alpha", price: "5000.00", stock: 10,
			description: strPtr("First item"), withImage: true,
		})
		mango := env.seedProduct(t, biz, catProductSeed{name: "Mango", price: "8000.00"})

		got, err := orchestrator.GetFullCatalog(ctx, env.pool, biz, "GHS")
		if err != nil {
			t.Fatalf("GetFullCatalog: %v", err)
		}
		want := "=== CATALOG (3 products — showing all) ===\n" +
			"[ID: " + alpha[:8] + "] Alpha — GH₵5,000 (10 available) [HAS_IMAGE]: First item\n" +
			"[ID: " + mango[:8] + "] Mango — GH₵8,000\n" +
			"[ID: " + zeta[:8] + "] Zeta — GH₵3,000 (1 available)\n"
		if got != want {
			t.Errorf("full catalog mismatch:\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("cat_full_catalog_excludes_unavailable_products", func(t *testing.T) {
		biz := env.newBusiness(t, "GHS")
		env.seedProduct(t, biz, catProductSeed{name: "AAA Hidden", price: "10.00", hidden: true})
		visible := env.seedProduct(t, biz, catProductSeed{name: "Visible", price: "20.00", stock: 2})

		got, err := orchestrator.GetFullCatalog(ctx, env.pool, biz, "GHS")
		if err != nil {
			t.Fatalf("GetFullCatalog: %v", err)
		}
		want := "=== CATALOG (1 products — showing all) ===\n" +
			"[ID: " + visible[:8] + "] Visible — GH₵20 (2 available)\n"
		if got != want {
			t.Errorf("unavailable leak:\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("cat_full_catalog_empty_branch_wording", func(t *testing.T) {
		biz := env.newBusiness(t, "GHS")
		got, err := orchestrator.GetFullCatalog(ctx, env.pool, biz, "GHS")
		if err != nil {
			t.Fatalf("GetFullCatalog: %v", err)
		}
		want := "=== CATALOG (0 products) ===\nNo products available.\n"
		if got != want {
			t.Errorf("empty branch:\n got: %q\nwant: %q", got, want)
		}
	})
}

// ---------------------------------------------------------------------------
// T8.5 getContextForBusiness assembly ORDER snapshot: Path A catalog block,
// then POLICIES & FAQs, then (only when an image was supplied) IMAGE MATCH.
// ---------------------------------------------------------------------------

func TestCatContextAssemblySnapshot(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()

	t.Run("cat_context_path_a_catalog_then_faqs_byte_exact", func(t *testing.T) {
		biz := env.newBusiness(t, "GHS")
		alpha := env.seedProduct(t, biz, catProductSeed{
			name: "Alpha", price: "45000.00", stock: 12,
			description: strPtr("Comfortable running shoes"), withImage: true,
		})
		beta := env.seedProduct(t, biz, catProductSeed{name: "Beta", price: "25.50"})
		env.seedFAQ(t, biz, "What is your return policy?", "7 day returns.", nil)

		stub, client := catStartOpenAIStub(t)
		deps := orchestrator.RetrieverDeps{Pool: env.pool, OpenAI: client}
		got, err := orchestrator.ContextForBusiness(ctx, deps, orchestrator.ContextArgs{
			BusinessID: biz,
			UserText:   "hello",
			Currency:   "GHS",
		})
		if err != nil {
			t.Fatalf("ContextForBusiness: %v", err)
		}
		want := "=== CATALOG (2 products — showing all) ===\n" +
			"[ID: " + alpha[:8] + "] Alpha — GH₵45,000 (12 available) [HAS_IMAGE]: Comfortable running shoes\n" +
			"[ID: " + beta[:8] + "] Beta — GH₵25.5\n" +
			"\n=== POLICIES & FAQs ===\n" +
			"Q: What is your return policy?\nA: 7 day returns.\n\n"
		if got != want {
			t.Errorf("context snapshot mismatch:\n got: %q\nwant: %q", got, want)
		}
		probes := stub.recorded()
		if len(probes) != 1 { // Path A needs NO product embeddings; only the FAQ embed fires
			t.Fatalf("embed calls = %d, want 1 (faqs only)", len(probes))
		}
		if probes[0].Input[0] != "hello" || probes[0].Dims != 768 ||
			probes[0].Model != "text-embedding-3-small" {
			t.Errorf("faq embed probe = %+v, want hello/768/text-embedding-3-small", probes[0])
		}
	})

	t.Run("cat_context_image_section_comes_last", func(t *testing.T) {
		biz := env.newBusiness(t, "GHS")
		env.seedProduct(t, biz, catProductSeed{name: "Solo", price: "90.00"})
		env.seedFAQ(t, biz, "Do you deliver?", "Yes, nationwide.", nil)

		q := harness.Embedding("cat-image-query", 768)
		perp := harness.Embedding("cat-perp", 768)
		pid := env.seedProduct(t, biz, catProductSeed{name: "Matched", price: "90.00"})
		env.seedImage(t, pid, "https://res.example.com/matched.jpg", 0, catCraftToward(q, perp, 0.90))

		_, client := catStartOpenAIStub(t)
		deps := orchestrator.RetrieverDeps{Pool: env.pool, OpenAI: client}
		got, err := orchestrator.ContextForBusiness(ctx, deps, orchestrator.ContextArgs{
			BusinessID:             biz,
			UserText:               "",
			Currency:               "GHS",
			CustomerImageEmbedding: q,
		})
		if err != nil {
			t.Fatalf("ContextForBusiness: %v", err)
		}
		iCatalog := strings.Index(got, "=== CATALOG (")
		iFaq := strings.Index(got, "=== POLICIES & FAQs ===")
		iImage := strings.Index(got, "=== IMAGE MATCH ===")
		if iCatalog < 0 || iFaq <= iCatalog || iImage <= iFaq {
			t.Fatalf("section order wrong: catalog@%d faq@%d image@%d\n%s", iCatalog, iFaq, iImage, got)
		}
		if !strings.Contains(got, "[HAS_IMAGE] (90% visual match)") {
			t.Errorf("image match line missing/percentage wrong:\n%s", got)
		}
	})
}

// ---------------------------------------------------------------------------
// catch-branch parity (:66-69): retrieval failures NEVER propagate; they
// append "Catalog retrieval failed.\n" to whatever accumulated first.
// ---------------------------------------------------------------------------

func TestCatContextFailureMarker(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()
	biz := env.newBusiness(t, "GHS")

	deadPool, err := pgxpool.New(ctx, "postgresql://postgres:postgres@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatalf("dead pool: %v", err)
	}
	defer deadPool.Close()

	_, client := catStartOpenAIStub(t)
	deps := orchestrator.RetrieverDeps{Pool: deadPool, OpenAI: client}
	got, err := orchestrator.ContextForBusiness(ctx, deps, orchestrator.ContextArgs{
		BusinessID: biz, UserText: "hi", Currency: "GHS",
	})
	if err != nil {
		t.Fatalf("ContextForBusiness must swallow errors like the TS catch, got: %v", err)
	}
	if want := "Catalog retrieval failed.\n"; got != want {
		t.Errorf("failure marker mismatch:\n got: %q\nwant: %q", got, want)
	}
}
