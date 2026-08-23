package orchestrator_test

import (
	"context"
	"encoding/base64"
	"math"
	"strconv"
	"strings"
	"testing"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

// ---------------------------------------------------------------------------
// deterministic vector algebra: the stub returns harness.Embedding(text, 768)
// so query vectors are known exactly; target embeddings are crafted with a
// Gram-Schmidt orthogonal component making cos(q, e) = cosTarget to float32
// precision. Expected cosine distances are computed BY HAND here (float64
// over the same float32 inputs) and drive every hit/no-hit expectation.
// ---------------------------------------------------------------------------

func catDot(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

func catCos(a, b []float32) float64 {
	return catDot(a, b) / math.Sqrt(catDot(a, a)*catDot(b, b))
}

// catCraftToward returns e = c·q̂ + s·r⊥ (unit), where r⊥ is r̂ with its q̂
// component removed — so q̂·e = c exactly, regardless of how close r is to q.
func catCraftToward(q, r []float32, cosTarget float64) []float32 {
	qNorm := math.Sqrt(catDot(q, q))
	rq := catDot(r, q)
	rNorm := math.Sqrt(catDot(r, r))

	e := make([]float32, len(q))
	for i := range q {
		qh := float64(q[i]) / qNorm
		rp := float64(r[i])/rNorm - rq/(rNorm*qNorm)*qh // r̂ − (r̂·q̂)q̂ component-wise
		c := cosTarget
		s := math.Sqrt(1 - cosTarget*cosTarget)
		e[i] = float32(c*qh + s*rp)
	}
	return e
}

const (
	catQueryText = "cat-query" // stub ⇒ harness.Embedding("cat-query", 768)
	catImageText = "cat-image-query"
	catGibberish = "cat-gibberish"
)

// ---------------------------------------------------------------------------
// T8.6 semanticProductSearch: threshold boundary (< 0.8), LIMIT 5,
// ordering by distance, embedding of userMessage || 'hello'.
// ---------------------------------------------------------------------------

func TestCatSemanticProductSearchBoundary(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()
	stub, client := catStartOpenAIStub(t)

	biz := env.newBusiness(t, "GHS")
	q := harness.Embedding(catQueryText, 768)
	perp := harness.Embedding("cat-perp", 768)

	type crafted struct {
		id  string
		cos float64
	}
	add := func(name string, cos float64) crafted {
		id := env.seedProduct(t, biz, catProductSeed{
			name: name, price: "10.00", stock: 1, embedding: catCraftToward(q, perp, cos),
		})
		return crafted{id: id, cos: catCos(q, catCraftToward(q, perp, cos))}
	}
	near := add("Near", 0.90) // distance ≈ 0.10 → hit
	mid := add("Mid", 0.25)   // distance ≈ 0.75 → hit (< 0.8 boundary, inside)
	far := add("Far", 0.15)   // distance ≈ 0.85 → MISS (< 0.8 fails, just outside)
	edge := add("Edge", 0.21) // distance ≈ 0.79 → hit, closest to the line from inside
	out2 := add("Out", 0.19)  // distance ≈ 0.81 → miss, closest to the line from outside

	// hand-computed sanity: the straddle actually straddles.
	for _, c := range []crafted{near, mid, edge} {
		if d := 1 - c.cos; d >= 0.8 {
			t.Fatalf("setup: %s expected inside threshold, hand distance %.6f", c.id[:8], d)
		}
	}
	for _, c := range []crafted{far, out2} {
		if d := 1 - c.cos; d < 0.8 {
			t.Fatalf("setup: %s expected outside threshold, hand distance %.6f", c.id[:8], d)
		}
	}
	// random-seed fillers sit near orthogonality (distance ≈ 1) — never hits.
	for i := 0; i < 6; i++ {
		env.seedProduct(t, biz, catProductSeed{
			name:      strings.Repeat("F", i+1) + " Filler",
			price:     "5.00",
			embedding: harness.Embedding("cat-filler-"+string(rune('a'+i)), 768),
		})
	}

	hits, err := orchestrator.SemanticProductSearch(ctx, env.pool, client, biz, catQueryText)
	if err != nil {
		t.Fatalf("SemanticProductSearch: %v", err)
	}

	gotIDs := make([]string, 0, len(hits))
	for _, h := range hits {
		gotIDs = append(gotIDs, h.ID)
	}
	wantIDs := []string{near.id, mid.id, edge.id} // ORDER BY distance ASC ⇔ cosine DESC
	if strings.Join(gotIDs, ",") != strings.Join(wantIDs, ",") {
		t.Errorf("hits = %v\nwant %v (cosines: near %.4f > mid %.4f > edge %.4f; far %.4f & out %.4f excluded)",
			gotIDs, wantIDs, near.cos, mid.cos, edge.cos, far.cos, out2.cos)
	}

	probes := stub.recorded()
	if len(probes) == 0 || probes[len(probes)-1].Input[0] != catQueryText ||
		probes[len(probes)-1].Dims != 768 || probes[len(probes)-1].Model != "text-embedding-3-small" {
		t.Errorf("embed probe = %+v, want %q @768 text-embedding-3-small", probes[len(probes)-1], catQueryText)
	}
}

func TestCatSemanticProductSearchLimitFive(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()
	_, client := catStartOpenAIStub(t)

	biz := env.newBusiness(t, "GHS")
	q := harness.Embedding(catQueryText, 768)
	perp := harness.Embedding("cat-perp", 768)

	var wantOrder []string
	cosines := []float64{0.95, 0.93, 0.91, 0.89, 0.87, 0.85, 0.83} // 7 eligible, only 5 survive
	for i, c := range cosines {
		id := env.seedProduct(t, biz, catProductSeed{
			name:  string(rune('A'+i)) + " Candidate",
			price: "12.00", stock: 1, embedding: catCraftToward(q, perp, c),
		})
		if len(wantOrder) < 5 { // nearest five, in order
			wantOrder = append(wantOrder, id)
		}
	}

	hits, err := orchestrator.SemanticProductSearch(ctx, env.pool, client, biz, catQueryText)
	if err != nil {
		t.Fatalf("SemanticProductSearch: %v", err)
	}
	gotIDs := make([]string, 0, len(hits))
	for _, h := range hits {
		gotIDs = append(gotIDs, h.ID)
	}
	if strings.Join(gotIDs, ",") != strings.Join(wantOrder, ",") {
		t.Errorf("LIMIT/order mismatch:\n got %v\nwant %v", gotIDs, wantOrder)
	}
}

func TestCatSemanticEmptyUserTextEmbedsHello(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()
	stub, client := catStartOpenAIStub(t)

	biz := env.newBusiness(t, "GHS")
	env.seedFAQ(t, biz, "Q?", "A.", nil)

	if _, err := orchestrator.SearchFaqs(ctx, env.pool, client, biz, ""); err != nil {
		t.Fatalf("SearchFaqs: %v", err)
	}
	probes := stub.recorded()
	if len(probes) == 0 || probes[0].Input[0] != "hello" {
		t.Errorf("empty userText must embed 'hello' (:113/:181), got %+v", probes)
	}
}

// ---------------------------------------------------------------------------
// T8.9 searchFaqs: top-3 by similarity, NO threshold, POLICIES & FAQs block.
// ---------------------------------------------------------------------------

func TestCatSearchFaqs(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()
	_, client := catStartOpenAIStub(t)

	biz := env.newBusiness(t, "GHS")
	q := harness.Embedding(catQueryText, 768)
	perp := harness.Embedding("cat-perp", 768)

	env.seedFAQ(t, biz, "Delivery takes how long?", "Same day in Accra.", catCraftToward(q, perp, 0.97))
	env.seedFAQ(t, biz, "Do you offer refunds?", "7-day refund window.", catCraftToward(q, perp, 0.60))
	env.seedFAQ(t, biz, "Where are you located?", "Osu, Accra.", catCraftToward(q, perp, 0.30))
	env.seedFAQ(t, biz, "Unrelated four?", "Would be fourth anyway.", catCraftToward(q, perp, 0.05))

	hits, err := orchestrator.SearchFaqs(ctx, env.pool, client, biz, catQueryText)
	if err != nil {
		t.Fatalf("SearchFaqs: %v", err)
	}
	if len(hits) != 3 {
		t.Fatalf("faq hits = %d, want exactly 3 (no threshold, LIMIT 3)", len(hits))
	}
	for i := range hits {
		if hits[i].Question == "" || hits[i].Answer == "" {
			t.Errorf("faq hit %d has empty fields: %+v", i, hits[i])
		}
	}
	if hits[0].Question != "Delivery takes how long?" ||
		hits[1].Question != "Do you offer refunds?" ||
		hits[2].Question != "Where are you located?" {
		t.Errorf("faq order wrong: %+v", hits)
	}

	block := orchestrator.FormatFaqs(hits)
	wantBlock := "\n=== POLICIES & FAQs ===\n" +
		"Q: Delivery takes how long?\nA: Same day in Accra.\n\n" +
		"Q: Do you offer refunds?\nA: 7-day refund window.\n\n" +
		"Q: Where are you located?\nA: Osu, Accra.\n\n"
	if block != wantBlock {
		t.Errorf("faq block:\n got %q\nwant %q", block, wantBlock)
	}
	if empty := orchestrator.FormatFaqs(nil); empty != "\n=== POLICIES & FAQs ===\nNo policies found.\n" {
		t.Errorf("empty faq block = %q", empty)
	}
}

// ---------------------------------------------------------------------------
// T8.10 image match: DISTINCT-ON dedup to best angle, strict < 0.25 distance
// gate (similarity > 0.75), LIMIT 3 distinct products, percentage =
// Math.round((1-distance) * 100) verified against hand-computed cosine.
// ---------------------------------------------------------------------------

func TestCatImageMatches(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()

	q := harness.Embedding(catImageText, 768)
	perp := harness.Embedding("cat-perp", 768)
	biz := env.newBusiness(t, "GHS")

	seedImg := func(productID, url string, cos float64) string {
		return env.seedImage(t, productID, url, int(cos*100), catCraftToward(q, perp, cos))
	}

	p1 := env.seedProduct(t, biz, catProductSeed{name: "Two Angles", price: "40.00"})
	seedImg(p1, "https://res.example.com/p1-a.jpg", 0.80) // eligible, worse angle
	seedImg(p1, "https://res.example.com/p1-b.jpg", 0.96) // best angle wins dedup

	justOutside := env.seedProduct(t, biz, catProductSeed{name: "Just Outside", price: "41.00"})
	seedImg(justOutside, "https://res.example.com/out.jpg", 0.74) // dist .26 ≥ .25 → excluded (strict <)

	p3 := env.seedProduct(t, biz, catProductSeed{name: "Third", price: "43.00"})
	seedImg(p3, "https://res.example.com/p3.jpg", 0.90)
	p4 := env.seedProduct(t, biz, catProductSeed{name: "Fourth", price: "44.00"})
	seedImg(p4, "https://res.example.com/p4.jpg", 0.88)
	p5 := env.seedProduct(t, biz, catProductSeed{name: "FifthCut", price: "45.00"})
	seedImg(p5, "https://res.example.com/p5.jpg", 0.86)
	p6 := env.seedProduct(t, biz, catProductSeed{name: "SixthCut", price: "46.00"})
	seedImg(p6, "https://res.example.com/p6.jpg", 0.84)

	unavailable := env.seedProduct(t, biz, catProductSeed{name: "Hidden Unavailable", price: "47.00", hidden: true})
	seedImg(unavailable, "https://res.example.com/hidden.jpg", 0.99) // is_available filter

	nullEmb := env.seedProduct(t, biz, catProductSeed{name: "Null Embedding", price: "48.00"})
	env.seedImage(t, nullEmb, "https://res.example.com/null.jpg", 0, nil) // IS NOT NULL filter

	matches, err := orchestrator.ImageMatches(ctx, env.pool, biz, q)
	if err != nil {
		t.Fatalf("ImageMatches: %v", err)
	}
	if len(matches) != 3 {
		t.Fatalf("matches = %d (%+v), want 3 (dedup + strict threshold + LIMIT)", len(matches), matches)
	}
	wantOrder := []string{p1, p3, p4} // distances ≈ .04, .10, .12; p5 (.14) cut by LIMIT
	for i, w := range wantOrder {
		if matches[i].ID != w {
			t.Errorf("match[%d].ID = %s…, want %s… (order by distance asc)", i, matches[i].ID[:8], w[:8])
		}
	}
	if matches[0].ImageURL == nil || *matches[0].ImageURL != "https://res.example.com/p1-b.jpg" {
		t.Errorf("DISTINCT ON must keep each product's BEST angle, got %v (want p1-b)", matches[0].ImageURL)
	}

	// hand-computed percentage math exactness: pct = Math.round(similarity*100),
	// similarity = 1 - distance computed over the SAME float32 inputs.
	expectedPct := int64(math.Round(catCos(q, catCraftToward(q, perp, 0.96)) * 100))
	gotPct := int64(math.Round(matches[0].Similarity * 100))
	if gotPct != expectedPct {
		t.Errorf("percentage = %d, want hand-computed %d", gotPct, expectedPct)
	}
	if simErr := math.Abs(catCos(q, catCraftToward(q, perp, 0.96)) - matches[0].Similarity); simErr > 1e-3 {
		t.Errorf("similarity drift too large: %.6f", simErr)
	}
	section := orchestrator.FormatImageMatches(matches, "GHS")
	wantLine := "[ID: " + p1[:8] + "] Two Angles — GH₵40 [HAS_IMAGE] (" +
		strconv.FormatInt(expectedPct, 10) + "% visual match)\n"
	if !strings.Contains(section, wantLine) {
		t.Errorf("image section missing golden line %q in:\n%s", wantLine, section)
	}
	if header := "\n=== IMAGE MATCH ===\nThe customer sent an image. The following products visually match:\n"; !strings.HasPrefix(section, header) {
		t.Errorf("image section header wrong:\n%q", section[:min(len(section), 120)])
	}

	if noMatch := orchestrator.FormatImageMatches(nil, "GHS"); noMatch !=
		"\n=== IMAGE MATCH ===\nNo products visually matched the customer's image.\n" {
		t.Errorf("no-match wording = %q", noMatch)
	}
	if unavailableMsg := orchestrator.GetImageContext(ctx, env.pool, biz, nil, "GHS"); unavailableMsg !=
		"\n=== IMAGE MATCH ===\nImage matching is unavailable. Rely on the LLM's vision capability to analyze the image.\n" {
		t.Errorf("nil-embedding degradation = %q", unavailableMsg)
	}
}

// ---------------------------------------------------------------------------
// T8.6 zero-hit branch + T8.8 'showing X most relevant of Y+' header through
// ContextForBusiness Path B (> FULL_CATALOG_THRESHOLD available products).
// ---------------------------------------------------------------------------

func TestCatPathBSectionAndHeader(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()
	_, client := catStartOpenAIStub(t)

	biz := env.newBusiness(t, "GHS")
	q := harness.Embedding(catQueryText, 768)
	perp := harness.Embedding("cat-perp", 768)

	// 29 fillers + 2 crafted = 31 available (> 30 ⇒ Path B).
	for i := 0; i < 29; i++ {
		env.seedProduct(t, biz, catProductSeed{
			name:      strings.Repeat("z", i+1) + " Filler",
			price:     "5.00",
			embedding: harness.Embedding("cat-bfiller-"+string(rune('a'+i)), 768),
		})
	}
	first := env.seedProduct(t, biz, catProductSeed{
		name: "First Match", price: "30.00", stock: 5, embedding: catCraftToward(q, perp, 0.95),
	})
	second := env.seedProduct(t, biz, catProductSeed{
		name: "Second Match", price: "45.00", stock: 2, embedding: catCraftToward(q, perp, 0.93),
	})

	deps := orchestrator.RetrieverDeps{Pool: env.pool, OpenAI: client}
	got, err := orchestrator.ContextForBusiness(ctx, deps, orchestrator.ContextArgs{
		BusinessID: biz, UserText: catQueryText, Currency: "NGN",
	})
	if err != nil {
		t.Fatalf("ContextForBusiness: %v", err)
	}
	wantHeader := "=== CATALOG (showing 2 most relevant of 31+ products) ===\n"
	if !strings.Contains(got, wantHeader) {
		t.Errorf("path B header missing:\n%s", got)
	}
	if !strings.Contains(got, "[ID: "+first[:8]+"] First Match — ₦30 (5 available)") ||
		!strings.Contains(got, "[ID: "+second[:8]+"] Second Match — ₦45 (2 available)") {
		t.Errorf("crafted hits missing/wrong currency:\n%s", got)
	}
	iHeader := strings.Index(got, wantHeader)
	iFaq := strings.Index(got, "=== POLICIES & FAQs ===")
	if iHeader >= iFaq {
		t.Errorf("catalog must precede FAQs: header@%d faq@%d", iHeader, iFaq)
	}
	if strings.Contains(got, "showing all") {
		t.Error("Path B must not contain the Path A header")
	}
}

func TestCatPathBZeroMatchWording(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()
	_, client := catStartOpenAIStub(t)

	biz := env.newBusiness(t, "GHS")
	gibberishVec := harness.Embedding(catGibberish, 768)
	perp := harness.Embedding("cat-perp", 768)

	// 31 products whose embeddings all sit FAR from the gibberish vector:
	// crafted at cosine 0.05 (distance 0.95 ≥ 0.8) — deterministically below
	// the relevance bar rather than trusting random luck.
	for i := 0; i < 31; i++ {
		env.seedProduct(t, biz, catProductSeed{
			name:      strings.Repeat("w", i+1) + " Stock Item",
			price:     "5.00",
			embedding: catCraftToward(gibberishVec, perp, 0.05),
		})
	}

	deps := orchestrator.RetrieverDeps{Pool: env.pool, OpenAI: client}
	got, err := orchestrator.ContextForBusiness(ctx, deps, orchestrator.ContextArgs{
		BusinessID: biz, UserText: catGibberish, Currency: "GHS",
	})
	if err != nil {
		t.Fatalf("ContextForBusiness: %v", err)
	}
	wantSection := "=== CATALOG ===\nNo products matched your query. Try describing what you're looking for.\n"
	if !strings.Contains(got, wantSection) {
		t.Errorf("zero-match branch wording missing:\n%s", got)
	}
}

// ---------------------------------------------------------------------------
// pipeline weld: CatalogAdapter's returned closure must be assignable to
// workers.CatalogFunc EXACTLY (data URL in, context out).
// ---------------------------------------------------------------------------

func TestCatCatalogAdapterWeldShape(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()

	var weld workers.CatalogFunc = orchestrator.CatalogAdapter(
		orchestrator.RetrieverDeps{}, nil,
	)
	_ = weld // compile-time shape proof; runtime behaviour covered below.

	stub, client := catStartOpenAIStub(t)
	var embedCalls [][]byte
	embedder := func(_ context.Context, image []byte, _ string) ([]float32, error) {
		embedCalls = append(embedCalls, image)
		return harness.Embedding(catImageText, 768), nil
	}

	biz := env.newBusiness(t, "GHS")
	env.seedProduct(t, biz, catProductSeed{name: "Plain", price: "10.00"})
	pid := env.seedProduct(t, biz, catProductSeed{name: "Photo Product", price: "60.00"})
	q := harness.Embedding(catImageText, 768)
	perp := harness.Embedding("cat-perp", 768)
	env.seedImage(t, pid, "https://res.example.com/photo.jpg", 0, catCraftToward(q, perp, 0.92))

	deps := orchestrator.RetrieverDeps{Pool: env.pool, OpenAI: client}
	adapter := orchestrator.CatalogAdapter(deps, embedder)

	got, err := adapter(ctx, biz, "", "GHS",
		"data:image/jpeg;base64,"+base64.StdEncoding.EncodeToString([]byte("fake-jpeg-bytes")))
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	if len(embedCalls) != 1 || string(embedCalls[0]) != "fake-jpeg-bytes" {
		t.Fatalf("embedder calls = %d, want 1 with decoded bytes", len(embedCalls))
	}
	iCatalog := strings.Index(got, "=== CATALOG (")
	iFaq := strings.Index(got, "=== POLICIES & FAQs ===")
	iImage := strings.Index(got, "=== IMAGE MATCH ===")
	if iCatalog < 0 || iFaq <= iCatalog || iImage <= iFaq {
		t.Fatalf("assembly order broken: catalog@%d faq@%d image@%d\n%s", iCatalog, iFaq, iImage, got)
	}
	if !strings.Contains(got, "[HAS_IMAGE] (92% visual match)") {
		t.Errorf("image match line wrong:\n%s", got)
	}

	probes := stub.recorded()
	for _, p := range probes {
		if p.Input[0] != "hello" {
			t.Errorf("unexpected embed input %q, want hello (empty user text)", p.Input[0])
		}
	}
}

func TestCatCatalogAdapterDegradesGracefully(t *testing.T) {
	env := catStart(t)
	ctx := context.Background()
	_, client := catStartOpenAIStub(t)

	biz := env.newBusiness(t, "GHS")
	env.seedProduct(t, biz, catProductSeed{name: "Only", price: "10.00"})

	deps := orchestrator.RetrieverDeps{Pool: env.pool, OpenAI: client}
	unavailable := "\n=== IMAGE MATCH ===\nImage matching is unavailable. Rely on the LLM's vision capability to analyze the image.\n"

	for name, tc := range map[string]struct {
		embedder orchestrator.ImageEmbedder
		dataURL  string
	}{
		"nil_embedder":       {nil, "data:image/jpeg;base64,aGVsbG8="},
		"failing_embedder":   {func(context.Context, []byte, string) ([]float32, error) { return nil, context.DeadlineExceeded }, "data:image/jpeg;base64,aGVsbG8="},
		"malformed_data_url": {nil, "not-a-data-url"},
		"bad_base64":         {nil, "data:image/jpeg;base64,%%%not-base64%%%"},
	} {
		t.Run("cat_adapter_"+name, func(t *testing.T) {
			got, err := orchestrator.CatalogAdapter(deps, tc.embedder)(ctx, biz, "hi", "GHS", tc.dataURL)
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			if !strings.Contains(got, "=== CATALOG (") || !strings.Contains(got, "=== POLICIES & FAQs ===") {
				t.Fatalf("base sections lost:\n%s", got)
			}
			if !strings.HasSuffix(got, unavailable) {
				t.Errorf("expected unavailable degradation suffix:\n%s", got)
			}
		})
	}
}
