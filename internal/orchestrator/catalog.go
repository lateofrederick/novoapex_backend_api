// Stage 8b catalog-retrieval context assembly (T8.5, T8.7, T8.8): the port of
// libs/orchestrator/src/retrieval/catalog-retriever.service.ts's formatting
// half — CURRENCY_CONFIG (:9-13), formatProduct (:234-255), getFullCatalog
// (:149-166), semanticProductSearch's header/formatting (:192-219),
// searchFaqs' POLICIES & FAQs block (:131-140), getImageMatchContext
// (:74-103) and the getContextForBusiness assembly order (:33-72).
//
// Error policy is byte-exact with the source: getContextForBusiness never
// rejects — its single try/catch appends "Catalog retrieval failed.\n" to
// whatever accumulated before the throwing step and SKIPS the remaining
// steps; getImageMatchContext has its own inner catch that degrades to the
// "Image matching is unavailable." notice instead of tripping the outer one.
package orchestrator

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
)

// ProductRow is the products select-list shape every catalog section renders
// from. Price keeps Postgres's NUMERIC text ("45000.00") so formatting can
// reproduce Number(price).toLocaleString(locale) exactly.
type ProductRow struct {
	ID          string
	Name        string
	Price       *string // nil ⇒ SQL NULL ⇒ symbol+"0" (unreachable: column is NOT NULL)
	Stock       int     // suffix only when > 0
	Description *string // trimmed once; omitted entirely when it trims to empty
	HasImage    bool    // EXISTS(product_images) ⇒ [HAS_IMAGE] marker
}

// currencyFormat mirrors CURRENCY_CONFIG (:9-13). All three locales group
// thousands with "," and use "." decimals under Intl defaults, so the symbol
// swap is the only visible difference.
type currencyFormat struct {
	Symbol string
	Locale string
}

var currencyFormats = map[string]currencyFormat{
	"GHS": {Symbol: "GH₵", Locale: "en-GH"},
	"NGN": {Symbol: "₦", Locale: "en-NG"},
	"USD": {Symbol: "$", Locale: "en-US"},
}

func currencyFor(code string) currencyFormat {
	if c, ok := currencyFormats[code]; ok {
		return c
	}
	return currencyFormats["GHS"] // CURRENCY_CONFIG[currency] || CURRENCY_CONFIG['GHS'] (:241, :88)
}

// catalogJsTrim reproduces String.prototype.trim's rune set for description
// trimming (differs from unicode.IsSpace on U+FEFF and U+0085).
func catalogJsTrim(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		if r == 0x85 {
			return false
		}
		return r == 0xFEFF || r == ' ' || r == '\t' || r == '\n' || r == '\v' ||
			r == '\f' || r == '\r' || r == 0x00A0 || r == 0x1680 ||
			(r >= 0x2000 && r <= 0x200A) || r == 0x2028 || r == 0x2029 || r == 0x202F || r == 0x3000
	})
}

// formatJSAmount renders a NUMERIC text value like JS
// `Number(price).toLocaleString(config.locale)` (:92, :246): trailing
// fraction zeros vanish ("45000.00" → "45,000", "25.50" → "25.5"), integer
// digits group by three with commas, negatives keep their sign outside the
// grouping. Prices are Decimal(14,2), so Intl's maximumFractionDigits=3 cap
// can never engage.
func formatJSAmount(decimalText string) string {
	text := strings.TrimSpace(decimalText)
	if text == "" {
		return "0"
	}
	neg := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(text, "+")
	text = strings.TrimPrefix(text, "-")

	intPart, frac, _ := strings.Cut(text, ".")
	frac = strings.TrimRight(frac, "0")
	intPart = strings.TrimSpace(intPart)
	if intPart == "" {
		intPart = "0"
	}

	var grouped strings.Builder
	head := len(intPart) % 3
	if head > 0 {
		grouped.WriteString(intPart[:head])
	}
	for i := head; i < len(intPart); i += 3 {
		if grouped.Len() > 0 {
			grouped.WriteByte(',')
		}
		grouped.WriteString(intPart[i : i+3])
	}

	out := grouped.String()
	if frac != "" {
		out += "." + frac
	}
	if neg && out != "0" {
		out = "-" + out
	}
	return out
}

// formatPrice renders `${config.symbol}${price.toLocaleString(locale)}` /
// `${config.symbol}0` for a null price (:244-247).
func formatPrice(price *string, cfg currencyFormat) string {
	if price == nil {
		return cfg.Symbol + "0"
	}
	return cfg.Symbol + formatJSAmount(*price)
}

// shortProductID mirrors product.id.substring(0, 8) (:242, :91): ids shorter
// than 8 characters survive whole.
func shortProductID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// FormatProduct ports formatProduct (:234-255) byte-exactly:
//
//	[ID: abc12345] Nike Air Max — GH₵45,000 (12 available) [HAS_IMAGE]: Comfortable shoes
//
// stock suffix ONLY when stock > 0, [HAS_IMAGE] between price/stock and the
// description, description as ": <trim>" only when it trims non-empty, no
// truncation ever applied to descriptions.
func FormatProduct(p ProductRow, currency string) string {
	cfg := currencyFor(currency)
	price := formatPrice(p.Price, cfg)
	stock := ""
	if p.Stock > 0 { // :248
		stock = fmt.Sprintf(" (%d available)", p.Stock)
	}
	desc := ""
	if p.Description != nil && catalogJsTrim(*p.Description) != "" { // :249-251
		desc = ": " + catalogJsTrim(*p.Description)
	}
	hasImage := ""
	if p.HasImage { // :252
		hasImage = " [HAS_IMAGE]"
	}
	return "[ID: " + shortProductID(p.ID) + "] " + p.Name + " — " + price + stock + hasImage + desc // :254
}

const productSelectColumns = `p.id, p.name, p.price, p.stock, p.description,
       EXISTS(SELECT 1 FROM product_images pi WHERE pi.product_id = p.id) AS has_image`

// countAvailableProducts is the shared COUNT(*) probe (:43-46, :192-197).
func countAvailableProducts(ctx context.Context, pool *pgxpool.Pool, businessID string) (int64, error) {
	var count int64
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM products WHERE business_id = $1 AND is_available = true`,
		businessID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("orchestrator: count products for business %s: %w", businessID, err)
	}
	return count, nil
}

// GetFullCatalog ports getFullCatalog / Path A (:149-166): every available
// product alphabetically, no embeddings involved.
func GetFullCatalog(ctx context.Context, pool *pgxpool.Pool, businessID, currency string) (string, error) {
	rows, err := pool.Query(ctx, `
		SELECT `+productSelectColumns+`
		FROM products p
		WHERE p.business_id = $1 AND p.is_available = true
		ORDER BY p.name ASC;`, businessID)
	if err != nil {
		return "", fmt.Errorf("orchestrator: full catalog for business %s: %w", businessID, err)
	}
	defer rows.Close()
	products, err := scanProductHits(rows)
	if err != nil {
		return "", err
	}

	if len(products) == 0 { // :158-160
		return "=== CATALOG (0 products) ===\nNo products available.\n", nil
	}
	header := fmt.Sprintf("=== CATALOG (%d products — showing all) ===\n", len(products)) // :162
	body := make([]string, len(products))
	for i, p := range products {
		body[i] = FormatProduct(p, currency)
	}
	return header + strings.Join(body, "\n") + "\n", nil // :165
}

// GetSemanticCatalog ports semanticProductSearch / Path B's string assembly
// (:177-219): total-count header, relevance-gated top-5 body, and the two
// zero-result escape hatches.
func GetSemanticCatalog(ctx context.Context, pool *pgxpool.Pool, oa *openai.Client, businessID, userText, currency string) (string, error) {
	totalCount, err := countAvailableProducts(ctx, pool, businessID) // :192-197
	if err != nil {
		return "", err
	}

	products, err := SemanticProductSearch(ctx, pool, oa, businessID, userText) // embed + query (:179-209)
	if err != nil {
		return "", err
	}

	if len(products) == 0 { // :212-214
		return "=== CATALOG ===\nNo products matched your query. Try describing what you're looking for.\n", nil
	}

	header := fmt.Sprintf("=== CATALOG (showing %d most relevant of %d+ products) ===\n",
		len(products), totalCount) // :216
	body := make([]string, len(products))
	for i, p := range products {
		body[i] = FormatProduct(p, currency)
	}
	return header + strings.Join(body, "\n") + "\n", nil // :219
}

// FormatFaqs ports searchFaqs' result formatting (:131-140).
func FormatFaqs(faqs []FAQHit) string {
	result := "\n=== POLICIES & FAQs ===\n"
	if len(faqs) == 0 {
		return result + "No policies found.\n"
	}
	for _, f := range faqs {
		result += "Q: " + f.Question + "\nA: " + f.Answer + "\n\n"
	}
	return result
}

// imageMatchUnavailable is getImageMatchContext's inner catch branch
// (:99-102): image-search trouble must never trip the outer catch.
const imageMatchUnavailable = "\n=== IMAGE MATCH ===\nImage matching is unavailable. Rely on the LLM's vision capability to analyze the image.\n"

// FormatImageMatches ports getImageMatchContext's success formatting
// (:84-98): percentage = Math.round(similarity * 100) where similarity =
// 1 - cosine_distance (:93, :168); every match necessarily carries an image.
func FormatImageMatches(matches []ImageHit, currency string) string {
	if len(matches) == 0 {
		return "\n=== IMAGE MATCH ===\nNo products visually matched the customer's image.\n"
	}
	cfg := currencyFor(currency)
	result := "\n=== IMAGE MATCH ===\nThe customer sent an image. The following products visually match:\n"
	for _, m := range matches {
		pct := math.Round(m.Similarity * 100) // :93 Math.round parity
		result += "[ID: " + shortProductID(m.ID) + "] " + m.Name +
			" — " + formatPrice(m.Price, cfg) +
			" [HAS_IMAGE] (" + strconv.FormatInt(int64(pct), 10) + "% visual match)\n" // :95
	}
	return result
}

// GetImageContext ports getImageMatchContext (:74-103) with the embedding
// step hoisted: callers pass the customer-photo vector directly. A nil or
// empty embedding means "could not be embedded" (adapter decode/embed
// failure) and degrades to the unavailable notice exactly like the source's
// inner catch; so does a failing similarity query.
func GetImageContext(ctx context.Context, pool *pgxpool.Pool, businessID string, imageEmbedding []float32, currency string) string {
	if len(imageEmbedding) == 0 {
		return imageMatchUnavailable
	}
	matches, err := ImageMatches(ctx, pool, businessID, imageEmbedding)
	if err != nil {
		return imageMatchUnavailable
	}
	return FormatImageMatches(matches, currency)
}

// RetrieverDeps carries what one retrieval pass needs. EmbedImage lives on
// CatalogAdapter below rather than here because the pipeline weld hands over
// an already-encoded data URL, not bytes.
type RetrieverDeps struct {
	Pool   *pgxpool.Pool
	OpenAI *openai.Client
}

// ContextArgs are the per-turn inputs of ContextForBusiness.
type ContextArgs struct {
	BusinessID string
	UserText   string // embedded for Path B + FAQs ('' ⇒ embed "hello")
	Currency   string // '' ⇒ GHS fallback
	// CustomerImageEmbedding models the source's optional inboundImageBuffer
	// (:37): nil = no customer image this turn (section omitted, :63);
	// non-nil but EMPTY = image supplied yet unembeddable (adapter decode/
	// embed failure) — GetImageContext then renders the unavailable notice,
	// exactly like findSimilarProductsByImage failing inside getImageMatch-
	// Context's inner catch (:99-102).
	CustomerImageEmbedding []float32
}

// ContextForBusiness ports getContextForBusiness (:33-72). Assembly ORDER:
//
//  1. COUNT available products → path selection at ≤ 30 (:43-56)
//  2. Path A GetFullCatalog | Path B GetSemanticCatalog            (:51-57)
//  3. POLICIES & FAQs (always semantic, appended after catalog)     (:59-60)
//  4. IMAGE MATCH only when a customer-image embedding was supplied (:62-65)
//
// Any failure in steps 1–3 appends "Catalog retrieval failed.\n" to the
// accumulated context, skips the remaining steps, and returns WITHOUT an
// error (:66-69) — the source never rejects, so neither does this. Step 4
// failures degrade internally to the unavailable notice instead.
func ContextForBusiness(ctx context.Context, deps RetrieverDeps, args ContextArgs) (string, error) {
	var contextString strings.Builder

	fail := func(err error) (string, error) { // catch-branch (:66-69)
		contextString.WriteString("Catalog retrieval failed.\n")
		return contextString.String(), nil
	}

	count, err := countAvailableProducts(ctx, deps.Pool, args.BusinessID) // :43-48
	if err != nil {
		return fail(err)
	}

	if count <= FullCatalogThreshold { // :51-57
		section, err := GetFullCatalog(ctx, deps.Pool, args.BusinessID, args.Currency)
		if err != nil {
			return fail(err)
		}
		contextString.WriteString(section)
	} else {
		section, err := GetSemanticCatalog(ctx, deps.Pool, deps.OpenAI, args.BusinessID, args.UserText, args.Currency)
		if err != nil {
			return fail(err)
		}
		contextString.WriteString(section)
	}

	faqs, err := SearchFaqs(ctx, deps.Pool, deps.OpenAI, args.BusinessID, args.UserText) // :59-60
	if err != nil {
		return fail(err)
	}
	contextString.WriteString(FormatFaqs(faqs))

	if args.CustomerImageEmbedding != nil { // :62-65 — a customer image was supplied
		contextString.WriteString(GetImageContext(ctx, deps.Pool, args.BusinessID, args.CustomerImageEmbedding, args.Currency))
	}

	return contextString.String(), nil
}

// ImageEmbedder computes a visual-search vector for raw image bytes. The
// production implementation is the Gemini gemini-embedding-001 @ 768 call
// (workers' generateImageEmbedding); tests supply deterministic fakes.
type ImageEmbedder func(ctx context.Context, image []byte, mimeType string) ([]float32, error)

// decodeDataURL splits the pipeline's inbound-image encoding
// (`data:<mime>;base64,<payload>` — workers/orchestrator.go:377) back into
// bytes and mime type.
func decodeDataURL(dataURL string) ([]byte, string, bool) {
	const prefix = "data:"
	if !strings.HasPrefix(dataURL, prefix) {
		return nil, "", false
	}
	rest := dataURL[len(prefix):]
	head, payload, ok := strings.Cut(rest, ";base64,")
	if !ok {
		return nil, "", false
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, "", false
	}
	return raw, head, true
}

// CatalogAdapter welds ContextForBusiness onto the pipeline's catalog hook —
// structurally identical to workers.CatalogFunc
// (internal/workers/orchestrator.go:78):
//
//	func(ctx context.Context, businessID, userMessage, currency, latestImageDataURL string) (string, error)
//
// ADAPTATION vs workers.CatalogFunc: the weld passes the customer photo as a
// base64 DATA URL while the retriever consumes a precomputed image EMBEDDING;
// this adapter closes the gap by decoding the URL and calling embedImage
// (production: Gemini image-embedding service). A malformed URL or a nil/
// failing embedImage yields an empty embedding, which GetImageContext renders
// as the source's "Image matching is unavailable." notice — matching the TS
// behaviour where findSimilarProductsByImage failures are swallowed inside
// getImageMatchContext and never reach the outer catch.
func CatalogAdapter(deps RetrieverDeps, embedImage ImageEmbedder) func(ctx context.Context, businessID, userMessage, currency, latestImageDataURL string) (string, error) {
	return func(ctx context.Context, businessID, userMessage, currency, latestImageDataURL string) (string, error) {
		args := ContextArgs{
			BusinessID: businessID,
			UserText:   userMessage,
			Currency:   currency,
		}
		if latestImageDataURL != "" { // an image was supplied this turn (:62-65)
			args.CustomerImageEmbedding = []float32{} // supplied-yet-unembeddable marker
			if image, mime, ok := decodeDataURL(latestImageDataURL); ok && embedImage != nil {
				if vector, err := embedImage(ctx, image, mime); err == nil {
					args.CustomerImageEmbedding = vector
				}
			}
		}
		return ContextForBusiness(ctx, deps, args)
	}
}
