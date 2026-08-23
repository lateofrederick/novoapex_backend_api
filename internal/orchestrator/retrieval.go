// Stage 8b catalog-retrieval query layer (T8.6, T8.9, T8.10): the port of the
// vector searches inside libs/orchestrator/src/retrieval/catalog-retriever.service.ts
// (semanticProductSearch :177-220, searchFaqs :110-141) and
// ImageEmbeddingService.findSimilarProductsByImage
// (libs/orchestrator/src/embedding/image-embedding.service.ts:126-170).
//
// Split vs source: the TS methods return fully formatted context STRINGS, but
// this port keeps retrieval pure (hits in) and puts every byte of section
// formatting in catalog.go — the same statements, just factored so the
// pipeline can inspect hits before they reach the prompt.
//
// Vectors travel as pgvector.NewVector bindings through raw pgx SQL (the
// columns are Prisma Unsupported("vector(768)") on the Node side); the
// ::vector casts mirror the TS `$n::vector` literals.
package orchestrator

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
)

const (
	// FullCatalogThreshold is FULL_CATALOG_THRESHOLD
	// (catalog-retriever.service.ts:7): at most this many available products
	// dumps the whole catalog without touching embeddings.
	FullCatalogThreshold = 30

	// semanticMaxDistance mirrors the hardcoded `< 0.8` cosine-distance gate
	// (:206); semanticLimit its `LIMIT 5` (:208).
	semanticMaxDistance = 0.8
	semanticLimit       = 5

	// faqLimit is searchFaqs' `LIMIT 3` (:128) — NO distance threshold: the
	// top-3 FAQs are always included regardless of similarity.
	faqLimit = 3

	// ImageMatchThreshold/ImageMatchLimit are findSimilarProductsByImage's
	// defaults (image-embedding.service.ts:130-131): 0.75 similarity ⇔ cosine
	// distance < 0.25, three distinct products.
	ImageMatchThreshold = 0.75
	ImageMatchLimit     = 3

	// textEmbeddingModel/textEmbeddingDimensions reproduce the embed() calls
	// (:111-119, :179-187): OpenAI text-embedding-3-small reduced to 768
	// dimensions — the dimension parameter must match the writer side.
	textEmbeddingModel     = "text-embedding-3-small"
	textEmbeddingDimension = 768
)

// ProductHit is a retrieval result carrying exactly the columns formatProduct
// needs (identical shape to catalog.ProductRow — an alias so both names read
// naturally at their call sites).
type ProductHit = ProductRow

// FAQHit is one question/answer pair from searchFaqs.
type FAQHit struct {
	Question string
	Answer   string
}

// ImageHit is one visually matched product from ImageMatches. Similarity is
// `1 - cosine_distance` in [threshold, 1]; Price is the NUMERIC(14,2) value
// as Postgres rendered it (nil only if the column were NULL).
type ImageHit struct {
	ID         string
	Name       string
	Price      *string
	ImageURL   *string
	Similarity float64
}

// embedUserMessage ports the `value: userMessage || 'hello'` embed calls
// (:113, :181): an empty turn still gets a vector so greeting turns can match
// FAQs instead of failing.
func embedUserMessage(ctx context.Context, oa *openai.Client, userText string) ([]float32, error) {
	value := userText
	if value == "" {
		value = "hello"
	}
	vector, err := oa.Embedding(ctx, textEmbeddingModel, textEmbeddingDimension, value)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: embed %q: %w", value, err)
	}
	return vector, nil
}

// SemanticProductSearch ports the query half of semanticProductSearch
// (:200-209): embed userText, then take the top-5 available products of the
// business whose embedding sits within cosine distance 0.8 of the query,
// nearest first.
func SemanticProductSearch(ctx context.Context, pool *pgxpool.Pool, oa *openai.Client, businessID, userText string) ([]ProductHit, error) {
	vector, err := embedUserMessage(ctx, oa, userText)
	if err != nil {
		return nil, err
	}

	rows, err := pool.Query(ctx, `
		SELECT p.id, p.name, p.price, p.stock, p.description,
		       EXISTS(SELECT 1 FROM product_images pi WHERE pi.product_id = p.id) AS has_image
		FROM products p
		WHERE p.business_id = $1
		  AND p.is_available = true
		  AND (p.embedding <=> $2::vector) < 0.8
		ORDER BY p.embedding <=> $2::vector
		LIMIT 5;`, businessID, pgvector.NewVector(vector))
	if err != nil {
		return nil, fmt.Errorf("orchestrator: semantic product search for business %s: %w", businessID, err)
	}
	defer rows.Close()
	return scanProductHits(rows)
}

// SearchFaqs ports searchFaqs' query (:123-129): the top-3 FAQs of the
// business nearest to the embedded user message, no similarity floor.
func SearchFaqs(ctx context.Context, pool *pgxpool.Pool, oa *openai.Client, businessID, userText string) ([]FAQHit, error) {
	vector, err := embedUserMessage(ctx, oa, userText)
	if err != nil {
		return nil, err
	}

	rows, err := pool.Query(ctx, `
		SELECT question, answer
		FROM faqs
		WHERE business_id = $1
		ORDER BY embedding <=> $2::vector
		LIMIT 3;`, businessID, pgvector.NewVector(vector))
	if err != nil {
		return nil, fmt.Errorf("orchestrator: faq search for business %s: %w", businessID, err)
	}
	defer rows.Close()

	out := make([]FAQHit, 0, faqLimit)
	for rows.Next() {
		var f FAQHit
		if err := rows.Scan(&f.Question, &f.Answer); err != nil {
			return nil, fmt.Errorf("orchestrator: scan faq row: %w", err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("orchestrator: iterate faq rows: %w", err)
	}
	return out, nil
}

// ImageMatches ports findSimilarProductsByImage's SQL
// (image-embedding.service.ts:146-161) minus its embedding step: the caller
// supplies the customer-photo vector (Gemini gemini-embedding-001 @ 768 on
// the write/query path). The two-level DISTINCT ON is load-bearing — Postgres
// requires the leading ORDER BY to match the DISTINCT ON expressions, so the
// per-product best-angle pick cannot share a query with the final ranking;
// `limit` therefore means "N distinct products".
func ImageMatches(ctx context.Context, pool *pgxpool.Pool, businessID string, imageEmbedding []float32) ([]ImageHit, error) {
	rows, err := pool.Query(ctx, `
		SELECT * FROM (
		  SELECT DISTINCT ON (p.id)
		         p.id, p.name, p.price, pi.url AS image_url,
		         pi.embedding <=> $2::vector AS distance
		  FROM product_images pi
		  JOIN products p ON p.id = pi.product_id
		  WHERE p.business_id = $1
		    AND p.is_available = true
		    AND pi.embedding IS NOT NULL
		    AND (pi.embedding <=> $2::vector) < $3
		  ORDER BY p.id, distance
		) best
		ORDER BY distance
		LIMIT $4;`,
		businessID, pgvector.NewVector(imageEmbedding), 1-ImageMatchThreshold, ImageMatchLimit)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: image similarity query for business %s: %w", businessID, err)
	}
	defer rows.Close()

	out := make([]ImageHit, 0, ImageMatchLimit)
	for rows.Next() {
		var (
			h        ImageHit
			price    pgtype.Numeric
			distance float64
		)
		if err := rows.Scan(&h.ID, &h.Name, &price, &h.ImageURL, &distance); err != nil {
			return nil, fmt.Errorf("orchestrator: scan image match row: %w", err)
		}
		h.Similarity = 1 - distance
		if price.Valid { // schema marks price NOT NULL; guard kept for parity
			s, err := price.Value()
			if err != nil {
				return nil, fmt.Errorf("orchestrator: render image match price: %w", err)
			}
			if str, ok := s.(string); ok {
				h.Price = &str
			}
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("orchestrator: iterate image match rows: %w", err)
	}
	return out, nil
}

// scanProductHits drains the shared products select-list (id, name, price,
// stock, description, has_image) into ProductHit values.
func scanProductHits(rows pgx.Rows) ([]ProductHit, error) {
	out := make([]ProductHit, 0, semanticLimit)
	for rows.Next() {
		var (
			p           ProductHit
			price       pgtype.Numeric
			description *string
		)
		if err := rows.Scan(&p.ID, &p.Name, &price, &p.Stock, &description, &p.HasImage); err != nil {
			return nil, fmt.Errorf("orchestrator: scan product row: %w", err)
		}
		p.Description = description
		if price.Valid {
			s, err := price.Value()
			if err != nil {
				return nil, fmt.Errorf("orchestrator: render product price: %w", err)
			}
			if str, ok := s.(string); ok {
				p.Price = &str
			}
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("orchestrator: iterate product rows: %w", err)
	}
	return out, nil
}
