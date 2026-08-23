package workers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgvector/pgvector-go"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/google"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// Aliases over pgx/v5 so EmbeddingDeps accepts *pgxpool.Pool without this
// file importing the pool package directly.
type (
	pgconnTag = pgconn.CommandTag
	Row       = pgx.Row
	Rows      = pgx.Rows
)

// ErrNoRows mirrors pgx.ErrNoRows for the not-found parity errors.
var ErrNoRows = pgx.ErrNoRows

// Byte-exact port of libs/orchestrator/src/embedding/* (T6.15–T6.20).
//
// PARITY CONTRACT (embedding.service.ts / image-embedding.service.ts):
//   - buildSearchDocument assembles `name` + optionally trimmed `description`
//     joined with ". " — any change silently alters every vector;
//   - text embeddings: OpenAI text-embedding-3-small at 768 dimensions
//     (dimension-reduction parameter passed identically);
//   - image embeddings: Gemini gemini-embedding-001, outputDimensionality 768,
//     inline base64 data part, empty text value omitted like the AI SDK does;
//   - vector writes are raw SQL casts of the "[v1,v2,…]" literal because the
//     columns are Prisma Unsupported("vector(768)").

const (
	TextEmbeddingModel       = "text-embedding-3-small"
	TextEmbeddingDimensions  = 768
	ImageEmbeddingModel      = "gemini-embedding-001"
	ImageEmbeddingDimensions = 768

	defaultImageMIME = "image/jpeg"
	maxImageBytes    = 20 << 20 // generous cap before base64-ing into the request
)

// EmbeddingDeps carries everything the embedding handlers need.
type EmbeddingDeps struct {
	Pool   PoolExec
	OpenAI *openai.Client
	Google *google.Client
	Log    *slog.Logger
}

// PoolExec is the pgx/v5 pool surface used here (subset of *pgxpool.Pool).
type PoolExec interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconnTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// RegisterEmbedding attaches both embedding task handlers to reg.
func RegisterEmbedding(reg queue.Registrar, deps EmbeddingDeps) error {
	svc := NewEmbeddingService(deps)
	reg.Register(queue.TaskEmbedProduct, svc.embedProductHandler())
	reg.Register(queue.TaskEmbedProductImage, svc.embedProductImageHandler())
	return nil
}

type EmbeddingService struct {
	pool   PoolExec
	openai *openai.Client
	google *google.Client
	log    *slog.Logger
	http   *http.Client
}

func NewEmbeddingService(deps EmbeddingDeps) *EmbeddingService {
	log := deps.Log
	if log == nil {
		log = slog.Default()
	}
	return &EmbeddingService{
		pool:   deps.Pool,
		openai: deps.OpenAI,
		google: deps.Google,
		log:    log,
		http:   &http.Client{Timeout: 60 * time.Second},
	}
}

// --- processor switch (T6.20) ----------------------------------------------

type embedProductPayload struct {
	ProductID string `json:"productId"`
}

type embedProductImagePayload struct {
	ProductImageID string `json:"productImageId"`
}

// DispatchTask is the port of EmbeddingProcessor.process's job-name switch:
// route by task type, error on unknown names. Registration wires both known
// types through this method so behaviour can never diverge between paths.
func (s *EmbeddingService) DispatchTask(ctx context.Context, taskType string, payload []byte) error {
	switch taskType {
	case queue.TaskEmbedProductImage:
		var p embedProductImagePayload
		if err := json.Unmarshal(payload, &p); err != nil || p.ProductImageID == "" {
			return fmt.Errorf("embedding: payload missing productImageId")
		}
		return s.EmbedProductImageRow(ctx, p.ProductImageID, false)
	case queue.TaskEmbedProduct:
		var p embedProductPayload
		if err := json.Unmarshal(payload, &p); err != nil || p.ProductID == "" {
			return fmt.Errorf("embedding: payload missing productId")
		}
		return s.EmbedProduct(ctx, p.ProductID)
	default:
		// Exact message from embedding.processor.ts with the job name (the
		// task type minus its "embedding:" queue prefix).
		return fmt.Errorf("Unknown embedding job type: %s", strings.TrimPrefix(taskType, queue.QEmbedding+":")) //nolint:staticcheck // ST1005: byte-exact port of the TS error text (T6.20)
	}
}

func (s *EmbeddingService) embedProductHandler() queue.Handler {
	return func(ctx context.Context, payload []byte) error {
		return s.DispatchTask(ctx, queue.TaskEmbedProduct, payload)
	}
}

func (s *EmbeddingService) embedProductImageHandler() queue.Handler {
	return func(ctx context.Context, payload []byte) error {
		return s.DispatchTask(ctx, queue.TaskEmbedProductImage, payload)
	}
}

// --- text path (T6.15–T6.18) ------------------------------------------------

// BuildSearchDocument ports EmbeddingService.buildSearchDocument byte-exactly:
//
//	parts = [product.name]
//	if description trims to non-empty: parts.push(description.trim())
//	return parts.join('. ')
//
// jsTrim reproduces JavaScript's String.prototype.trim rune set (which differs
// from Go's unicode.IsSpace on U+FEFF — trimmed — and U+0085 — kept).
func BuildSearchDocument(name string, description *string) string {
	parts := make([]string, 0, 2)
	parts = append(parts, name)
	if description != nil {
		trimmed := jsTrim(*description)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return strings.Join(parts, ". ")
}

func jsTrim(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		if r == 0x85 { // NEL: unicode.IsSpace says yes, JS trim says no
			return false
		}
		return unicode.IsSpace(r) || r == 0xFEFF // ZWNBSP: JS trims, IsSpace doesn't
	})
}

func (s *EmbeddingService) generateTextEmbedding(ctx context.Context, text string) ([]float32, error) {
	return s.openai.Embedding(ctx, TextEmbeddingModel, TextEmbeddingDimensions, text)
}

// EmbedProduct ports EmbeddingService.embedProduct including the raw-SQL
// vector write and its exact not-found error text.
func (s *EmbeddingService) EmbedProduct(ctx context.Context, productID string) error {
	var id, name string
	var description *string
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, description FROM products WHERE id = $1`, productID,
	).Scan(&id, &name, &description)
	switch {
	case errors.Is(err, ErrNoRows):
		return fmt.Errorf("Product not found: %s", productID) //nolint:staticcheck // ST1005: byte-exact port of embedding.service.ts error text (T6.17)
	case err != nil:
		return fmt.Errorf("embedding: fetch product %s: %w", productID, err)
	}

	searchDocument := BuildSearchDocument(name, description)
	s.log.Debug("embedding product",
		slog.String("product_id", id),
		slog.String("search_document", searchDocument))

	vector, err := s.generateTextEmbedding(ctx, searchDocument)
	if err != nil {
		return fmt.Errorf("embedding: generate for product %s: %w", id, err)
	}

	if _, err := s.pool.Exec(ctx,
		`UPDATE products SET embedding = $1::vector WHERE id = $2`,
		pgvector.NewVector(vector), id,
	); err != nil {
		return fmt.Errorf("embedding: write vector for product %s: %w", id, err)
	}

	s.log.Info("text embedded product", slog.String("product", name), slog.String("product_id", id))
	return nil
}

// BulkEmbedResult mirrors BulkEmbedResult in embedding.service.ts.
type BulkEmbedResult struct {
	Total     int              `json:"total"`
	Succeeded int              `json:"succeeded"`
	Failed    int              `json:"failed"`
	Errors    []BulkEmbedError `json:"errors"`
}

type BulkEmbedError struct {
	ProductID string `json:"productId"`
	Error     string `json:"error"`
}

// EmbedAllForBusiness ports EmbeddingService.embedAllForBusiness: sequential
// processing to spare the OpenAI quota, individual failures logged but never
// aborting the batch, partial-failure summary returned.
func (s *EmbeddingService) EmbedAllForBusiness(ctx context.Context, businessID string) (BulkEmbedResult, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM products WHERE business_id = $1`, businessID)
	if err != nil {
		return BulkEmbedResult{}, fmt.Errorf("embedding: list products for business %s: %w", businessID, err)
	}
	productIDs, err := collectStrings(rows)
	if err != nil {
		return BulkEmbedResult{}, err
	}

	result := BulkEmbedResult{Total: len(productIDs)}
	s.log.Info("starting bulk embedding",
		slog.String("business_id", businessID), slog.Int("products", result.Total))

	for _, pid := range productIDs {
		if err := s.EmbedProduct(ctx, pid); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BulkEmbedError{ProductID: pid, Error: err.Error()})
			s.log.Error("bulk embedding item failed",
				slog.String("product_id", pid), slog.Any("error", err))
			continue
		}
		result.Succeeded++
	}

	s.log.Info("bulk embedding complete",
		slog.String("business_id", businessID),
		slog.Int("succeeded", result.Succeeded),
		slog.Int("total", result.Total),
		slog.Int("failed", result.Failed))
	return result, nil
}

// --- image path (T6.19) ------------------------------------------------------

func (s *EmbeddingService) generateImageEmbedding(ctx context.Context, image []byte, mimeType string) ([]float32, error) {
	if mimeType == "" {
		mimeType = defaultImageMIME
	}
	return s.google.EmbedContent(ctx, ImageEmbeddingModel, ImageEmbeddingDimensions,
		[]google.Part{{InlineData: &google.InlineData{
			MimeType: mimeType,
			Data:     base64.StdEncoding.EncodeToString(image),
		}}})
}

func (s *EmbeddingService) generateImageEmbeddingFromURL(ctx context.Context, imageURL string) ([]float32, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imageURL, nil)
	if err != nil {
		return nil, fmt.Errorf("embedding: build image download %s: %w", imageURL, err)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding: download image %s: %w", imageURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Failed to download image from %s: %d", imageURL, resp.StatusCode) //nolint:staticcheck // ST1005: byte-exact port of generateImageEmbeddingFromUrl error text (T6.19)
	}
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = defaultImageMIME
	} else if i := strings.IndexByte(contentType, ';'); i > 0 {
		contentType = strings.TrimSpace(contentType[:i]) // strip charset params
	}
	buf, err := io.ReadAll(io.LimitReader(resp.Body, maxImageBytes))
	if err != nil {
		return nil, fmt.Errorf("embedding: read image %s: %w", imageURL, err)
	}
	return s.generateImageEmbedding(ctx, buf, contentType)
}

// EmbedProductImageRow ports ImageEmbeddingService.embedProductImageRow:
// per-row so a failing image retries alone; skip when already embedded unless
// force; exact TS error texts preserved.
func (s *EmbeddingService) EmbedProductImageRow(ctx context.Context, productImageID string, force bool) error {
	var id, url, productID, productName string
	err := s.pool.QueryRow(ctx, `
		SELECT pi.id, pi.url, p.id, p.name
		FROM product_images pi
		JOIN products p ON p.id = pi.product_id
		WHERE pi.id = $1`, productImageID,
	).Scan(&id, &url, &productID, &productName)
	switch {
	case errors.Is(err, ErrNoRows):
		return fmt.Errorf("Product image not found: %s", productImageID) //nolint:staticcheck // ST1005: byte-exact port of image-embedding.service.ts error text (T6.19)
	case err != nil:
		return fmt.Errorf("embedding: fetch product image %s: %w", productImageID, err)
	}

	if !force {
		var embedded bool
		if err := s.pool.QueryRow(ctx,
			`SELECT (embedding IS NOT NULL) AS embedded FROM product_images WHERE id = $1`,
			productImageID,
		).Scan(&embedded); err != nil {
			return fmt.Errorf("embedding: check embedded state for image %s: %w", productImageID, err)
		}
		if embedded {
			s.log.Debug("product image already embedded; skipping", slog.String("image_id", productImageID))
			return nil
		}
	}

	s.log.Info("embedding product image from url",
		slog.String("image_id", id),
		slog.String("product", productName),
		slog.String("url", url))

	vector, err := s.generateImageEmbeddingFromURL(ctx, url)
	if err != nil {
		return fmt.Errorf("embedding: generate for image %s: %w", id, err)
	}

	if _, err := s.pool.Exec(ctx,
		`UPDATE product_images SET embedding = $1::vector WHERE id = $2`,
		pgvector.NewVector(vector), id,
	); err != nil {
		return fmt.Errorf("embedding: write vector for image %s: %w", id, err)
	}

	s.log.Info("image embedded", slog.String("image_id", id), slog.String("product_id", productID))
	return nil
}

// EmbedProductImages ports ImageEmbeddingService.embedProductImages: every
// image sequentially; individual failures warn and continue.
func (s *EmbeddingService) EmbedProductImages(ctx context.Context, productID string, force bool) error {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM product_images WHERE product_id = $1 ORDER BY position ASC`, productID)
	if err != nil {
		return fmt.Errorf("embedding: list images for product %s: %w", productID, err)
	}
	imageIDs, err := collectStrings(rows)
	if err != nil {
		return err
	}
	if len(imageIDs) == 0 {
		s.log.Warn("product has no images; skipping image embedding", slog.String("product_id", productID))
		return nil
	}
	for _, imgID := range imageIDs {
		if err := s.EmbedProductImageRow(ctx, imgID, force); err != nil {
			s.log.Warn("failed to embed image for product (continuing)",
				slog.String("image_id", imgID),
				slog.String("product_id", productID),
				slog.Any("error", err))
		}
	}
	return nil
}

// collectStrings drains a single-column string query (used for id lists).
func collectStrings(rows Rows) ([]string, error) {
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("embedding: scan id row: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("embedding: iterate id rows: %w", err)
	}
	return out, nil
}

// SimilarProduct mirrors findSimilarProductsByImage's row shape.
type SimilarProduct struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	Price      float64 `json:"price"`
	ImageURL   *string `json:"imageUrl"`
	Distance   float64 `json:"-"`
	Similarity float64 `json:"similarity"`
}

// FindSimilarProductsByImage ports ImageEmbeddingService.findSimilarProductsByImage
// verbatim, including the two-level DISTINCT ON structure (the inner query
// picks each product's best angle; the outer ranks and limits — Postgres
// demands this split).
func (s *EmbeddingService) FindSimilarProductsByImage(
	ctx context.Context,
	businessID string,
	image []byte,
	mimeType string,
	threshold float64,
	limit int,
) ([]SimilarProduct, error) {
	vector, err := s.generateImageEmbedding(ctx, image, mimeType)
	if err != nil {
		return nil, fmt.Errorf("embedding: generate query image vector: %w", err)
	}
	cosineDistanceThreshold := 1 - threshold

	rows, err := s.pool.Query(ctx, `
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
		LIMIT $4;`, businessID, pgvector.NewVector(vector), cosineDistanceThreshold, limit)
	if err != nil {
		return nil, fmt.Errorf("embedding: similarity query for business %s: %w", businessID, err)
	}
	defer rows.Close()

	out := make([]SimilarProduct, 0)
	for rows.Next() {
		var r SimilarProduct
		if err := rows.Scan(&r.ID, &r.Name, &r.Price, &r.ImageURL, &r.Distance); err != nil {
			return nil, fmt.Errorf("embedding: scan similarity row: %w", err)
		}
		r.Similarity = 1 - r.Distance
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("embedding: iterate similarity rows: %w", err)
	}
	return out, nil
}
