package handlers

// products_write.go ports the product CRUD surface of
// apps/mobile-api/src/products/products.{controller,service}.ts (T4.2b/c,
// T4.4–T4.7, T4.23c): zod(v4)-shaped 400s, Prisma-payload responses, dynamic
// partial UPDATE mirroring Prisma's dto spread, cascade delete with
// best-effort Cloudinary cleanup, and an EmbedPublisher hook on every
// create/update (T4.1 seam — see internal/storage.EmbedPublisher).
//
// Mount: chi Mount("/products", MountProductsWrite(deps)) alongside the
// Stage-2 read subtree from NewProducts.

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/money"
	"github.com/novoapex/novoapex-backend-api/internal/storage"
)

// ProductsDeps wires the write surface. Storage defaults to the env-configured
// Cloudinary provider and Embeds to storage.NullEmbedder when nil.
type ProductsDeps struct {
	Pool    *pgxpool.Pool
	Storage storage.StorageProvider
	Embeds  storage.EmbedPublisher
}

func (d ProductsDeps) embedder() storage.EmbedPublisher {
	if d.Embeds != nil {
		return d.Embeds
	}
	return storage.NullEmbedder{}
}

// MountProductsWrite attaches the product write surface under /products on
// the given router (the constructor central integration wires in cmd/api).
func MountProductsWrite(r chi.Router, deps ProductsDeps) {
	r.Post("/", s4p_productCreate(deps))
	r.Patch("/{id}", s4p_productPatch(deps))
	r.Delete("/{id}", s4p_productDelete(deps))
	r.Post("/{id}/out-of-stock", s4p_productOutOfStock(deps))
	r.Post("/{id}/image", s4p_imageUpload(deps, false))
	r.Post("/{id}/images", s4p_imageUpload(deps, true))
	r.Delete("/{id}/images/{imageId}", s4p_imageRemove(deps))
	r.Put("/{id}/images/reorder", s4p_imageReorder(deps))
}

// ---- shared small helpers -------------------------------------------------

// s4p_newUUID renders a random RFC-4122 v4 UUID without adding a dependency;
// source uses crypto.randomUUID() for image row ids.
func s4p_newUUID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// s4p_decodeBody decodes a JSON object body into v. An empty body behaves
// like express.json() leaving req.body as {} (all optional fields absent);
// malformed JSON answers the filter envelope with the parser's message.
func s4p_decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, err.Error()))
		return false
	}
	if len(raw) == 0 {
		return true // {} — absent fields surface through validation
	}
	if err := json.Unmarshal(raw, v); err != nil {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest,
			"Unexpected token in JSON: "+err.Error()))
		return false
	}
	return true
}

// s4p_rawBody decodes a JSON object body into a key -> raw JSON map so each
// field can be type-checked the way zod sees it (missing vs null vs wrong
// type vs value).
func s4p_rawBody(w http.ResponseWriter, r *http.Request) (map[string]json.RawMessage, bool) {
	var m map[string]json.RawMessage
	if !s4p_decodeBody(w, r, &m) {
		return nil, false
	}
	if m == nil {
		m = map[string]json.RawMessage{}
	}
	return m, true
}

// ---- zod-v4 issue emitters -------------------------------------------------
// Messages follow zod 4.3.x defaults (node_modules/zod/v4/locales/en.js):
//   invalid_type  "Invalid input: expected X, received Y"
//   too_small     "Too small: expected string to have >=N characters" /
//                 "Too small: expected number to be >N|>=N" /
//                 "Too small: expected array to have >=N items"
//   too_big       symmetric
//   invalid_format(uuid) "Invalid UUID"
//   custom        the refine message verbatim.

const s4pSafeIntMax = 9007199254740991 // Number.MAX_SAFE_INTEGER

// s4p_receivedType maps a raw JSON value onto util.parsedType(input) names.
func s4p_receivedType(raw json.RawMessage) string {
	switch {
	case raw == nil:
		return "undefined"
	case bytesEqual(raw, []byte("null")):
		return "null"
	case bytesEqual(raw, []byte("true")), bytesEqual(raw, []byte("false")):
		return "boolean"
	case len(raw) > 0 && raw[0] == '"':
		return "string"
	case len(raw) > 0 && (raw[0] == '['):
		return "array"
	case len(raw) > 0 && raw[0] == '{':
		return "object"
	default:
		return "number"
	}
}

func bytesEqual(a, b []byte) bool { return string(a) == string(b) }

func s4p_invalidType(key, expected string, raw json.RawMessage) httpx.FieldIssue {
	return httpx.FieldIssue{
		Code:    "invalid_type",
		Path:    key,
		Message: fmt.Sprintf("Invalid input: expected %s, received %s", expected, s4p_receivedType(raw)),
	}
}

func s4p_tooSmallString(key string, min int) httpx.FieldIssue {
	return httpx.FieldIssue{Code: "too_small", Path: key,
		Message: fmt.Sprintf("Too small: expected string to have >=%d characters", min)}
}

// s4p_reqString validates a required z.string().min(n).
func s4p_reqString(issues *[]httpx.FieldIssue, key string, raw json.RawMessage, min int) *string {
	return s4p_checkString(issues, key, raw, true, min)
}

// s4p_optString validates a z.string().optional().
func s4p_optString(issues *[]httpx.FieldIssue, key string, raw json.RawMessage) *string {
	return s4p_checkString(issues, key, raw, false, 0)
}

func s4p_checkString(issues *[]httpx.FieldIssue, key string, raw json.RawMessage, required bool, min int) *string {
	if raw == nil {
		if required {
			*issues = append(*issues, s4p_invalidType(key, "string", nil))
		}
		return nil
	}
	if s4p_receivedType(raw) != "string" {
		*issues = append(*issues, s4p_invalidType(key, "string", raw))
		return nil
	}
	var s string
	_ = json.Unmarshal(raw, &s)
	if min > 0 && len([]rune(s)) < min {
		*issues = append(*issues, s4p_tooSmallString(key, min))
		return nil
	}
	return &s
}

// s4p_checkNumber validates a z.number() leaf: presence/type, then optional
// .positive()/.int()/.nonnegative() refinements in declaration order.
func s4p_checkNumber(issues *[]httpx.FieldIssue, key string, raw json.RawMessage,
	required, positive, integer, nonnegative bool) *decimal.Decimal {
	if raw == nil {
		if required {
			*issues = append(*issues, s4p_invalidType(key, "number", nil))
		}
		return nil
	}
	t := s4p_receivedType(raw)
	if t != "number" {
		*issues = append(*issues, s4p_invalidType(key, "number", raw))
		return nil
	}
	n, err := decimal.NewFromString(string(raw))
	if err != nil {
		*issues = append(*issues, s4p_invalidType(key, "number", raw))
		return nil
	}

	if integer && !n.Equal(n.Truncate(0)) {
		*issues = append(*issues, s4p_invalidType(key, "int", raw)) // "expected int, received number"
		return nil
	}
	if positive && n.Sign() <= 0 {
		*issues = append(*issues, httpx.FieldIssue{Code: "too_small", Path: key,
			Message: "Too small: expected number to be >0"})
		return nil
	}
	if nonnegative && n.Sign() < 0 {
		*issues = append(*issues, httpx.FieldIssue{Code: "too_small", Path: key,
			Message: "Too small: expected number to be >=0"})
		return nil
	}
	if integer {
		if n.GreaterThan(decimal.NewFromBigInt(big.NewInt(s4pSafeIntMax), 0)) {
			*issues = append(*issues, httpx.FieldIssue{Code: "too_big", Path: key,
				Message: fmt.Sprintf("Too big: expected int to be <=%d", s4pSafeIntMax)})
			return nil
		}
		if n.LessThan(decimal.NewFromBigInt(big.NewInt(-s4pSafeIntMax), 0)) {
			*issues = append(*issues, httpx.FieldIssue{Code: "too_small", Path: key,
				Message: fmt.Sprintf("Too small: expected int to be >=%d", -s4pSafeIntMax)})
			return nil
		}
	}
	return &n
}

func s4p_checkBool(issues *[]httpx.FieldIssue, key string, raw json.RawMessage, required bool) *bool {
	if raw == nil {
		if required {
			*issues = append(*issues, s4p_invalidType(key, "boolean", nil))
		}
		return nil
	}
	if !bytesEqual(raw, []byte("true")) && !bytesEqual(raw, []byte("false")) {
		*issues = append(*issues, s4p_invalidType(key, "boolean", raw))
		return nil
	}
	v := bytesEqual(raw, []byte("true"))
	return &v
}

// ---- product DTO validation (T4.2b/c) --------------------------------------

// s4p_productInput is the validated shape of Create/UpdateProductSchema.
type s4p_productInput struct {
	Name         *string
	Description  *string
	Price        *decimal.Decimal
	Stock        *decimal.Decimal
	Sku          *string
	IsAvailable  *bool
	Category     *string
	StockNote    *string
	DeliveryNote *string
}

// s4p_validateProduct walks the schema keys in declaration order collecting
// ALL issues (zod does not short-circuit across object properties).
// required selects CreateProductSchema vs UpdateProductSchema.
func s4p_validateProduct(m map[string]json.RawMessage, required bool) (*s4p_productInput, []httpx.FieldIssue) {
	in := &s4p_productInput{}
	issues := make([]httpx.FieldIssue, 0, 4)

	in.Name = s4p_checkString(&issues, "name", m["name"], required, 2)
	in.Description = s4p_checkString(&issues, "description", m["description"], false, 0)
	in.Price = s4p_checkNumber(&issues, "price", m["price"], required, true, false, false)
	stockRaw := s4p_checkNumber(&issues, "stock", m["stock"], required, false, true, true)
	in.Stock = stockRaw
	in.Sku = s4p_checkString(&issues, "sku", m["sku"], false, 0)
	in.IsAvailable = s4p_checkBool(&issues, "isAvailable", m["isAvailable"], false)
	in.Category = s4p_checkString(&issues, "category", m["category"], false, 0)
	in.StockNote = s4p_checkString(&issues, "stockNote", m["stockNote"], false, 0)
	in.DeliveryNote = s4p_checkString(&issues, "deliveryNote", m["deliveryNote"], false, 0)

	if len(issues) > 0 {
		return nil, issues
	}
	return in, nil
}

// ---- response assembly -----------------------------------------------------

// s4p_productCols is the Prisma payload column list shared by every
// product-returning query (no embedding — Unsupported on the Node side).
const s4p_productCols = `id, business_id, name, description, category, request_count,
stock_note, delivery_note, price, stock, sku, is_available, created_at, updated_at`

// s4p_productRow mirrors those columns regardless of which query produced them.
type s4p_productRow struct {
	ID           string
	BusinessID   string
	Name         string
	Description  pgtype.Text
	Category     pgtype.Text
	RequestCount int32
	StockNote    pgtype.Text
	DeliveryNote pgtype.Text
	Price        decimal.Decimal
	Stock        int32
	Sku          pgtype.Text
	IsAvailable  bool
	CreatedAt    pgtype.Timestamp
	UpdatedAt    pgtype.Timestamp
}

func (row *s4p_productRow) scanFrom(rs pgx.Rows) error {
	return rs.Scan(&row.ID, &row.BusinessID, &row.Name, &row.Description, &row.Category,
		&row.RequestCount, &row.StockNote, &row.DeliveryNote, &row.Price, &row.Stock,
		&row.Sku, &row.IsAvailable, &row.CreatedAt, &row.UpdatedAt)
}

func s4p_mapProductRow(row s4p_productRow, imgs []gen.ListProductImagesForProductsRow) productJSON {
	pj := productJSON{
		ID:           row.ID,
		BusinessID:   row.BusinessID,
		Name:         row.Name,
		Description:  ep_text(row.Description),
		Category:     ep_text(row.Category),
		RequestCount: row.RequestCount,
		StockNote:    ep_text(row.StockNote),
		DeliveryNote: ep_text(row.DeliveryNote),
		Price:        ep_num(row.Price),
		Stock:        row.Stock,
		SKU:          ep_text(row.Sku),
		IsAvailable:  row.IsAvailable,
		CreatedAt:    epISO(row.CreatedAt.Time),
		UpdatedAt:    epISO(row.UpdatedAt.Time),
		Images:       ep_productImages(imgs),
	}
	if len(pj.Images) > 0 {
		url := pj.Images[0].URL
		pj.ImageURL = &url
	}
	return pj
}

// s4p_findOne ports ProductsService.findOne (404 message included): the
// ownership check AND the response payload loader for every write path.
func (deps ProductsDeps) findOne(ctx context.Context, pool *pgxpool.Pool, businessID, id string) (s4p_productRow, []gen.ListProductImagesForProductsRow, error) {
	rows, err := pool.Query(ctx, `SELECT `+s4p_productCols+` FROM products WHERE id = $1 AND business_id = $2`, id, businessID)
	if err != nil {
		return s4p_productRow{}, nil, err
	}
	row := s4p_productRow{}
	if !rows.Next() {
		rows.Close()
		return s4p_productRow{}, nil, pgx.ErrNoRows
	}
	if err := row.scanFrom(rows); err != nil {
		rows.Close()
		return s4p_productRow{}, nil, err
	}
	rows.Close()

	imgs, err := gen.New(pool).ListProductImagesForProducts(ctx, []string{id})
	if err != nil {
		return s4p_productRow{}, nil, err
	}
	return row, imgs, nil
}

// s4p_writeFindOne runs findOne and translates ErrNoRows into the Nest 404.
func s4p_writeFindOne(w http.ResponseWriter, r *http.Request, deps ProductsDeps, bizID, id string) (s4p_productRow, []gen.ListProductImagesForProductsRow, bool) {
	row, imgs, err := deps.findOne(r.Context(), deps.Pool, bizID, id)
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Product not found"))
		return s4p_productRow{}, nil, false
	}
	if err != nil {
		httpx.WriteError(w, r, err)
		return s4p_productRow{}, nil, false
	}
	return row, imgs, true
}

func s4p_respondProduct(w http.ResponseWriter, status int, row s4p_productRow, imgs []gen.ListProductImagesForProductsRow) {
	_ = httpx.WriteJSON(w, status, s4p_mapProductRow(row, imgs))
}

// ---- POST /products (T4.4) ----------------------------------------------

// s4p_productCreate ports ProductsService.create: prisma.product.create with
// the dto spread over DB defaults, then one durable 'embed-product' job.
func s4p_productCreate(deps ProductsDeps) http.HandlerFunc {
	q := gen.New(deps.Pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		m, ok := s4p_rawBody(w, r)
		if !ok {
			return
		}
		in, issues := s4p_validateProduct(m, true)
		if issues != nil {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		isAvailable := true
		if in.IsAvailable != nil {
			isAvailable = *in.IsAvailable
		}
		id := s4p_newUUID()
		created, err := q.CreateProduct(r.Context(), gen.CreateProductParams{
			ID:           id,
			BusinessID:   bizID,
			Name:         deref(in.Name),
			Description:  s4p_nillableText(in.Description),
			Price:        *in.Price,
			Stock:        int32(in.Stock.IntPart()),
			Sku:          s4p_nillableText(in.Sku),
			IsAvailable:  isAvailable,
			Category:     s4p_nillableText(in.Category),
			StockNote:    s4p_nillableText(in.StockNote),
			DeliveryNote: s4p_nillableText(in.DeliveryNote),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		s4p_publishProductEmbed(r.Context(), deps, created.ID)
		imgs, err := q.ListProductImagesForProducts(r.Context(), []string{created.ID})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		s4p_respondProduct(w, http.StatusCreated, s4p_rowFromCreate(created), imgs)
	}
}

func s4p_rowFromCreate(c gen.CreateProductRow) s4p_productRow {
	return s4p_productRow{
		ID: c.ID, BusinessID: c.BusinessID, Name: c.Name, Description: c.Description,
		Category: c.Category, RequestCount: c.RequestCount, StockNote: c.StockNote,
		DeliveryNote: c.DeliveryNote, Price: c.Price, Stock: c.Stock, Sku: c.Sku,
		IsAvailable: c.IsAvailable, CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt,
	}
}

// s4p_publishProductEmbed wraps PublishProductEmbed: enqueue failures are
// logged, never surfaced (source enqueueEmbedding contract).
func s4p_publishProductEmbed(ctx context.Context, deps ProductsDeps, productID string) {
	if err := deps.embedder().PublishProductEmbed(ctx, productID); err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf(
			"Failed to enqueue embedding for product %s: %v", productID, err))
	}
}

func s4p_publishImageEmbed(ctx context.Context, deps ProductsDeps, imageID string) {
	if err := deps.embedder().PublishImageEmbed(ctx, imageID); err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf(
			"Failed to enqueue image embedding for [%s]: %v", imageID, err))
	}
}

// ---- PATCH /products/{id} (T4.5, T4.23c) ----------------------------------

type s4p_set struct {
	col string
	val any
}

// s4p_productPatch ports ProductsService.update: ownership check via findOne,
// then prisma.update({data: dto}) — ONLY provided columns are written, plus
// @updatedAt. Any successful update re-enqueues 'embed-product'.
func s4p_productPatch(deps ProductsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		id := chi.URLParam(r, "id")

		m, ok := s4p_rawBody(w, r)
		if !ok {
			return
		}
		in, issues := s4p_validateProduct(m, false)
		if issues != nil {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		if _, _, ok := s4p_writeFindOne(w, r, deps, bizID, id); !ok {
			return
		}

		sets := make([]s4p_set, 0, 9)
		if in.Name != nil {
			sets = append(sets, s4p_set{"name", *in.Name})
		}
		if in.Description != nil {
			sets = append(sets, s4p_set{"description", s4p_textArg(in.Description)})
		}
		if in.Price != nil {
			sets = append(sets, s4p_set{"price", *in.Price})
		}
		if in.Stock != nil {
			sets = append(sets, s4p_set{"stock", int32(in.Stock.IntPart())})
		}
		if in.Sku != nil {
			sets = append(sets, s4p_set{"sku", s4p_textArg(in.Sku)})
		}
		if in.IsAvailable != nil {
			sets = append(sets, s4p_set{"is_available", *in.IsAvailable})
		}
		if in.Category != nil {
			sets = append(sets, s4p_set{"category", s4p_textArg(in.Category)})
		}
		if in.StockNote != nil {
			sets = append(sets, s4p_set{"stock_note", s4p_textArg(in.StockNote)})
		}
		if in.DeliveryNote != nil {
			sets = append(sets, s4p_set{"delivery_note", s4p_textArg(in.DeliveryNote)})
		}

		row, err := s4p_execUpdate(r.Context(), deps.Pool, id, sets)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		s4p_publishProductEmbed(r.Context(), deps, id)
		q := gen.New(deps.Pool)
		imgs, err := q.ListProductImagesForProducts(r.Context(), []string{id})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		s4p_respondProduct(w, http.StatusOK, row, imgs)
	}
}

// s4p_execUpdate builds the partial UPDATE (Prisma data spread) and re-reads
// the payload columns in one statement. updated_at always moves (@updatedAt).
func s4p_execUpdate(ctx context.Context, pool *pgxpool.Pool, id string, sets []s4p_set) (s4p_productRow, error) {
	query := "UPDATE products SET "
	args := make([]any, 0, len(sets)+1)
	for i, s := range sets {
		if i > 0 {
			query += ", "
		}
		args = append(args, s.val)
		query += fmt.Sprintf("%s = $%d", s.col, len(args))
	}
	query += ", updated_at = CURRENT_TIMESTAMP WHERE id = $" + fmt.Sprint(len(args)+1)
	args = append(args, id)
	query += " RETURNING " + s4p_productCols

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return s4p_productRow{}, err
	}
	defer rows.Close()
	row := s4p_productRow{}
	if !rows.Next() {
		return s4p_productRow{}, pgx.ErrNoRows
	}
	if err := row.scanFrom(rows); err != nil {
		return s4p_productRow{}, err
	}
	return row, nil
}

// ---- DELETE /products/{id} (T4.6) ------------------------------------------

// s4p_productDelete ports ProductsService.remove: capture storage keys before
// the cascade, delete the row, then fire-and-forget remote cleanup that can
// never turn the success into a 500.
func s4p_productDelete(deps ProductsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		id := chi.URLParam(r, "id")
		q := gen.New(deps.Pool)

		_, imgs, ok := s4p_writeFindOne(w, r, deps, bizID, id)
		if !ok {
			return
		}

		err := q.DeleteProductByIDAndBusiness(r.Context(),
			gen.DeleteProductByIDAndBusinessParams{ID: id, BusinessID: bizID})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		for _, img := range imgs {
			if img.StorageKey.Valid && img.StorageKey.String != "" {
				s4p_deleteAsset(r.Context(), deps.Storage, img.StorageKey.String)
			}
		}

		_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{
			"message": "Product deleted successfully",
		})
	}
}

// s4p_deleteAsset mirrors deleteAssets(): swallow everything, an orphaned
// Cloudinary asset is a cost problem, not a correctness one.
func s4p_deleteAsset(ctx context.Context, sp storage.StorageProvider, publicID string) {
	if sp == nil {
		return
	}
	if err := sp.DeleteImage(ctx, publicID); err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf(
			"Failed to delete stored asset '%s' (non-blocking): %v", publicID, err))
	}
}

// ---- POST /products/{id}/out-of-stock (T4.7) -------------------------------

// s4p_productOutOfStock ports markOutOfStock: it routes THROUGH update, so
// stock=0 + isAvailable=false land in one statement and 'embed-product' is
// re-enqueued like any other update.
func s4p_productOutOfStock(deps ProductsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		id := chi.URLParam(r, "id")

		if _, _, ok := s4p_writeFindOne(w, r, deps, bizID, id); !ok {
			return
		}

		zero := decimal.Zero
		row, err := s4p_execUpdate(r.Context(), deps.Pool, id, []s4p_set{
			{"stock", int32(zero.IntPart())},
			{"is_available", false},
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		s4p_publishProductEmbed(r.Context(), deps, id)
		q := gen.New(deps.Pool)
		imgs, err := q.ListProductImagesForProducts(r.Context(), []string{id})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		s4p_respondProduct(w, http.StatusCreated, row, imgs)
	}
}

// ---- param/text plumbing ---------------------------------------------------

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// s4p_nillableText converts an optional-string pointer to pgtype.Text where
// nil means ABSENT (zod .optional() cannot produce explicit null).
func s4p_nillableText(s *string) pgtype.Text {
	if s == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *s, Valid: true}
}

func s4p_textArg(s *string) any {
	return s4p_nillableText(s)
}

// money.Number keeps wire parity for callers that need it; referenced so the
// import survives refactors (price serialization flows through ep_num).
var _ = money.Number{}
