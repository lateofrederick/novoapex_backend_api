package handlers

// images.go ports the product-image pipeline of
// apps/mobile-api/src/products/products.service.ts (T4.10–T4.15, T4.23a):
//
//   - multer FilesInterceptor('files',5)/FileInterceptor('file') +
//     ParseFilePipe parity for the multipart contract (field names, 5MB,
//     png/jpeg magic numbers);
//   - SHA-256 content-hash dedup BEFORE upload (skips Cloudinary AND the
//     embedding enqueue), with the (product_id, content_hash) unique index
//     as the concurrent-race backstop;
//   - positions appended densely inside the insert transaction;
//   - delete with ascending re-densification;
//   - reorder as a two-phase write under @@unique([productId, position]):
//     park at +REORDER_OFFSET -> write finals -> all inside ONE transaction.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/storage"
)

const (
	s4pMaxImages      = 5    // ProductsService.MAX_IMAGES
	s4pReorderOffset  = 1000 // ProductsService.REORDER_OFFSET
	s4pMaxUploadBytes = 5 * 1024 * 1024
)

// ---- POST /{id}/image and /{id}/images (T4.10, T4.11) ----------------------

type s4p_uploadFile struct {
	data     []byte
	mimetype string // client-provided Content-Type (for validator messages)
}

// s4p_imageUpload serves both aliases; single expects field 'file', multi
// 'files' capped at five parts.
func s4p_imageUpload(deps ProductsDeps, multi bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		if deps.Storage == nil {
			httpx.WriteError(w, r, errors.New("storage provider not configured"))
			return
		}
		id := chi.URLParam(r, "id")

		files, merr := s4p_parseMultipart(r, multi)
		if merr != nil {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, *merr))
			return
		}
		if len(files) == 0 { // ParseFilePipe fileIsRequired default
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, "File is required"))
			return
		}
		if msg := s4p_validateFiles(files); msg != nil {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, *msg))
			return
		}

		s4p_uploadImages(w, r, deps, bizID, id, files)
	}
}

// s4p_parseMultipart reproduces the observable behavior of multer's
// array('files',5)/single('file') on top of busboy:
//   - a request that is not multipart/form-data is skipped by multer entirely
//     (zero files -> the pipe answers 'File is required');
//   - any file part under an unexpected field name -> LIMIT_UNEXPECTED_FILE,
//     rendered by transformException as "Unexpected field - <field>";
//   - a second 'file' part / a sixth 'files' part -> same error naming it;
//   - multipart header with a broken body -> busboy's Multipart: ... errors.
//
// The returned string pointer is the BadRequestException message.
func s4p_parseMultipart(r *http.Request, multi bool) ([]s4p_uploadFile, *string) {
	field := "files"
	maxCount := 0 // unlimited until maxCount applies
	if !multi {
		field = "file"
		maxCount = 1
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		return nil, nil // multer skips; ParseFilePipe reports the missing file
	}
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, ptr("Multipart: Boundary not found")
	}

	files := make([]s4p_uploadFile, 0, 2)
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, ptr("Multipart: Malformed part header")
		}
		name := part.FormName()
		if part.FileName() == "" { // text fields land in req.body; irrelevant here
			_, _ = io.Copy(io.Discard, io.LimitReader(part, 1<<20))
			continue
		}
		if name != field || (maxCount > 0 && len(files) >= maxCount) || (multi && len(files) >= s4pMaxImages) {
			return nil, ptr("Unexpected field - " + name)
		}
		data, err := io.ReadAll(part)
		if err != nil {
			return nil, ptr("Multipart: Unexpected end of file")
		}
		ct := part.Header.Get("Content-Type")
		if ct == "" {
			ct = mime.TypeByExtension(part.FileName())
		}
		files = append(files, s4p_uploadFile{data: data, mimetype: ct})
	}
	return files, nil
}

func ptr[T any](v T) *T { return &v }

// s4p_validateFiles runs IMAGE_VALIDATORS in order per file:
// MaxFileSizeValidator({maxSize: 5*1024*1024}) — strict less-than — then
// FileTypeValidator('.(png|jpeg|jpg)'), which in this Nest version inspects
// magic numbers via file-type with NO mimetype fallback configured.
func s4p_validateFiles(files []s4p_uploadFile) *string {
	for _, f := range files {
		if len(f.data) >= s4pMaxUploadBytes {
			return ptr(fmt.Sprintf(
				"Validation failed (current file size is %d, expected size is less than %d)",
				len(f.data), s4pMaxUploadBytes))
		}
		if detected := s4p_sniffImageMime(f.data); detected == "" {
			return ptr(fmt.Sprintf(
				"Validation failed (current file type is %s, expected type is .(png|jpeg|jpg))",
				f.mimetype))
		}
	}
	return nil
}

// s4p_sniffImageMime stands in for file-type's fileTypeFromBuffer for the two
// accepted formats. Empty when the magic number is unrecognized.
func s4p_sniffImageMime(b []byte) string {
	switch {
	case len(b) >= 8 && string(b[:8]) == "\x89PNG\r\n\x1a\n":
		return "image/png"
	case len(b) >= 3 && b[0] == 0xFF && b[1] == 0xD8 && b[2] == 0xFF:
		return "image/jpeg"
	default:
		return ""
	}
}

type s4p_hashed struct {
	file        s4p_uploadFile
	contentHash string
}

// s4p_uploadImages ports ProductsService.uploadImages end-to-end:
// ownership guard -> hash -> intra-request dedup -> stored-hash dedup ->
// concurrent uploads -> transactional count/position check + inserts ->
// per-image embed enqueue -> findOne payload.
func s4p_uploadImages(w http.ResponseWriter, r *http.Request, deps ProductsDeps, bizID, id string, files []s4p_uploadFile) {
	q := gen.New(deps.Pool)
	ctx := r.Context()

	// Ownership guard BEFORE any bytes reach storage.
	if _, _, ok := s4p_writeFindOne(w, r, deps, bizID, id); !ok {
		return
	}

	hashed := make([]s4p_hashed, len(files))
	for i, f := range files {
		sum := sha256.Sum256(f.data)
		hashed[i] = s4p_hashed{f, hex.EncodeToString(sum[:])}
	}

	// Duplicates within this request — the same photo picked twice. Keep first.
	seen := make(map[string]bool, len(hashed))
	distinct := make([]s4p_hashed, 0, len(hashed))
	for _, h := range hashed {
		if seen[h.contentHash] {
			continue
		}
		seen[h.contentHash] = true
		distinct = append(distinct, h)
	}

	// Duplicates of what the product already holds.
	hashList := make([]string, len(distinct))
	for i, h := range distinct {
		hashList[i] = h.contentHash
	}
	storedRows, err := q.FindStoredImageHashes(ctx, gen.FindStoredImageHashesParams{
		ProductID: id, Column2: hashList,
	})
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	stored := make(map[string]bool, len(storedRows))
	for _, t := range storedRows {
		if t.Valid && t.String != "" {
			stored[t.String] = true
		}
	}
	fresh := make([]s4p_hashed, 0, len(distinct))
	for _, h := range distinct {
		if !stored[h.contentHash] {
			fresh = append(fresh, h)
		}
	}
	if skipped := len(files) - len(fresh); skipped > 0 {
		slog.InfoContext(ctx, fmt.Sprintf("Skipped %d duplicate image(s) for product %s.", skipped, id))
	}

	// Every file was a duplicate — nothing to do, and no error: this is the
	// retry path working as intended.
	if len(fresh) == 0 {
		s4p_respondFoundProduct(w, r, deps, bizID, id)
		return
	}

	folder := "novoapex/products/" + bizID
	urls, publicIDs, upErrs := s4p_uploadConcurrently(ctx, deps.Storage, folder, fresh)
	if firstErr(upErrs) != nil {
		// Nothing was persisted; whatever succeeded is an unreachable orphan.
		for j := range fresh {
			if upErrs[j] == nil {
				s4p_deleteAsset(ctx, deps.Storage, publicIDs[j])
			}
		}
		httpx.WriteError(w, r, firstErr(upErrs))
		return
	}

	insertedIDs, insertErr := s4p_insertImageRows(ctx, deps.Pool, id, fresh, urls, publicIDs)
	if insertErr != nil {
		// Nothing was persisted (single-transaction rollback), so every asset
		// we just uploaded is an unreachable orphan — compensate regardless of
		// WHY the transaction failed.
		for j := range fresh {
			s4p_deleteAsset(ctx, deps.Storage, publicIDs[j])
		}
		var pgErr *pgconn.PgError
		if errors.As(insertErr, &pgErr) && pgErr.Code == "23505" {
			// A concurrent upload took the next position or stored these exact
			// bytes first. A retry now short-circuits on the pre-check.
			// Skip-success: the client gets the product back instead of an error.
			s4p_respondFoundProduct(w, r, deps, bizID, id)
			return
		}
		httpx.WriteError(w, r, insertErr)
		return
	}

	for _, imgID := range insertedIDs {
		s4p_publishImageEmbed(ctx, deps, imgID)
	}
	s4p_respondFoundProduct(w, r, deps, bizID, id)
}

// s4p_uploadConcurrently mirrors Promise.allSettled: independent round trips,
// input order preserved via result indices, failures collected per slot.
func s4p_uploadConcurrently(ctx context.Context, sp storage.StorageProvider, folder string, fresh []s4p_hashed) ([]string, []string, []error) {
	urls := make([]string, len(fresh))
	publicIDs := make([]string, len(fresh))
	errs := make([]error, len(fresh))
	var wg sync.WaitGroup
	for i := range fresh {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			urls[i], publicIDs[i], errs[i] = sp.UploadImage(ctx, fresh[i].file.data, folder)
		}(i)
	}
	wg.Wait()
	return urls, publicIDs, errs
}

func firstErr(errs []error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// s4p_insertImageRows runs the serialized count+max aggregate and the batch
// insert inside ONE transaction, mirroring the source's $transaction block
// (count read outside it would let two concurrent uploads both observe 3).
// Returns the inserted row ids for the embedding jobs.
func s4p_insertImageRows(ctx context.Context, pool *pgxpool.Pool,
	productID string, fresh []s4p_hashed, urls, publicIDs []string) ([]string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	txq := gen.New(tx)

	agg, err := txq.AggregateProductImages(ctx, productID)
	if err != nil {
		return nil, err
	}
	if agg.Total+int64(len(fresh)) > s4pMaxImages {
		return nil, httpx.NewHTTPException(http.StatusBadRequest, fmt.Sprintf(
			"A product can have at most %d images (has %d, tried to add %d).",
			s4pMaxImages, agg.Total, len(fresh)))
	}

	nextPosition := agg.MaxPosition + 1
	ids := make([]string, len(fresh))
	for i, h := range fresh {
		ids[i] = s4p_newUUID()
		if err := txq.InsertProductImage(ctx, gen.InsertProductImageParams{
			ID:          ids[i],
			ProductID:   productID,
			Url:         urls[i],
			StorageKey:  pgtype.Text{String: publicIDs[i], Valid: true},
			ContentHash: pgtype.Text{String: h.contentHash, Valid: true},
			Position:    nextPosition + int32(i),
		}); err != nil {
			return nil, err
		}
	}
	return ids, tx.Commit(ctx)
}

// s4p_respondFoundProduct re-reads and answers with the findOne payload.
func s4p_respondFoundProduct(w http.ResponseWriter, r *http.Request, deps ProductsDeps, bizID, id string) {
	row, imgs, ok := s4p_writeFindOne(w, r, deps, bizID, id)
	if !ok {
		return
	}
	s4p_respondProduct(w, http.StatusCreated, row, imgs)
}

// ---- DELETE /products/{id}/images/{imageId} (T4.13) ------------------------

// s4p_imageRemove ports removeImage: delete then close the gap ascending so
// positions stay dense; asset cleanup is best-effort and never blocks.
func s4p_imageRemove(deps ProductsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		id := chi.URLParam(r, "id")
		imageID := chi.URLParam(r, "imageId")

		if _, _, ok := s4p_writeFindOne(w, r, deps, bizID, id); !ok {
			return
		}

		q := gen.New(deps.Pool)
		ctx := r.Context()
		img, err := q.FindImageByIDAndProduct(ctx, gen.FindImageByIDAndProductParams{
			ID: imageID, ProductID: id,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Product image not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		if err := s4p_inTx(ctx, deps.Pool, func(txq *gen.Queries) error {
			if _, err := txq.LockImageRowsByProduct(ctx, id); err != nil {
				return err
			}
			if err := txq.DeleteImageByID(ctx, imageID); err != nil {
				return err
			}
			return s4p_redensify(ctx, txq, id)
		}); err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		if img.StorageKey.Valid && img.StorageKey.String != "" {
			s4p_deleteAsset(ctx, deps.Storage, img.StorageKey.String)
		}
		s4p_respondFoundProductStatus(w, r, deps, bizID, id, http.StatusOK)
	}
}

// s4p_redensify walks the remaining rows oldest-first; each row only ever
// moves down into a slot the previous iteration just vacated, so the unique
// (product_id, position) index is never tripped.
func s4p_redensify(ctx context.Context, txq *gen.Queries, productID string) error {
	remaining, err := txq.ListImageIDPositions(ctx, productID)
	if err != nil {
		return err
	}
	for index, row := range remaining {
		if row.Position != int32(index) {
			if err := txq.UpdateImagePosition(ctx, gen.UpdateImagePositionParams{
				ID: row.ID, Position: int32(index),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- PUT /products/{id}/images/reorder (T4.2d, T4.14) ----------------------

var s4pUUIDRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type s4p_reorderBody struct {
	ImageIDs []json.RawMessage `json:"imageIds"`
}

// s4p_imageReorder ports reorderImages. The submitted list must be the
// product's complete image set — a partial list cannot produce a dense order.
func s4p_imageReorder(deps ProductsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		id := chi.URLParam(r, "id")

		if _, _, ok := s4p_writeFindOne(w, r, deps, bizID, id); !ok {
			return
		}

		var body s4p_reorderBody
		if !s4p_decodeBody(w, r, &body) {
			return
		}
		imageIDs, issues := s4p_validateReorder(body.ImageIDs)
		if issues != nil {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		q := gen.New(deps.Pool)
		ctx := r.Context()
		current, err := q.ListImageIDPositions(ctx, id)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		currentSet := make(map[string]bool, len(current))
		for _, c := range current {
			currentSet[c.ID] = true
		}
		submittedSet := make(map[string]bool, len(imageIDs))
		for _, sid := range imageIDs {
			submittedSet[sid] = true
		}
		if len(currentSet) != len(submittedSet) || !sameKeys(currentSet, submittedSet) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, fmt.Sprintf(
				"imageIds must list every image of this product exactly once "+
					"(product has %d, received %d).", len(currentSet), len(submittedSet))))
			return
		}

		// Two-phase write inside ONE transaction: park everything above the
		// real range first because the unique index is checked per row. All
		// row locks are taken in id order up front so concurrent reorders
		// serialize (last applied wins) instead of deadlocking.
		if err := s4p_inTx(ctx, deps.Pool, func(txq *gen.Queries) error {
			if _, err := txq.LockImageRowsByProduct(ctx, id); err != nil {
				return err
			}
			if err := txq.ParkImagePositions(ctx, gen.ParkImagePositionsParams{
				ProductID: id, Position: s4pReorderOffset,
			}); err != nil {
				return err
			}
			for index, sid := range imageIDs {
				if err := txq.UpdateImagePosition(ctx, gen.UpdateImagePositionParams{
					ID: sid, Position: int32(index),
				}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		s4p_respondFoundProductStatus(w, r, deps, bizID, id, http.StatusOK)
	}
}

// s4p_validateReorder ports ReorderImagesSchema: array of uuid strings,
// min(1), max(5), refined against duplicates. Issues are emitted zod-v4
// style: base array checks, then element checks, then the refine.
func s4p_validateReorder(raw []json.RawMessage) ([]string, []httpx.FieldIssue) {
	const key = "imageIds"

	if raw == nil {
		return nil, []httpx.FieldIssue{{Code: "invalid_type", Path: key,
			Message: "Invalid input: expected array, received undefined"}}
	}
	n := len(raw)
	if n < 1 {
		return nil, []httpx.FieldIssue{{Code: "too_small", Path: key,
			Message: "Too small: expected array to have >=1 items"}}
	}
	if n > s4pMaxImages {
		return nil, []httpx.FieldIssue{{Code: "too_big", Path: key,
			Message: "Too big: expected array to have <=5 items"}}
	}

	ids := make([]string, n)
	for i, el := range raw {
		var s string
		if s4p_receivedType(el) != "string" || json.Unmarshal(el, &s) != nil || !s4pUUIDRe.MatchString(s) {
			return nil, []httpx.FieldIssue{{
				Code: "invalid_format", Path: fmt.Sprintf("%s.%d", key, i),
				Message: "Invalid UUID",
			}}
		}
		ids[i] = s
	}

	seen := make(map[string]bool, n)
	dup := false
	for _, s := range ids {
		if seen[s] {
			dup = true
			break
		}
		seen[s] = true
	}
	if dup {
		return nil, []httpx.FieldIssue{{Code: "custom", Path: key,
			Message: "imageIds must not contain duplicates"}}
	}
	return ids, nil
}

func sameKeys(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// s4p_inTx scopes fn to one pgx transaction with rollback-on-error semantics
// (the Go analogue of prisma.$transaction(async (tx) => ...)).
func s4p_inTx(ctx context.Context, pool *pgxpool.Pool, fn func(txq *gen.Queries) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(gen.New(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func s4p_respondFoundProductStatus(w http.ResponseWriter, r *http.Request, deps ProductsDeps, bizID, id string, status int) {
	row, imgs, ok := s4p_writeFindOne(w, r, deps, bizID, id)
	if !ok {
		return
	}
	s4p_respondProduct(w, status, row, imgs)
}
