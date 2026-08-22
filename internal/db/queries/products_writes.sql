-- Stage 4A vendor write models: businesses create + product/image mutations.
-- SOURCE: apps/mobile-api/src/businesses/businesses.service.ts,
--         apps/mobile-api/src/products/products.service.ts.
-- Vector columns are never selected (Prisma Unsupported("vector(768)") —
-- payloads carry no embedding key, matching the Stage 2 read contract).

-- BusinessesService.create: INSERT with caller-supplied owner_phone;
-- businesses_owner_phone_key is the 409 backstop for the pre-check race.
-- name: CreateBusiness :one
INSERT INTO businesses (id, name, whatsapp_phone_number_id, owner_phone, currency, category, location, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
RETURNING *;

-- ProductsService.create: spread dto + businessId, include images(position asc).
-- Optional dto fields arrive as nil pgtype.Text; DB defaults fill the rest.
-- name: CreateProduct :one
INSERT INTO products (id, business_id, name, description, price, stock, sku, is_available, category, stock_note, delivery_note, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP)
RETURNING id, business_id, name, description, category, request_count, stock_note, delivery_note, price, stock, sku, is_available, created_at, updated_at;

-- ProductsService.remove: rows cascade from products; scoped by business so a
-- stale ownership window cannot cross tenants.
-- name: DeleteProductByIDAndBusiness :exec
DELETE FROM products WHERE id = $1 AND business_id = $2;

-- Upload transaction aggregate (products.service.ts uploadImages): count +
-- MAX(position) answered about the same rows inside one serialised tx.
-- name: AggregateProductImages :one
SELECT COUNT(*)::bigint AS total, COALESCE(MAX(position), -1)::int AS max_position
FROM product_images WHERE product_id = $1;

-- Pre-upload dedup probe: which of this request's hashes does the product
-- already hold? NULL legacy hashes never match (NULL = ANY(...) is NULL).
-- name: FindStoredImageHashes :many
SELECT content_hash FROM product_images
WHERE product_id = $1 AND content_hash = ANY($2::text[]);

-- One row per fresh upload; ids generated app-side (randomUUID in source).
-- name: InsertProductImage :exec
INSERT INTO product_images (id, product_id, url, storage_key, content_hash, position)
VALUES ($1, $2, $3, $4, $5, $6);

-- removeImage scope guard: a valid image id under another product must 404.
-- name: FindImageByIDAndProduct :one
SELECT id, url, storage_key, content_hash FROM product_images WHERE id = $1 AND product_id = $2;

-- name: DeleteImageByID :exec
DELETE FROM product_images WHERE id = $1;

-- Re-densification + reorder reads: remaining rows oldest-position first.
-- name: ListImageIDPositions :many
SELECT id, position FROM product_images WHERE product_id = $1 ORDER BY position ASC;

-- Deterministic lock acquisition for reorder/remove transactions: taking all
-- of a product's image row locks in id order up front makes concurrent
-- reorders serialize instead of deadlocking mid two-phase write.
-- name: LockImageRowsByProduct :many
SELECT id FROM product_images WHERE product_id = $1 ORDER BY id FOR UPDATE;

-- Ascending slot-fill keeps the unique (product_id, position) index satisfied.
-- name: UpdateImagePosition :exec
UPDATE product_images SET position = $2 WHERE id = $1;

-- Reorder phase one: park every row of the product above the real range
-- (REORDER_OFFSET = 1000) so finals 0..n never collide mid-flight.
-- name: ParkImagePositions :exec
UPDATE product_images SET position = position + $2 WHERE product_id = $1;
