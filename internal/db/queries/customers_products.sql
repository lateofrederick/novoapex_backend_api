-- Customers + products read models (Stage 2).
-- Every query filters by business_id (T0.17 discipline).
-- Vector columns are deliberately NOT selected: Prisma marks embedding as
-- Unsupported("vector(768)") so the generated payload types omit the field
-- entirely — Node responses carry no embedding key and neither must ours.

-- name: ListCustomersByBusiness :many
SELECT id, business_id, phone, name, acquisition_channel, first_contact_at, last_contact_at, created_at, updated_at
FROM customers
WHERE business_id = $1
ORDER BY last_contact_at DESC
LIMIT $2 OFFSET $3;

-- name: CountCustomersByBusiness :one
SELECT COUNT(*)::bigint AS total FROM customers WHERE business_id = $1;

-- name: CountCustomersCreatedSince :one
SELECT COUNT(*)::bigint AS total FROM customers
WHERE business_id = $1 AND created_at >= $2;

-- name: GetCustomerByIDAndBusiness :one
SELECT id, business_id, phone, name, acquisition_channel, first_contact_at, last_contact_at, created_at, updated_at
FROM customers
WHERE id = $1 AND business_id = $2;

-- name: GetCustomerProfileByCustomerID :one
SELECT id, customer_id, preferences, delivery_area, average_order_value, order_frequency_days, last_order_at, last_reengagement_at, total_orders, total_spent, sentiment, updated_at
FROM customer_profiles
WHERE customer_id = $1;

-- Products -----------------------------------------------------------------

-- name: ListProductsByBusiness :many
SELECT id, business_id, name, description, category, request_count, stock_note, delivery_note, price, stock, sku, is_available, created_at, updated_at
FROM products
WHERE business_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountProductsByBusiness :one
SELECT COUNT(*)::bigint AS total FROM products WHERE business_id = $1;

-- name: GetProductByIDAndBusiness :one
SELECT id, business_id, name, description, category, request_count, stock_note, delivery_note, price, stock, sku, is_available, created_at, updated_at
FROM products
WHERE id = $1 AND business_id = $2;

-- name: ListProductImagesForProducts :many
SELECT id, product_id, url, storage_key, content_hash, position, created_at
FROM product_images
WHERE product_id = ANY($1::text[])
ORDER BY product_id ASC, position ASC;

-- products.service.ts getMetrics: count / count stock<=0 / sum stock /
-- raw SUM(price*stock) returning Number(...) or 0.
-- name: CountProductsOutOfStock :one
SELECT COUNT(*)::bigint AS total FROM products WHERE business_id = $1 AND stock <= 0;

-- name: SumProductStockUnits :one
SELECT COALESCE(SUM(stock), 0)::bigint AS total_units FROM products WHERE business_id = $1;

-- name: SumInventoryValue :one
SELECT COALESCE(SUM(price * stock), 0)::numeric AS total_value FROM products WHERE business_id = $1;
