-- Physical shop locations CRUD (business_locations table, migration
-- 20260902120000_add_business_locations). Port of
-- apps/mobile-api/src/locations/*.ts (Node). Every query is business-scoped
-- (T0.17 discipline).

-- name: CreateLocation :one
INSERT INTO business_locations
    (id, business_id, name, address, shop_number, landmark, opening_time,
     closing_time, offers_delivery, offers_pickup, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
RETURNING id, business_id, name, address, shop_number, landmark, opening_time,
          closing_time, offers_delivery, offers_pickup, is_active, created_at, updated_at;

-- name: ListLocationsByBusiness :many
SELECT id, business_id, name, address, shop_number, landmark, opening_time,
       closing_time, offers_delivery, offers_pickup, is_active, created_at, updated_at
FROM business_locations
WHERE business_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountLocationsByBusiness :one
SELECT COUNT(*)::bigint AS total FROM business_locations WHERE business_id = $1;

-- name: GetLocationByIDAndBusiness :one
SELECT id, business_id, name, address, shop_number, landmark, opening_time,
       closing_time, offers_delivery, offers_pickup, is_active, created_at, updated_at
FROM business_locations
WHERE id = $1 AND business_id = $2;

-- name: DeleteLocationByIDAndBusiness :execrows
DELETE FROM business_locations WHERE id = $1 AND business_id = $2;
