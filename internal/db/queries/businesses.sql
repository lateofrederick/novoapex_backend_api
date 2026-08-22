-- name: GetBusinessByID :one
SELECT * FROM businesses WHERE id = $1;

-- name: ListBusinessesByOwnerPhone :one
SELECT * FROM businesses WHERE owner_phone = $1 LIMIT 1;
