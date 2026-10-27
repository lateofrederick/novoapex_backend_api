-- Device tokens write and read models for Expo push notifications.

-- name: UpsertDeviceToken :one
INSERT INTO device_tokens (
    id, business_id, owner_phone, expo_push_token, platform, device_name, created_at, updated_at, last_used_at
) VALUES (
    $1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
)
ON CONFLICT (expo_push_token) DO UPDATE SET
    business_id  = EXCLUDED.business_id,
    owner_phone  = EXCLUDED.owner_phone,
    platform     = EXCLUDED.platform,
    device_name  = COALESCE(EXCLUDED.device_name, device_tokens.device_name),
    updated_at   = CURRENT_TIMESTAMP,
    last_used_at = CURRENT_TIMESTAMP
RETURNING id, business_id, owner_phone, expo_push_token, platform, device_name, created_at, updated_at, last_used_at;

-- name: DeleteDeviceToken :execrows
DELETE FROM device_tokens
WHERE expo_push_token = $1 AND business_id = $2;

-- name: ListDeviceTokensByBusiness :many
SELECT id, business_id, owner_phone, expo_push_token, platform, device_name, created_at, updated_at, last_used_at
FROM device_tokens
WHERE business_id = $1
ORDER BY last_used_at DESC;

-- name: DeleteDeviceTokensByTokens :execrows
DELETE FROM device_tokens
WHERE expo_push_token = ANY($1::text[]);

-- name: TouchDeviceToken :execrows
UPDATE device_tokens
SET last_used_at = CURRENT_TIMESTAMP
WHERE expo_push_token = $1;
