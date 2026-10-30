-- -----------------------------------------------------------------------------
-- Order Intents (Ephemeral Cart / Checkout Staging)
-- -----------------------------------------------------------------------------

-- name: GetActiveOrderIntent :one
SELECT id, business_id, customer_id, conversation_id, status, total_amount, currency,
       fulfillment_type, location_id, delivery_address, items, order_id, source_message_id,
       expires_at, created_at, updated_at
FROM order_intents
WHERE conversation_id = $1
  AND status = 'AWAITING_CONFIRMATION'
  AND expires_at > CURRENT_TIMESTAMP;

-- name: StageOrderIntent :one
INSERT INTO order_intents (
    id, business_id, customer_id, conversation_id, status,
    total_amount, currency, fulfillment_type, location_id,
    delivery_address, items, source_message_id, expires_at, updated_at
) VALUES (
    $1, $2, $3, $4, 'AWAITING_CONFIRMATION',
    $5, $6, $7, $8,
    $9, $10, $11, $12, CURRENT_TIMESTAMP
)
ON CONFLICT (conversation_id) WHERE status = 'AWAITING_CONFIRMATION' DO UPDATE SET
    total_amount = EXCLUDED.total_amount,
    currency = EXCLUDED.currency,
    fulfillment_type = EXCLUDED.fulfillment_type,
    location_id = EXCLUDED.location_id,
    delivery_address = EXCLUDED.delivery_address,
    items = EXCLUDED.items,
    source_message_id = EXCLUDED.source_message_id,
    expires_at = EXCLUDED.expires_at,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, business_id, customer_id, conversation_id, status, total_amount, currency,
          fulfillment_type, location_id, delivery_address, items, order_id, source_message_id,
          expires_at, created_at, updated_at;

-- name: ConvertOrderIntent :execrows
UPDATE order_intents
SET status = 'CONVERTED',
    order_id = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
  AND status IN ('AWAITING_CONFIRMATION', 'CONFIRMED');

-- name: CancelActiveOrderIntent :execrows
UPDATE order_intents
SET status = 'CANCELLED',
    updated_at = CURRENT_TIMESTAMP
WHERE conversation_id = $1
  AND status = 'AWAITING_CONFIRMATION';

-- name: ExpireOldOrderIntents :execrows
UPDATE order_intents
SET status = 'EXPIRED',
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'AWAITING_CONFIRMATION'
  AND expires_at <= CURRENT_TIMESTAMP;
