-- Stage 7B worker models (T7.9–T7.22): the CRM materialiser's durable reads
-- and writes. SOURCE: libs/queue/src/handlers/{order-ledger,profile-builder,
-- customer-capture}.handler.ts + libs/orchestrator/src/conversation-state.service.ts.
--
-- Multi-row payloads (order items, per-product stock decrements) travel as a
-- single ::jsonb array parameter unpacked by jsonb_to_recordset so every
-- statement here is fixed-shape, fully parameterized SQL — LLM-derived values
-- can never widen a pattern or inject a clause (T7.15).

-- Order ledger ---------------------------------------------------------------

-- resolveProduct (order-ledger.handler.ts:377-401): short-ID prefix lookup
-- scoped to the business and gated on is_available. Callers must already have
-- validated the short ID against ^[0-9a-f]{8}$ and escaped LIKE wildcards;
-- this stays a bound-parameter LIKE as defence in depth.
-- name: ResolveProductByShortID :one
SELECT id, name, price, stock
FROM products
WHERE business_id = $1
  AND is_available = true
  AND id LIKE $2
LIMIT 1;

-- handleOrderConfirmation business probe (order-ledger.handler.ts:128-136):
-- currency stamps the order; whatsappPhoneNumberId/paymentCallbackUrl gate the
-- invoice message. A missing row is not fatal for order creation.
-- name: GetBusinessForOrder :one
SELECT currency, whatsapp_phone_number_id, name, payment_callback_url
FROM businesses
WHERE id = $1;

-- order.create (order-ledger.handler.ts:144-154): status CONFIRMED,
-- idempotencyKey = sourceMessageId (unique index orders_idempotency_key_key is
-- the duplicate-job backstop), tenant currency stamped on the row.
-- fulfillment_type/location_id added for the delivery-vs-pickup port
-- (order-ledger.handler.ts resolveFulfillment): location_id nil for DELIVERY
-- or an unresolved PICKUP short id (never blocks order creation).
-- name: InsertOrder :one
INSERT INTO orders
    (id, business_id, customer_id, conversation_id, idempotency_key, status, total_amount, currency, fulfillment_type, location_id, updated_at)
VALUES ($1, $2, $3, $4, $5, 'CONFIRMED', $6, $7, $8, $9, CURRENT_TIMESTAMP)
RETURNING id;

-- resolveFulfillment's pickup-location lookup (order-ledger.handler.ts
-- resolvePickupLocation): short-ID prefix match, scoped to the business, and
-- gated on offers_pickup + is_active exactly like the Node port. Callers must
-- already have validated the short ID against ^[0-9a-f]{8}$.
-- name: ResolvePickupLocationByShortID :one
SELECT id FROM business_locations
WHERE business_id = $1
  AND offers_pickup = true
  AND is_active = true
  AND id LIKE $2
LIMIT 1;

-- orderItem.createMany (order-ledger.handler.ts:157-165): one ledger row per
-- detected entry — duplicates for the same product are kept verbatim (each
-- carries its own price snapshot). Prices travel as exact decimal STRINGS and
-- are cast SQL-side; they never pass through float64 JSON numbers.
-- name: InsertOrderItems :exec
INSERT INTO order_items (id, order_id, product_id, product_name, quantity, unit_price)
SELECT v.id, v.order_id, v.product_id, v.product_name, v.quantity, v.unit_price::numeric
FROM jsonb_to_recordset(sqlc.arg('items')::jsonb)
     AS v(id text, order_id text, product_id text, product_name text, quantity int, unit_price text);

-- THE atomic conditional decrement (order-ledger.handler.ts:194-200, T7.15):
-- quantities are aggregated per product caller-side, then one statement
-- decrements only rows that actually hold enough stock. Fewer affected rows
-- than entries means something was short -> caller rolls the whole transaction
-- back instead of overselling.
-- name: DecrementStockIfAvailable :execrows
UPDATE products AS p
SET stock = p.stock - v.qty
FROM jsonb_to_recordset(sqlc.arg('entries')::jsonb) AS v(id text, qty int)
WHERE p.id = v.id
  AND p.stock >= v.qty;

-- Conversation state machine --------------------------------------------------

-- transition() pre-read (order-ledger.handler.ts:304-307): current state
-- decides whether an INVOICING move may even be attempted.
-- name: GetConversationStateByID :one
SELECT state FROM conversations WHERE id = $1;

-- Profile builder / customer capture ------------------------------------------

-- profile-builder findUnique + updateProfileStats read reuse the Stage 3
-- GetCustomerProfileByCustomerID query from customers_products.sql.

-- profile-builder defensive create (profile-builder.handler.ts:36-43): bare
-- profile when missing; ON CONFLICT keeps two racing turns from erroring.
-- name: CreateCustomerProfileIfMissing :execrows
INSERT INTO customer_profiles (id, customer_id, updated_at)
VALUES ($1, $2, CURRENT_TIMESTAMP)
ON CONFLICT (customer_id) DO NOTHING;

-- Preference accumulation write (profile-builder.handler.ts:59-60): merged
-- (deduplicated, capped) array is computed caller-side.
-- name: UpdateProfilePreferences :execrows
UPDATE customer_profiles SET preferences = $1, updated_at = CURRENT_TIMESTAMP
WHERE customer_id = $2;

-- Delivery area latest-wins (profile-builder.handler.ts:71-73).
-- name: UpdateProfileDeliveryArea :execrows
UPDATE customer_profiles SET delivery_area = $1, updated_at = CURRENT_TIMESTAMP
WHERE customer_id = $2;

-- Sentiment change-only write (profile-builder.handler.ts:77-79).
-- name: UpdateProfileSentiment :execrows
UPDATE customer_profiles SET sentiment = $1, updated_at = CURRENT_TIMESTAMP
WHERE customer_id = $2;

-- updateProfileStats (order-ledger.handler.ts:431-439): totals recomputed
-- caller-side with exact decimal math; frequency NULL until the second order.
-- name: UpdateProfileOrderStats :execrows
UPDATE customer_profiles
SET total_orders = $1,
    total_spent = $2,
    average_order_value = $3,
    last_order_at = CURRENT_TIMESTAMP,
    order_frequency_days = $4,
    updated_at = CURRENT_TIMESTAMP
WHERE customer_id = $5;

-- updateProfileStats first-contact read (order-ledger.handler.ts:419-422).
-- name: GetCustomerFirstContactAt :one
SELECT first_contact_at FROM customers WHERE id = $1;

-- customer-capture.updateCustomerName (customer-capture.handler.ts:32-40).
-- name: GetCustomerNameByID :one
SELECT name FROM customers WHERE id = $1;

-- Longest-wins decision made caller-side (customer-capture.handler.ts:42-48).
-- name: UpdateCustomerName :execrows
UPDATE customers SET name = $1, updated_at = CURRENT_TIMESTAMP
WHERE id = $2;

-- Follow-up scheduling ---------------------------------------------------------

-- ScheduledFollowUp.createMany (order-ledger.handler.ts:345-362): both rows
-- land durably in one statement; offsets (+2h/+24h) are caller-supplied so
-- tests can pin them exactly.
-- name: InsertScheduledFollowUpPair :exec
INSERT INTO scheduled_follow_ups (id, order_id, business_id, customer_id, job_type, scheduled_at)
VALUES ($1, $2, $3, $4, $5, $6),
       ($7, $8, $9, $10, $11, $12);

-- Singular follow-up scheduling, additive to the pair above: the +48h
-- unpaid-invoice-second reminder (order-ledger.handler.ts) and the
-- delivery-confirmation reminder scheduled from orders_write.go on the
-- DELIVERED transition. Kept as its own query rather than widening the pair
-- above so the already-tested 2-row statement is untouched.
-- name: InsertScheduledFollowUp :exec
INSERT INTO scheduled_follow_ups (id, order_id, business_id, customer_id, job_type, scheduled_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- Payment-behaviour tracking (followup.go / payment_events.go) -----------------
--
-- Both files are raw-SQL throughout (no sqlc usage) — the late-payment-count
-- increment and preferred-payment-network write live inline there as plain
-- pool.Exec calls, matching each file's own established convention, rather
-- than as sqlc queries here.

-- Marketing opt-in ------------------------------------------------------------

-- customer-capture.handler.ts updateMarketingOptIn: records explicit
-- consent/decline from the wants_updates CRM signal.
-- name: UpdateCustomerMarketingOptIn :execrows
UPDATE customers SET marketing_opt_in = $1, updated_at = CURRENT_TIMESTAMP
WHERE id = $2;
