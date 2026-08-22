-- Stage 4B write models: orders ops (T4.16/T4.17), conversation ops
-- (T4.18-T4.20) and the payout request money path (T4.21).
-- Every query is business-scoped either directly or through a pre-ownership-
-- checked id, mirroring the Node updateMany/findFirst where clauses.

-- Orders --------------------------------------------------------------------

-- OrdersService.updateFulfillment (orders.service.ts:76-87): updateMany
-- {where {id, businessId}, data {status}} — @updatedAt bumps updated_at.
-- name: UpdateOrderFulfillment :execrows
UPDATE orders SET status = $1, updated_at = CURRENT_TIMESTAMP
WHERE id = $2 AND business_id = $3;

-- OrdersService.escalate (orders.service.ts:89-100): when the order carries a
-- conversationId it flips that conversation to the escalated state. NOTE: the
-- source sets ONLY isEscalatedToHuman + state here — escalation_reason stays
-- untouched.
-- name: EscalateConversationByID :execrows
UPDATE conversations
SET is_escalated_to_human = true, state = 'ESCALATED', updated_at = CURRENT_TIMESTAMP
WHERE id = $1;

-- Conversation ops ----------------------------------------------------------

-- ConversationsService.takeover (conversations.service.ts:90-101):
-- updateMany {id, businessId} -> isEscalatedToHuman=true, state='ESCALATED'.
-- name: TakeoverConversation :execrows
UPDATE conversations
SET is_escalated_to_human = true, state = 'ESCALATED', updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND business_id = $2;

-- ConversationsService.release (conversations.service.ts:103-114):
-- updateMany {id, businessId} -> isEscalatedToHuman=false, state='LEAD'.
-- name: ReleaseConversation :execrows
UPDATE conversations
SET is_escalated_to_human = false, state = 'LEAD', updated_at = CURRENT_TIMESTAMP
WHERE id = $1 AND business_id = $2;

-- reply ownership probe + business join (conversations.service.ts:117-124:
-- findFirst {id, businessId} include business).
-- name: GetConversationWithBusiness :one
SELECT c.id, c.business_id, c.customer_phone,
       b.id AS b_id, b.whatsapp_phone_number_id AS b_whatsapp_phone_number_id,
       b.currency AS b_currency
FROM conversations c
JOIN businesses b ON b.id = c.business_id
WHERE c.id = $1 AND c.business_id = $2;

-- WhatsApp 24h window gate (conversation-orchestrator.service.ts:34-41):
-- newest inbound_messages.timestamp for the conversation decides. MAX() over
-- an empty set yields NULL (Valid=false) which maps to "no inbound => closed".
-- name: GetLatestInboundTimestamp :one
SELECT MAX(timestamp)::timestamp AS latest FROM inbound_messages WHERE conversation_id = $1;

-- Persist the outbound row BEFORE enqueueing (orchestrator lines 45-74):
-- blocked sends get raw_payload '{}' and status 'failed_24h_window_closed';
-- allowed sends get the WhatsApp text payload shape and status 'pending'.
-- name: InsertOutboundMessage :one
INSERT INTO outbound_messages
    (id, recipient_phone, message_type, text_content, raw_payload, meta_response,
     status, business_id, conversation_id)
VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8)
RETURNING id, whatsapp_message_id, recipient_phone, message_type, text_content,
          template_name, image_url, product_id, raw_payload, meta_response,
          status, business_id, conversation_id, created_at;

-- Payouts -------------------------------------------------------------------

-- PayoutsService.requestPayout phase 1 (payouts.service.ts:129-133):
-- per-business transaction-scoped advisory lock serialises concurrent
-- requests before the balance read.
-- name: LockPayoutBalanceKey :exec
SELECT pg_advisory_xact_lock(hashtext($1));

-- Business row for currency + cached recipient code
-- (payouts.service.ts:112-118 and :158).
-- name: GetBusinessForPayout :one
SELECT id, currency, paystack_recipient_code FROM businesses WHERE id = $1;

-- Reserve funds as a PENDING payout inside the locked transaction
-- (payouts.service.ts:143-151). reference is unique (payouts_reference_key).
-- name: InsertPendingPayout :one
INSERT INTO payouts
    (id, business_id, amount, currency, reference, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
RETURNING id, business_id, amount, currency, status, reference,
          paystack_transfer_id, created_at, updated_at;

-- Cache the freshly created transfer recipient on the business
-- (payouts.service.ts:176-179).
-- name: SetPaystackRecipientCode :execrows
UPDATE businesses SET paystack_recipient_code = $1, updated_at = CURRENT_TIMESTAMP
WHERE id = $2;

-- Phase 2 completion: store transfer code + final status
-- (payouts.service.ts:192-198).
-- name: CompletePayout :one
UPDATE payouts SET paystack_transfer_id = $1, status = $2, updated_at = CURRENT_TIMESTAMP
WHERE id = $3
RETURNING id, business_id, amount, currency, status, reference,
          paystack_transfer_id, created_at, updated_at;

-- Failure release (payouts.service.ts:201-205): FAILED payouts stop counting
-- against the available balance.
-- name: MarkPayoutFailed :execrows
UPDATE payouts SET status = 'FAILED', updated_at = CURRENT_TIMESTAMP WHERE id = $1;
