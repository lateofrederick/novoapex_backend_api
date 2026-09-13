-- Orders + inbox (conversations/messages) read models (Stage 2).
-- Every query filters by business_id (T0.17 discipline); messages are scoped
-- through a pre-ownership-checked conversation id exactly like the Node
-- service (conversations.service.ts verifies {id, businessId} first).
-- raw_payload/meta_response stay jsonb -> []byte; no vector columns here.

-- Orders -------------------------------------------------------------------

-- orders.service findAll: include customer:true, location:true (delivery-vs-
-- pickup port, this session's Node work), orderBy createdAt desc.
-- name: ListOrdersWithCustomer :many
SELECT o.id, o.business_id, o.customer_id, o.conversation_id, o.idempotency_key,
       o.status, o.total_amount, o.currency, o.fulfillment_type, o.location_id,
       o.created_at, o.updated_at,
       c.id AS c_id, c.business_id AS c_business_id, c.phone AS c_phone, c.name AS c_name,
       c.acquisition_channel AS c_acquisition_channel,
       c.marketing_opt_in AS c_marketing_opt_in,
       c.first_contact_at AS c_first_contact_at,
       c.last_contact_at AS c_last_contact_at,
       c.created_at AS c_created_at, c.updated_at AS c_updated_at,
       l.id AS l_id, l.business_id AS l_business_id, l.name AS l_name, l.address AS l_address,
       l.shop_number AS l_shop_number, l.landmark AS l_landmark, l.opening_time AS l_opening_time,
       l.closing_time AS l_closing_time, l.offers_delivery AS l_offers_delivery,
       l.offers_pickup AS l_offers_pickup, l.is_active AS l_is_active,
       l.created_at AS l_created_at, l.updated_at AS l_updated_at
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
LEFT JOIN business_locations l ON l.id = o.location_id
WHERE o.business_id = $1
ORDER BY o.created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountOrdersByBusiness :one
SELECT COUNT(*)::bigint AS total FROM orders WHERE business_id = $1;

-- name: CountOrdersByStatus :one
SELECT COUNT(*)::bigint AS total FROM orders WHERE business_id = $1 AND status = $2;

-- orders summary totalRevenue: aggregate _sum.totalAmount where status PAID.
-- name: SumOrderTotalsWherePaid :one
SELECT COALESCE(SUM(total_amount), 0)::numeric AS total_revenue FROM orders WHERE business_id = $1 AND status = 'PAID';

-- findOne: include customer + items.product + location. The order row is
-- already business-scoped; its items join out from that verified order id.
-- name: GetOrderByIDAndBusiness :one
SELECT id, business_id, customer_id, conversation_id, idempotency_key, status, total_amount, currency, fulfillment_type, location_id, created_at, updated_at
FROM orders
WHERE id = $1 AND business_id = $2;

-- location for GetOrderByIDAndBusiness's response (mirrors the ordinary
-- locations findOne query — orders_write.go's own lookups stay narrower).
-- name: GetLocationByID :one
SELECT id, business_id, name, address, shop_number, landmark, opening_time,
       closing_time, offers_delivery, offers_pickup, is_active, created_at, updated_at
FROM business_locations
WHERE id = $1;

-- name: ListOrderItemsWithProduct :many
SELECT oi.id, oi.order_id, oi.product_id, oi.product_name, oi.quantity, oi.unit_price,
       p.business_id AS p_business_id, p.name AS p_name, p.description AS p_description,
       p.category AS p_category, p.request_count AS p_request_count,
       p.stock_note AS p_stock_note, p.delivery_note AS p_delivery_note,
       p.price AS p_price, p.stock AS p_stock, p.sku AS p_sku,
       p.is_available AS p_is_available, p.created_at AS p_created_at,
       p.updated_at AS p_updated_at
FROM order_items oi
JOIN products p ON p.id = oi.product_id
WHERE oi.order_id = $1
ORDER BY oi.id ASC;

-- Inbox --------------------------------------------------------------------

-- conversations.service getSummary/getHandoffs.
-- name: CountConversationsByBusiness :one
SELECT COUNT(*)::bigint AS total FROM conversations WHERE business_id = $1;

-- name: CountEscalatedConversations :one
SELECT COUNT(*)::bigint AS total FROM conversations WHERE business_id = $1 AND is_escalated_to_human = true;

-- getHandoffs: include customer:true (nullable customerId -> customer:null),
-- orderBy updatedAt desc.
-- name: ListEscalatedConversationsWithCustomer :many
SELECT v.id, v.business_id, v.customer_id, v.customer_phone, v.state,
       v.is_escalated_to_human, v.language, v.escalation_reason, v.created_at, v.updated_at,
       c.id AS c_id, c.business_id AS c_business_id, c.phone AS c_phone, c.name AS c_name,
       c.acquisition_channel AS c_acquisition_channel,
       c.marketing_opt_in AS c_marketing_opt_in,
       c.first_contact_at AS c_first_contact_at,
       c.last_contact_at AS c_last_contact_at,
       c.created_at AS c_created_at, c.updated_at AS c_updated_at
FROM conversations v
LEFT JOIN customers c ON c.id = v.customer_id
WHERE v.business_id = $1 AND v.is_escalated_to_human = true
ORDER BY v.updated_at DESC
LIMIT $2 OFFSET $3;

-- getMessages: ownership probe then per-side recent windows.
-- name: GetConversationIDForBusiness :one
SELECT id FROM conversations WHERE id = $1 AND business_id = $2;

-- name: ListRecentInboundMessages :many
SELECT id, whatsapp_message_id, sender_phone, recipient_phone, message_type, text_content,
       raw_payload, timestamp, business_id, conversation_id, created_at
FROM inbound_messages
WHERE conversation_id = $1
ORDER BY timestamp DESC
LIMIT $2;

-- name: ListRecentOutboundMessages :many
SELECT id, whatsapp_message_id, recipient_phone, message_type, text_content, template_name,
       image_url, product_id, raw_payload, meta_response, status, business_id, conversation_id, created_at
FROM outbound_messages
WHERE conversation_id = $1
ORDER BY created_at DESC
LIMIT $2;
