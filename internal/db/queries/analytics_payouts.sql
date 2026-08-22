-- Dashboard / analytics / payouts read models (Stage 2).
-- Every query filters by business_id (T0.17 discipline).
-- Money columns stay numeric so the sqlc decimal override keeps them exact;
-- handlers wrap them in internal/money.Number for the JSON-number contract.

-- Dashboard summary (analytics.service getDashboardSummary) -----------------

-- name: CountPaymentsByStatus :one
SELECT COUNT(*)::bigint AS total FROM payments WHERE business_id = $1 AND status = $2;

-- name: SumPaymentAmountsByStatus :one
SELECT COALESCE(SUM(amount), 0)::numeric AS total FROM payments WHERE business_id = $1 AND status = $2;

-- Analytics overview (analytics.service getOverview) ------------------------

-- rescuedLeads: PAID orders whose conversation was escalated to a human.
-- name: CountRescuedPaidOrders :one
SELECT COUNT(*)::bigint AS total
FROM orders o
JOIN conversations v ON v.id = o.conversation_id
WHERE o.business_id = $1 AND o.status = 'PAID' AND v.is_escalated_to_human = true;

-- Handoff reasons (analytics.service getHandoffReasons) ---------------------

-- groupBy escalationReason where not null, order count desc. A reason ASC
-- tiebreak is added for deterministic output; Node leaves tie order undefined.
-- name: CountConversationsByEscalationReason :many
SELECT escalation_reason AS reason, COUNT(*)::bigint AS total
FROM conversations
WHERE business_id = $1 AND escalation_reason IS NOT NULL
GROUP BY escalation_reason
ORDER BY total DESC, reason ASC;

-- Demand (analytics.service getDemand): top 5 by request_count. created_at/id
-- tiebreak added for determinism for the same reason as above.
-- name: TopDemandProducts :many
SELECT id, name, request_count
FROM products
WHERE business_id = $1
ORDER BY request_count DESC, created_at DESC, id ASC
LIMIT 5;

-- Payouts + revenue (payouts.service getBalance, RevenueService) ------------

-- getTotalRevenue: SUCCESS payment amounts only.
-- name: SumSuccessfulPaymentAmounts :one
SELECT COALESCE(SUM(amount), 0)::numeric AS total_revenue FROM payments WHERE business_id = $1 AND status = 'SUCCESS';

-- getTotalRevenue with an inclusive paidAt window.
-- name: SumSuccessfulPaymentAmountsBetween :one
SELECT COALESCE(SUM(amount), 0)::numeric AS total_revenue FROM payments
WHERE business_id = $1 AND status = 'SUCCESS' AND paid_at >= $2 AND paid_at <= $3;

-- getPipelineValue: order totals still awaiting money.
-- name: SumPipelineOrderTotals :one
SELECT COALESCE(SUM(total_amount), 0)::numeric AS pipeline_value FROM orders
WHERE business_id = $1 AND status IN ('CONFIRMED', 'PAYMENT_PENDING');

-- getBalance committed side: payouts that reserve funds (SUCCESS or PENDING).
-- name: SumCommittedPayoutAmounts :one
SELECT COALESCE(SUM(amount), 0)::numeric AS committed FROM payouts
WHERE business_id = $1 AND status IN ('SUCCESS', 'PENDING');

-- name: GetBusinessCurrencyByID :one
SELECT currency FROM businesses WHERE id = $1;

-- getRevenueByPeriod: date_trunc windows over successful payments, newest first.
-- granularity is passed as text ('day'|'week'|'month') mirroring the Node
-- raw-query string assembly.
-- name: RevenueByPeriodAllTime :many
SELECT date_trunc(sqlc.arg('granularity')::text, paid_at)::timestamp AS period,
       COALESCE(SUM(amount), 0)::numeric AS total_revenue,
       COUNT(*)::bigint AS transaction_count
FROM payments
WHERE business_id = $1 AND status = 'SUCCESS'
GROUP BY period
ORDER BY period DESC;

-- name: RevenueByPeriodBetween :many
SELECT date_trunc(sqlc.arg('granularity')::text, paid_at)::timestamp AS period,
       COALESCE(SUM(amount), 0)::numeric AS total_revenue,
       COUNT(*)::bigint AS transaction_count
FROM payments
WHERE business_id = $1 AND status = 'SUCCESS' AND paid_at >= $2 AND paid_at <= $3
GROUP BY period
ORDER BY period DESC;

-- getTopProducts: quantity/revenue per product across non-cancelled orders.
-- name: TopProductsByRevenue :many
SELECT oi.product_id,
       oi.product_name,
       SUM(oi.quantity)::bigint AS total_quantity,
       SUM(oi.quantity * oi.unit_price)::numeric AS total_revenue
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE o.business_id = $1 AND o.status NOT IN ('CANCELLED')
GROUP BY oi.product_id, oi.product_name
ORDER BY total_revenue DESC
LIMIT $2;

-- getPaymentsByStatus: full status breakdown, count desc (count then status
-- tiebreak for determinism; Node leaves tie order undefined).
-- name: PaymentTotalsByStatus :many
SELECT status, COUNT(*)::bigint AS total, COALESCE(SUM(amount), 0)::numeric AS total_amount
FROM payments
WHERE business_id = $1
GROUP BY status
ORDER BY total DESC, status::text ASC;

-- Payout history (payouts.service getHistory) -------------------------------

-- name: ListPayoutsByBusiness :many
SELECT id, business_id, amount, currency, status, reference, paystack_transfer_id, created_at, updated_at
FROM payouts
WHERE business_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountPayoutsByBusiness :one
SELECT COUNT(*)::bigint AS total FROM payouts WHERE business_id = $1;
