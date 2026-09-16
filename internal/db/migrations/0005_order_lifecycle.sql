-- 0005_order_lifecycle: columns the order.created consumers need for
-- idempotency and observability.
--
-- payment_url: the generated checkout link (NULL until payment-init runs);
-- doubles as payment-init's idempotency marker.
-- stats_recorded_at: marks that profile-stats has counted this order, so an
-- at-least-once redelivery never double-increments total_orders/total_spent.
ALTER TABLE orders ADD COLUMN payment_url TEXT;
ALTER TABLE orders ADD COLUMN stats_recorded_at TIMESTAMP(3);
