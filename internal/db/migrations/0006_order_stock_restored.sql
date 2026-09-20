-- 0006_order_stock_restored: idempotency marker for the restock consumer.
-- When an order is cancelled, order.cancelled fans out to a restock consumer
-- that reverses the checkout stock decrement. stock_restored_at marks that the
-- reversal happened, so an at-least-once redelivery never double-restocks.
ALTER TABLE orders ADD COLUMN stock_restored_at TIMESTAMP(3);
