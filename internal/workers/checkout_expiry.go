// Checkout expiry: a cron sweep that cancels orders whose final unpaid-invoice
// reminder fired without payment. Cancellation reuses cancelOrder (order
// CANCELLED + conversation INVOICING->CANCELLED + order.cancelled event), which
// then fans out to the restock consumer.
package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// checkoutExpiryGrace is how long after the final (unpaid-invoice-second)
// reminder an order is left before it is cancelled automatically.
const checkoutExpiryGrace = 24 * time.Hour

// SweepCheckoutExpiry cancels overdue unpaid orders: status CONFIRMED or
// PAYMENT_PENDING whose unpaid-invoice-second reminder fired more than
// checkoutExpiryGrace ago. The advisory lock makes the scan a singleton across
// replicas; cancelOrder itself is idempotent.
func SweepCheckoutExpiry(ctx context.Context, deps Deps) error {
	pool := deps.Pool
	if pool == nil {
		return errors.New("checkout-expiry: nil pool")
	}

	cutoff := time.Now().UTC().Add(-checkoutExpiryGrace)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("checkout-expiry: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('checkout-expiry'))`); err != nil {
		return fmt.Errorf("checkout-expiry: advisory lock: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT o.id
		  FROM orders o
		  JOIN scheduled_follow_ups sfu
		    ON sfu.order_id = o.id AND sfu.job_type = 'unpaid-invoice-second'
		 WHERE o.status IN ('CONFIRMED', 'PAYMENT_PENDING')
		   AND sfu.executed_at IS NOT NULL
		   AND sfu.executed_at <= $1
		 ORDER BY o.created_at
		 LIMIT 100`, cutoff)
	if err != nil {
		return fmt.Errorf("checkout-expiry: expired query: %w", err)
	}
	var orderIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("checkout-expiry: scan expired: %w", err)
		}
		orderIDs = append(orderIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("checkout-expiry: iterate expired: %w", err)
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("checkout-expiry: commit scan: %w", err)
	}

	cancelled := 0
	for _, orderID := range orderIDs {
		if err := cancelOrder(ctx, pool, orderID, "checkout_expired", time.Now().UTC()); err != nil {
			slog.Error("checkout-expiry: cancel order failed",
				"orderId", orderID, "err", err)
			continue
		}
		cancelled++
	}

	slog.Info("Checkout expiry sweep complete",
		"expired", len(orderIDs), "cancelled", cancelled)
	return nil
}
