// Restock: the order.cancelled consumer that reverses the checkout stock
// decrement. Idempotent via orders.stock_restored_at (claim-then-restore in one
// transaction), so an at-least-once redelivery never double-restocks.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/events"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// RestockDeps carries the restock consumer's collaborators.
type RestockDeps struct {
	Pool *pgxpool.Pool
}

// errStockAlreadyRestored is a non-fatal rejection: this order's stock was
// already restored (an at-least-once redelivery).
var errStockAlreadyRestored = errors.New("restock: order already restocked")

// RegisterRestock attaches the restock handler to the worker registrar.
func RegisterRestock(reg queue.Registrar, deps RestockDeps) {
	reg.Register(queue.TaskRestock, func(ctx context.Context, payload []byte) error {
		var evt events.OrderCancelled
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("restock: decode order.cancelled: %w", err)
		}
		return HandleRestock(ctx, deps, evt)
	})
}

// HandleRestock restores stock for a cancelled order once.
func HandleRestock(ctx context.Context, deps RestockDeps, evt events.OrderCancelled) error {
	txErr := pgx.BeginFunc(ctx, deps.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE orders SET stock_restored_at = CURRENT_TIMESTAMP
			  WHERE id = $1 AND stock_restored_at IS NULL`, evt.OrderID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errStockAlreadyRestored
		}

		// Reverse the checkout decrement, aggregating per product so duplicate
		// line items restore their combined quantity exactly once.
		_, err = tx.Exec(ctx, `
			UPDATE products p
			   SET stock = p.stock + agg.total_qty, updated_at = CURRENT_TIMESTAMP
			  FROM (SELECT product_id, SUM(quantity) AS total_qty
			          FROM order_items
			         WHERE order_id = $1
			         GROUP BY product_id) agg
			 WHERE agg.product_id = p.id`, evt.OrderID)
		return err
	})

	switch {
	case txErr == nil:
		// committed
	case errors.Is(txErr, errStockAlreadyRestored):
		slog.Info("Restock skipped — already restored", "orderId", evt.OrderID)
		return nil
	default:
		return txErr
	}

	slog.Info("Stock restored for cancelled order", "orderId", evt.OrderID)
	return nil
}
