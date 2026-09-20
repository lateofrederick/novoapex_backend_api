// Package workers
// Profile stats: the order.created consumer that updates the customer's
// aggregate order counters (totalOrders/totalSpent/averageOrderValue/
// lastOrderAt/orderFrequencyDays). Idempotent via orders.stats_recorded_at, a
// claim marker written in the same transaction as the increment.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/events"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// ProfileStatsDeps carries the profile-stats collaborators.
type ProfileStatsDeps struct {
	Pool *pgxpool.Pool
}

// errStatsAlreadyRecorded is a non-fatal rejection: this order's stats were
// already counted (an at-least-once redelivery).
var errStatsAlreadyRecorded = errors.New("profile-stats: order already counted")

// RegisterProfileStats attaches the profile-stats handler to the worker registrar.
func RegisterProfileStats(reg queue.Registrar, deps ProfileStatsDeps) {
	reg.Register(queue.TaskProfileStats, func(ctx context.Context, payload []byte) error {
		var evt events.OrderCreated
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("profile-stats: decode order.created: %w", err)
		}
		return HandleProfileStats(ctx, deps, evt)
	})
}

// HandleProfileStats increments the customer's order counters once per order.
func HandleProfileStats(ctx context.Context, deps ProfileStatsDeps, evt events.OrderCreated) error {
	total, err := decimal.NewFromString(evt.TotalAmount)
	if err != nil {
		return fmt.Errorf("profile-stats: parse total %q: %w", evt.TotalAmount, err)
	}

	txErr := pgx.BeginFunc(ctx, deps.Pool, func(tx pgx.Tx) error {
		// Claim the marker (first write wins) so the increment below is
		// atomic with it: a crash rolls both back together.
		tag, err := tx.Exec(ctx,
			`UPDATE orders SET stats_recorded_at = CURRENT_TIMESTAMP
			  WHERE id = $1 AND stats_recorded_at IS NULL`, evt.OrderID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errStatsAlreadyRecorded
		}

		tq := gen.New(tx)
		profile, err := tq.GetCustomerProfileByCustomerID(ctx, evt.CustomerID)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil // profile missing: nothing to count
		case err != nil:
			return err
		}

		newTotalOrders := profile.TotalOrders + 1
		newTotalSpent := profile.TotalSpent.Add(total)
		newAverageOrderValue := newTotalSpent.Div(decimal.NewFromInt(int64(newTotalOrders)))

		var orderFrequencyDays pgtype.Float8
		if newTotalOrders > 1 {
			firstContactAt, cerr := tq.GetCustomerFirstContactAt(ctx, evt.CustomerID)
			switch {
			case errors.Is(cerr, pgx.ErrNoRows):
				// leave NULL
			case cerr != nil:
				return cerr
			default:
				if firstContactAt.Valid {
					daysSinceFirstContact := time.Since(firstContactAt.Time).Hours() / 24
					orderFrequencyDays = pgtype.Float8{
						Float64: daysSinceFirstContact / float64(newTotalOrders-1),
						Valid:   true,
					}
				}
			}
		}

		_, uerr := tq.UpdateProfileOrderStats(ctx, gen.UpdateProfileOrderStatsParams{
			TotalOrders:        newTotalOrders,
			TotalSpent:         newTotalSpent,
			AverageOrderValue:  decimalToNumeric(newAverageOrderValue),
			OrderFrequencyDays: orderFrequencyDays,
			CustomerID:         evt.CustomerID,
		})
		return uerr
	})

	switch {
	case txErr == nil:
		// committed
	case errors.Is(txErr, errStatsAlreadyRecorded):
		slog.Info("Profile stats already recorded — skipping", "orderId", evt.OrderID)
		return nil
	default:
		return txErr
	}

	slog.Info("Customer profile stats updated",
		"customerId", evt.CustomerID,
		"orderId", evt.OrderID,
		"orderAmount", total.StringFixed(2))
	return nil
}

// decimalToNumeric converts shopspring decimals into pgtype.Numeric so exact
// values reach NUMERIC(14,2) columns without a float64 hop.
func decimalToNumeric(d decimal.Decimal) pgtype.Numeric {
	return pgtype.Numeric{
		Int:   d.Coefficient(),
		Exp:   d.Exponent(),
		Valid: true,
	}
}
