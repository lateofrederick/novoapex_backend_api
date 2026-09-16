// Package workers
// Follow-up scheduling: the order.created consumer that durably schedules the
// abandoned-cart (+2h) and unpaid-invoice (+24h/+48h) reminders. Idempotent by
// an existence check — the order.created event is at-least-once, so a redelivery
// after a crash must not double-schedule.
package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/events"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// FollowUpScheduleDeps carries the follow-up scheduler's collaborators.
type FollowUpScheduleDeps struct {
	Pool *pgxpool.Pool
}

// RegisterFollowUpSchedule attaches the follow-up scheduling handler.
func RegisterFollowUpSchedule(reg queue.Registrar, deps FollowUpScheduleDeps) {
	reg.Register(queue.TaskFollowUpSchedule, func(ctx context.Context, payload []byte) error {
		var evt events.OrderCreated
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("follow-up schedule: decode order.created: %w", err)
		}
		return HandleFollowUpSchedule(ctx, deps, evt)
	})
}

// HandleFollowUpSchedule schedules the three unpaid-order follow-ups for a
// freshly created order.
func HandleFollowUpSchedule(ctx context.Context, deps FollowUpScheduleDeps, evt events.OrderCreated) error {
	var exists bool
	if err := deps.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM scheduled_follow_ups WHERE order_id = $1)`, evt.OrderID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		slog.Info("Follow-ups already scheduled — skipping", "orderId", evt.OrderID)
		return nil
	}

	q := gen.New(deps.Pool)
	now := time.Now().UTC()

	if err := q.InsertScheduledFollowUpPair(ctx, gen.InsertScheduledFollowUpPairParams{
		ID:          crmNewID(),
		OrderID:     evt.OrderID,
		BusinessID:  evt.BusinessID,
		CustomerID:  evt.CustomerID,
		JobType:     "abandoned-cart",
		ScheduledAt: pgtype.Timestamp{Time: now.Add(2 * time.Hour), Valid: true},

		ID_2:          crmNewID(),
		OrderID_2:     evt.OrderID,
		BusinessID_2:  evt.BusinessID,
		CustomerID_2:  evt.CustomerID,
		JobType_2:     "unpaid-invoice-first",
		ScheduledAt_2: pgtype.Timestamp{Time: now.Add(24 * time.Hour), Valid: true},
	}); err != nil {
		return fmt.Errorf("follow-up schedule: schedule pair: %w", err)
	}

	if err := q.InsertScheduledFollowUp(ctx, gen.InsertScheduledFollowUpParams{
		ID:          crmNewID(),
		OrderID:     evt.OrderID,
		BusinessID:  evt.BusinessID,
		CustomerID:  evt.CustomerID,
		JobType:     "unpaid-invoice-second",
		ScheduledAt: pgtype.Timestamp{Time: now.Add(48 * time.Hour), Valid: true},
	}); err != nil {
		return fmt.Errorf("follow-up schedule: schedule second reminder: %w", err)
	}

	slog.Info("Follow-ups scheduled",
		"orderId", evt.OrderID,
		"count", 3,
		"types", "abandoned-cart,+2h unpaid-invoice-first,+24h unpaid-invoice-second,+48h")
	return nil
}
