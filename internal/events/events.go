// Package events defines the domain-event contract for the transactional
// outbox (the domain_events table) that coordinates the checkout/CRM split.
//
// Producers insert an event in the SAME transaction as the state change it
// describes, so an event can never exist without its state change (and vice
// versa). A dispatcher later publishes PENDING rows to queue subscribers;
// consumers are at-least-once and must dedupe on their own natural keys.
package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Event types emitted by the checkout pipeline.
const (
	TypeOrderCreated   = "order.created"
	TypeOrderCancelled = "order.cancelled"
)

// AggregateTypeOrder identifies the aggregate (the order) for order lifecycle
// events. Consumers key their idempotency on AggregateID + Type.
const AggregateTypeOrder = "order"

// ChannelName is the Postgres NOTIFY channel producers signal on commit, so the
// dispatcher wakes immediately instead of waiting for the poll backstop.
const ChannelName = "domain_event"

// OrderCreated is the self-contained payload of an order.created event, so
// subscribers never have to query the producer for what they need.
type OrderCreated struct {
	OrderID        string `json:"orderId"`
	BusinessID     string `json:"businessId"`
	CustomerID     string `json:"customerId"`
	ConversationID string `json:"conversationId"`
	TotalAmount    string `json:"totalAmount"` // exact decimal string, no float
	Currency       string `json:"currency"`
}

// OrderCancelled is the self-contained payload of an order.cancelled event.
type OrderCancelled struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason"`
}

// Event is one row to write to domain_events. Payload is marshalled verbatim
// into the JSONB column; define typed structs per event type for the contract.
type Event struct {
	AggregateType string
	AggregateID   string
	Type          string
	Payload       any
}

// Insert writes one event inside tx and signals the dispatcher via NOTIFY on
// commit. Emission is idempotent via the unique (aggregate_type, aggregate_id,
// event_type) index: a conflicting insert returns false instead of erroring,
// which callers may treat as already emitted.
func Insert(ctx context.Context, tx pgx.Tx, e Event) (bool, error) {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return false, fmt.Errorf("events: marshal %s payload: %w", e.Type, err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO domain_events (id, aggregate_type, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (aggregate_type, aggregate_id, event_type) DO NOTHING`,
		uuid.NewString(), e.AggregateType, e.AggregateID, e.Type, payload)
	if err != nil {
		return false, fmt.Errorf("events: insert %s for %s/%s: %w", e.Type, e.AggregateType, e.AggregateID, err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	// Fire on commit: the notification reaches the dispatcher only once the
	// event's transaction is durable, so it never observes a phantom row.
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, ChannelName, e.AggregateID); err != nil {
		return false, fmt.Errorf("events: notify %s for %s/%s: %w", e.Type, e.AggregateType, e.AggregateID, err)
	}
	return true, nil
}
