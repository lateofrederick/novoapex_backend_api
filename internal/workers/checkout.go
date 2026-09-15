// Package workers Stage checkout: the transactional order-creation pipeline, split out of the
// CRM materialiser (see the event-driven checkout/CRM decoupling design).
//
// A checkout job runs when the orchestrator observes an order_confirmed turn.
// It creates the order, its items and the conditional stock decrement, flips
// the conversation CHECKOUT -> INVOICING, and emits an order.created event —
// all in ONE transaction. Payment initiation, profile-stats and follow-up
// scheduling are separate consumers of order.created, not part of this job.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/domain"
	"github.com/novoapex/novoapex-backend-api/internal/events"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// CheckoutJob is the queue payload for order creation: the order-relevant
// subset of a confirmed turn, decoupled from the CRM enrichment signal.
type CheckoutJob struct {
	BusinessID        string         `json:"businessId"`
	CustomerID        string         `json:"customerId"`
	ConversationID    string         `json:"conversationId"`
	CustomerPhone     string         `json:"customerPhone,omitempty"`
	SourceMessageID   string         `json:"sourceMessageId,omitempty"` // deterministic order-idempotency key
	DetectedItems     []DetectedItem `json:"detectedItems"`
	FulfillmentChoice *string        `json:"fulfillmentChoice,omitempty"`
	PickupLocationID  *string        `json:"pickupLocationId,omitempty"`
}

// CheckoutDeps carries everything order creation touches. Unlike the CRM
// materialiser it needs no publisher: the order.created event is persisted
// transactionally and a dispatcher (not this job) fans it out.
type CheckoutDeps struct {
	Pool *pgxpool.Pool
}

// errCheckoutNotOrderable is a non-fatal rejection: the conversation is not in
// an orderable state (BROWSING/CHECKOUT) — already invoicing, or a race — so
// no order may be created.
var errCheckoutNotOrderable = errors.New("checkout: conversation is not in an orderable state")

// errCheckoutInsufficientStock mirrors InsufficientStockError: thrown inside
// the order transaction to roll it back.
var errCheckoutInsufficientStock = errors.New("Insufficient stock for one or more order items") //nolint:staticcheck // ST1005: verbatim port of the TS error message

// RegisterCheckout attaches the checkout handler to the worker registrar.
func RegisterCheckout(reg queue.Registrar, deps CheckoutDeps) {
	reg.Register(queue.TaskCheckout, func(ctx context.Context, payload []byte) error {
		var job CheckoutJob
		if err := json.Unmarshal(payload, &job); err != nil {
			return fmt.Errorf("checkout: decode job payload: %w", err)
		}
		return HandleCheckout(ctx, deps, job)
	})
}

// HandleCheckout creates one order from a confirmed turn. Every non-fatal
// rejection (not in CHECKOUT, duplicate, insufficient stock) returns nil so
// the job does not retry.
func HandleCheckout(ctx context.Context, deps CheckoutDeps, job CheckoutJob) error {
	if len(job.DetectedItems) == 0 {
		slog.Warn("Checkout job with no detected items — skipping", "conversationId", job.ConversationID)
		return nil
	}

	q := gen.New(deps.Pool)

	// 1. Resolve short product IDs to full products (read-only; LLM output is
	// untrusted, so invalid/unknown IDs are dropped rather than rejected).
	resolved, err := checkoutResolveProducts(ctx, q, job)
	if err != nil {
		return err
	}
	if len(resolved) == 0 {
		slog.Error("All checkout items failed resolution — no order created",
			"conversationId", job.ConversationID)
		return nil
	}

	// 2. Total using exact decimal arithmetic.
	totalAmount := decimal.Zero
	for _, item := range resolved {
		totalAmount = totalAmount.Add(item.unitPrice.Mul(decimal.NewFromInt32(item.quantity)))
	}

	// 3. Tenant currency (business lookup is best-effort: a missing row still
	// creates the order under the default currency).
	orderCurrency := "GHS"
	business, bizErr := q.GetBusinessForOrder(ctx, job.BusinessID)
	switch {
	case bizErr == nil && business.Currency != "":
		orderCurrency = business.Currency
	case errors.Is(bizErr, pgx.ErrNoRows):
		// fall through with default currency
	default:
		return bizErr
	}

	// 4. Fulfillment (delivery vs pickup); never blocks order creation.
	fulfillmentType, locationID, err := checkoutResolveFulfillment(ctx, q, job)
	if err != nil {
		return err
	}

	// 5. The order + state flip + event in ONE transaction. The CAS transition
	// claims the one-order-at-a-time slot; if it loses, the whole tx rolls back
	// and no order or event is written.
	orderID := crmNewID()
	txErr := pgx.BeginFunc(ctx, deps.Pool, func(tx pgx.Tx) error {
		tq := gen.New(tx)

		ok, terr := domain.TransitionToTx(ctx, tx, job.ConversationID,
			[]domain.ConversationState{domain.StateBrowsing, domain.StateCheckout},
			domain.StateInvoicing)
		if terr != nil {
			return terr
		}
		if !ok {
			return errCheckoutNotOrderable
		}

		if _, ierr := tq.InsertOrder(ctx, gen.InsertOrderParams{
			ID:              orderID,
			BusinessID:      job.BusinessID,
			CustomerID:      job.CustomerID,
			ConversationID:  crmText(job.ConversationID),
			IdempotencyKey:  crmText(job.SourceMessageID),
			TotalAmount:     totalAmount,
			Currency:        orderCurrency,
			FulfillmentType: fulfillmentType,
			LocationID:      locationID,
		}); ierr != nil {
			return ierr
		}

		items := make([]checkoutOrderItemRow, len(resolved))
		for i, item := range resolved {
			items[i] = checkoutOrderItemRow{
				ID:          crmNewID(),
				OrderID:     orderID,
				ProductID:   item.productID,
				ProductName: item.productName,
				Quantity:    item.quantity,
				UnitPrice:   item.unitPrice.String(),
			}
		}
		itemsJSON, merr := json.Marshal(items)
		if merr != nil {
			return merr
		}
		if ierr := tq.InsertOrderItems(ctx, itemsJSON); ierr != nil {
			return ierr
		}

		// Aggregate per-product quantities, then one conditional decrement.
		qtyByProduct := make(map[string]int64, len(resolved))
		var entries []checkoutStockEntry
		for _, item := range resolved {
			if _, seen := qtyByProduct[item.productID]; !seen {
				entries = append(entries, checkoutStockEntry{ID: item.productID})
			}
			qtyByProduct[item.productID] += int64(item.quantity)
		}
		for i := range entries {
			entries[i].Qty = int32(qtyByProduct[entries[i].ID])
		}
		entriesJSON, merr := json.Marshal(entries)
		if merr != nil {
			return merr
		}
		affected, derr := tq.DecrementStockIfAvailable(ctx, entriesJSON)
		if derr != nil {
			return derr
		}
		if affected != int64(len(entries)) {
			return errCheckoutInsufficientStock
		}

		// Emit order.created in the same tx — the event cannot exist without
		// its order (and vice versa).
		if _, eerr := events.Insert(ctx, tx, events.Event{
			AggregateType: events.AggregateTypeOrder,
			AggregateID:   orderID,
			Type:          events.TypeOrderCreated,
			Payload: events.OrderCreated{
				OrderID:     orderID,
				BusinessID:  job.BusinessID,
				CustomerID:  job.CustomerID,
				TotalAmount: totalAmount.String(),
				Currency:    orderCurrency,
			},
		}); eerr != nil {
			return eerr
		}
		return nil
	})

	switch {
	case txErr == nil:
		// committed
	case errors.Is(txErr, errCheckoutNotOrderable):
		slog.Warn("Checkout skipped — conversation not in an orderable state",
			"conversationId", job.ConversationID)
		return nil
	case checkoutDuplicateIdempotency(txErr):
		slog.Warn("Checkout skipped — duplicate idempotency key",
			"sourceMessageId", job.SourceMessageID)
		return nil
	case errors.Is(txErr, errCheckoutInsufficientStock):
		slog.Warn("Checkout rolled back — insufficient stock",
			"conversationId", job.ConversationID)
		return nil
	default:
		return txErr
	}

	slog.Info("Order created",
		"orderId", orderID,
		"conversationId", job.ConversationID,
		"customerId", job.CustomerID,
		"totalAmount", totalAmount.StringFixed(2),
		"itemCount", len(resolved))
	return nil
}

// checkoutResolvedItem is the post-resolution ledger entry.
type checkoutResolvedItem struct {
	productID   string
	productName string
	unitPrice   decimal.Decimal
	quantity    int32
}

// checkoutOrderItemRow is one orderItem.createMany payload row (exact decimal
// string cast SQL-side; never a float64 JSON number).
type checkoutOrderItemRow struct {
	ID          string `json:"id"`
	OrderID     string `json:"order_id"`
	ProductID   string `json:"product_id"`
	ProductName string `json:"product_name"`
	Quantity    int32  `json:"quantity"`
	UnitPrice   string `json:"unit_price"`
}

// checkoutStockEntry is one aggregated per-product stock decrement.
type checkoutStockEntry struct {
	ID  string `json:"id"`
	Qty int32  `json:"qty"`
}

// checkoutResolveProducts resolves each detected short ID to a full product,
// dropping invalid or unknown IDs (LLM output is untrusted) without failing
// the job.
func checkoutResolveProducts(ctx context.Context, q *gen.Queries, job CheckoutJob) ([]checkoutResolvedItem, error) {
	resolved := make([]checkoutResolvedItem, 0, len(job.DetectedItems))
	for _, item := range job.DetectedItems {
		if !crmValidShortID.MatchString(item.ProductID) {
			slog.Warn("Invalid product short ID rejected",
				"event", "invalid_short_id",
				"shortId", item.ProductID,
				"businessId", job.BusinessID)
			continue
		}

		product, err := q.ResolveProductByShortID(ctx, gen.ResolveProductByShortIDParams{
			BusinessID: job.BusinessID,
			ID:         crmEscapeLikePattern(item.ProductID) + "%",
		})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			slog.Warn("Product not found or unavailable, skipping",
				"shortId", item.ProductID, "businessId", job.BusinessID)
			continue
		case err != nil:
			return nil, err
		}

		quantity := item.Quantity
		if quantity == 0 {
			quantity = 1 // TS `item.quantity || 1`
		}
		resolved = append(resolved, checkoutResolvedItem{
			productID:   product.ID,
			productName: product.Name,
			unitPrice:   product.Price,
			quantity:    quantity,
		})
	}
	return resolved, nil
}

// checkoutResolveFulfillment resolves delivery-vs-pickup. Defaults to DELIVERY
// unless the customer chose pickup; an unresolvable pickup location still
// returns PICKUP with a nil location, never blocking order creation.
func checkoutResolveFulfillment(ctx context.Context, q *gen.Queries, job CheckoutJob) (gen.FulfillmentType, pgtype.Text, error) {
	if job.FulfillmentChoice == nil || *job.FulfillmentChoice != "pickup" {
		return gen.FulfillmentTypeDELIVERY, pgtype.Text{}, nil
	}

	shortID := ""
	if job.PickupLocationID != nil {
		shortID = *job.PickupLocationID
	}
	if shortID == "" || !crmValidShortID.MatchString(shortID) {
		slog.Warn("Pickup chosen but no valid location ID was provided — order created without a location",
			"businessId", job.BusinessID)
		return gen.FulfillmentTypePICKUP, pgtype.Text{}, nil
	}

	locID, err := q.ResolvePickupLocationByShortID(ctx, gen.ResolvePickupLocationByShortIDParams{
		BusinessID: job.BusinessID,
		ID:         crmEscapeLikePattern(shortID) + "%",
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		slog.Warn("Pickup location could not be resolved — order created without a location",
			"businessId", job.BusinessID, "shortLocationId", shortID)
		return gen.FulfillmentTypePICKUP, pgtype.Text{}, nil
	case err != nil:
		return "", pgtype.Text{}, err
	}

	return gen.FulfillmentTypePICKUP, pgtype.Text{String: locID, Valid: true}, nil
}

// checkoutDuplicateIdempotency maps the order unique-violation onto the
// duplicate-idempotency branch (orders_idempotency_key_key).
func checkoutDuplicateIdempotency(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23505" && strings.Contains(pgErr.ConstraintName, "idempotency_key")
}
