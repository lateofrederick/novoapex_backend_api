// Package workers ports the Stage 7 business-logic processors and crons
// (libs/queue/src/processors): payment-event.processor.ts,
// follow-up.processor.ts, follow-up-scanner.processor.ts,
// outbox-sweep.processor.ts, retention-scanner.processor.ts.
//
// Money boundary: paystack.NormalisedPaymentEvent carries provider MINOR
// units; money.FromMinorUnits is the single minor->major conversion point.
// Everything downstream (payments.amount, order totals, message copy) stays
// shopspring decimal major units end-to-end.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/domain"
	"github.com/novoapex/novoapex-backend-api/internal/events"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/money"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// waClient is the narrow WhatsApp seam; *whatsapp.Client satisfies it today.
type waClient interface {
	SendTextMessage(ctx context.Context, phoneNumberID, to, text string) error
}

// Deps carries the worker subtree collaborators. A nil Publisher logs instead
// of enqueuing (ADR 0001 bridge gap); durable rows are never lost on it.
type Deps struct {
	Pool      *pgxpool.Pool
	Publisher queue.Publisher
	WA        waClient

	ReengagementThresholdMultiplier float64
	ReengagementCooldownDays        int
	NewArrivalsIntervalDays         int
}

// PaymentEventJob mirrors PaymentEventJobData (schemas/payment-event.schema.ts).
type PaymentEventJob struct {
	ProviderName string          `json:"providerName"`
	RawPayload   json.RawMessage `json:"rawPayload"`
	ReceivedAt   string          `json:"receivedAt"`
}

// outboundSendJob is the TaskOutboundSend body:
// outboundQueue.add('send-message', {outboundMessageId}).
type outboundSendJob struct {
	OutboundMessageID string `json:"outboundMessageId"`
}

// RegisterPaymentEvents wires the payment-events handler onto reg.
func RegisterPaymentEvents(reg queue.Registrar, deps Deps) {
	reg.Register(queue.TaskPaymentProcess, func(ctx context.Context, payload []byte) error {
		var job PaymentEventJob
		if err := json.Unmarshal(payload, &job); err != nil {
			return fmt.Errorf("payment-events: decode job: %w", err)
		}
		evt, err := paystack.ParseWebhookEvent(job.RawPayload)
		if err != nil {
			return fmt.Errorf("payment-events: parse webhook (%s): %w", job.ProviderName, err)
		}
		return HandlePaymentEvent(ctx, deps, evt)
	})
}

// HandlePaymentEvent ports PaymentEventProcessor.process
// (payment-event.processor.ts:41-348):
//
//  1. idempotency on payments.external_reference (+ unique-violation swallow),
//  2. Strategy A reconcile by reference == order.id with Decimal-equal amount,
//  3. Strategy B fallback phone+amount scoped by metadata businessId over the
//     top-5 CONFIRMED/PAYMENT_PENDING orders; ambiguous >1 abandons entirely
//     creating NO payment row (characterized T0.11b),
//  4. cancel pending scheduled follow-ups for reconciled orders,
//  5. Payment row with all fields incl rawWebhookPayload preserved verbatim,
//  6. customer confirmation outbound_messages row persisted BEFORE the
//     outbound enqueue; publish failure never loses the row (the outbox sweep
//     recovers it).
func HandlePaymentEvent(ctx context.Context, deps Deps, evt paystack.NormalisedPaymentEvent) error {
	pool := deps.Pool
	if pool == nil {
		return errors.New("payment-events: nil pool")
	}

	slog.InfoContext(ctx, "Processing payment event",
		"event", "payment_event_started",
		"providerName", evt.Provider)

	amountMajor := money.FromMinorUnits(evt.AmountMinor)
	customerPhone := customerPhoneFromRaw(evt.RawJSON)

	if evt.Reference != "" {
		var existingID string
		err := pool.QueryRow(ctx,
			`SELECT id FROM payments WHERE external_reference = $1`, evt.Reference).Scan(&existingID)
		if err == nil {
			slog.InfoContext(ctx, "Duplicate payment event — skipping",
				"event", "payment_event_duplicate",
				"externalReference", evt.Reference,
				"existingPaymentId", existingID)
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("payment-events: idempotency probe: %w", err)
		}
	}

	var customerID, orderID, businessID string
	now := time.Now().UTC()

	if evt.Reference != "" {
		var oBiz, oCust string
		var total decimal.Decimal
		err := pool.QueryRow(ctx, `
			SELECT o.business_id, o.customer_id, o.total_amount
			  FROM orders o
			  JOIN customers c ON c.id = o.customer_id
			 WHERE o.id = $1`, evt.Reference).Scan(&oBiz, &oCust, &total)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return fmt.Errorf("payment-events: strategy A lookup: %w", err)
		default:
			if total.Equal(amountMajor) {
				businessID, customerID, orderID = oBiz, oCust, evt.Reference
				if err := settleReconciledOrder(ctx, pool, evt, evt.Reference, now); err != nil {
					return err
				}
				slog.InfoContext(ctx, "Payment reconciled with order by reference",
					"event", "payment_reconciled",
					"orderId", evt.Reference,
					"customerId", oCust,
					"orderAmount", total.String(),
					"paymentAmount", amountMajor.String())
			} else {
				slog.WarnContext(ctx, "Order found by reference but amount mismatch",
					"event", "payment_amount_mismatch",
					"orderId", evt.Reference,
					"orderAmount", total.String(),
					"paymentAmount", amountMajor.String())
			}
		}
	}

	if orderID == "" && customerPhone != "" {
		query := `
			SELECT c.id, c.business_id, o.id, o.total_amount
			  FROM customers c
			  LEFT JOIN LATERAL (
			       SELECT id, total_amount
			         FROM orders
			        WHERE customer_id = c.id
			          AND status IN ('CONFIRMED', 'PAYMENT_PENDING')
			        ORDER BY created_at DESC
			        LIMIT 5) o ON true
			 WHERE c.phone = $1`
		args := []any{customerPhone}
		if evt.BusinessID != "" {
			query += ` AND c.business_id = $2`
			args = append(args, evt.BusinessID)
		}

		type candidate struct {
			customerID string
			businessID string
			orderID    string
		}
		customers := map[string]string{}
		var candidates []candidate

		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("payment-events: strategy B query: %w", err)
		}
		for rows.Next() {
			var cID, cBiz string
			var oID pgtype.Text
			var total decimal.Decimal
			if err := rows.Scan(&cID, &cBiz, &oID, &total); err != nil {
				rows.Close()
				return fmt.Errorf("payment-events: strategy B scan: %w", err)
			}
			customers[cID] = cBiz
			if !oID.Valid || !total.Equal(amountMajor) {
				continue
			}
			candidates = append(candidates, candidate{customerID: cID, businessID: cBiz, orderID: oID.String})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("payment-events: strategy B rows: %w", err)
		}
		rows.Close()

		switch {
		case len(candidates) == 1:
			match := candidates[0]
			customerID, businessID, orderID = match.customerID, match.businessID, match.orderID
			if err := settleReconciledOrder(ctx, pool, evt, match.orderID, now); err != nil {
				return err
			}
			slog.InfoContext(ctx, "Payment reconciled with order by phone+amount",
				"event", "payment_reconciled",
				"orderId", match.orderID,
				"customerId", match.customerID,
				"paymentAmount", amountMajor.String())
		case len(candidates) > 1:
			slog.WarnContext(ctx, "Ambiguous phone+amount match across tenants — manual reconciliation needed",
				"event", "payment_ambiguous_match",
				"customerPhone", customerPhone,
				"amount", amountMajor.String(),
				"candidateCount", len(candidates))
		case evt.BusinessID != "" && len(customers) == 1:
			for cID := range customers {
				customerID = cID
			}
			businessID = evt.BusinessID
			logPaymentUnmatched(ctx, customerPhone, customerID, amountMajor)
		case len(customers) == 1:
			for cID, cBiz := range customers {
				customerID, businessID = cID, cBiz
			}
			logPaymentUnmatched(ctx, customerPhone, customerID, amountMajor)
		case len(customers) > 1:
			slog.WarnContext(ctx, "Payment phone maps to multiple tenants with no order match — cannot attribute",
				"event", "payment_ambiguous_no_order",
				"customerPhone", customerPhone,
				"amount", amountMajor.String(),
				"tenantCount", len(customers))
		}
	}

	if businessID == "" {
		slog.WarnContext(ctx, "Cannot determine business for payment — no matching customer found",
			"event", "payment_no_business",
			"customerPhone", customerPhone,
			"externalReference", evt.Reference)
		return nil
	}

	if orderID != "" {
		tag, err := pool.Exec(ctx, `
			UPDATE scheduled_follow_ups
			   SET cancelled_at = $2
			 WHERE order_id = $1 AND executed_at IS NULL AND cancelled_at IS NULL`,
			orderID, now)
		if err != nil {
			return fmt.Errorf("payment-events: cancel follow-ups: %w", err)
		}
		if tag.RowsAffected() > 0 {
			slog.InfoContext(ctx, "Cancelled pending follow-ups after payment",
				"event", "follow_ups_cancelled",
				"orderId", orderID,
				"cancelledCount", tag.RowsAffected())
		}
	}

	status := paymentStatusFor(evt.Status)

	paymentID, err := insertPayment(ctx, pool, paymentInsert{
		businessID:     businessID,
		customerID:     customerID,
		orderID:        orderID,
		reference:      evt.Reference,
		amount:         amountMajor,
		currency:       evt.Currency,
		provider:       evt.Provider,
		network:        evt.AuthorizationBank,
		status:         status,
		paidAt:         evt.PaidAt.UTC(),
		reconciled:     orderID != "",
		reconciledAt:   now,
		rawWebhookBody: evt.RawJSON,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			slog.InfoContext(ctx, "Duplicate payment event — skipping",
				"event", "payment_event_duplicate",
				"externalReference", evt.Reference)
			return nil
		}
		return fmt.Errorf("payment-events: create payment: %w", err)
	}

	slog.InfoContext(ctx, "Payment record created",
		"event", "payment_created",
		"paymentId", paymentID,
		"status", status,
		"reconciled", orderID != "")

	// Payment-behaviour signal: remember how this customer actually pays.
	// Latest successful payment wins — same policy as sentiment/delivery_area
	// in ProfileBuilderHandler. Non-blocking: never let this fail the webhook.
	if customerID != "" && evt.Status == "success" && evt.AuthorizationBank != "" {
		if _, perr := pool.Exec(ctx,
			`UPDATE customer_profiles SET preferred_payment_network = $1, updated_at = CURRENT_TIMESTAMP WHERE customer_id = $2`,
			evt.AuthorizationBank, customerID); perr != nil {
			slog.WarnContext(ctx, "Failed to record preferred payment network (non-blocking)",
				"customerId", customerID, "error", perr.Error())
		}
	}

	if orderID != "" && evt.Status == "success" && customerID != "" {
		if err := sendPaymentConfirmation(ctx, deps, confirmationInput{
			businessID:  businessID,
			customerID:  customerID,
			currency:    evt.Currency,
			amountMajor: amountMajor,
		}); err != nil {
			return err
		}
	}

	return nil
}

func paymentStatusFor(normalisedStatus string) string {
	switch normalisedStatus {
	case "success":
		return "SUCCESS"
	case "failed":
		return "FAILED"
	default:
		return "PENDING"
	}
}

// settleReconciledOrder applies the payment outcome to a reconciled order and
// its conversation state:
//
//	success -> order PAID + conversation INVOICING -> PAID
//	failed  -> order CANCELLED + conversation INVOICING -> CANCELLED
//	           + order.cancelled event (for restock / reset)
//
// Anything else that reconciled (e.g. a "pending" mobile-money settle) is
// treated as paid, matching the reconciliation's amount-match contract.
func settleReconciledOrder(ctx context.Context, pool *pgxpool.Pool, evt paystack.NormalisedPaymentEvent, orderID string, now time.Time) error {
	if evt.Status == "failed" {
		return cancelOrder(ctx, pool, orderID, "payment_failed", now)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE orders SET status = 'PAID', updated_at = $2 WHERE id = $1`, orderID, now); err != nil {
		return fmt.Errorf("payment-events: mark order PAID: %w", err)
	}
	return transitionOrderConversation(ctx, pool, orderID, domain.StateInvoicing, domain.StatePaid)
}

// cancelOrder marks the order CANCELLED (unless already paid), closes the
// conversation INVOICING -> CANCELLED and emits order.cancelled — all in one
// transaction so the event can never exist without its state change. Shared by
// the failed-payment path and the checkout-expiry scanner.
func cancelOrder(ctx context.Context, pool *pgxpool.Pool, orderID, reason string, now time.Time) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE orders SET status = 'CANCELLED', updated_at = $2
			  WHERE id = $1 AND status IN ('CONFIRMED', 'PAYMENT_PENDING')`, orderID, now)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // already paid / cancelled / missing
		}

		var convID string
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(conversation_id, '') FROM orders WHERE id = $1`, orderID).Scan(&convID); err != nil {
			return err
		}
		if convID != "" {
			if _, err := domain.TransitionTx(ctx, tx, convID, domain.StateInvoicing, domain.StateCancelled); err != nil {
				return err
			}
		}

		_, err = events.Insert(ctx, tx, events.Event{
			AggregateType: events.AggregateTypeOrder,
			AggregateID:   orderID,
			Type:          events.TypeOrderCancelled,
			Payload:       events.OrderCancelled{OrderID: orderID, Reason: reason},
		})
		return err
	})
}

// transitionOrderConversation transitions the order's conversation from `from`
// to `to`; a no-op when the order has no conversation or already left `from`.
func transitionOrderConversation(ctx context.Context, pool *pgxpool.Pool, orderID string, from, to domain.ConversationState) error {
	var convID string
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(conversation_id, '') FROM orders WHERE id = $1`, orderID).Scan(&convID); err != nil {
		return fmt.Errorf("payment-events: order conversation lookup: %w", err)
	}
	if convID == "" {
		return nil
	}
	ok, err := domain.Transition(ctx, pool, convID, from, to)
	if err != nil {
		return err
	}
	if !ok {
		slog.InfoContext(ctx, "Order conversation transition skipped",
			"orderId", orderID, "from", from, "to", to)
	}
	return nil
}

type paymentInsert struct {
	businessID     string
	customerID     string
	orderID        string
	reference      string
	amount         decimal.Decimal
	currency       string
	provider       string
	network        string
	status         string
	paidAt         time.Time
	reconciled     bool
	reconciledAt   time.Time
	rawWebhookBody []byte
}

func insertPayment(ctx context.Context, pool *pgxpool.Pool, p paymentInsert) (string, error) {
	var customerArg, orderArg any
	if p.customerID != "" {
		customerArg = p.customerID
	}
	if p.orderID != "" {
		orderArg = p.orderID
	}
	var referenceArg, networkArg any
	if p.reference != "" {
		referenceArg = p.reference
	}
	if p.network != "" {
		networkArg = p.network
	}
	var paidAtArg any
	if !p.paidAt.IsZero() {
		paidAtArg = p.paidAt
	}
	var reconciledAtArg any
	if p.reconciled {
		reconciledAtArg = p.reconciledAt
	}

	id := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO payments
			(id, business_id, customer_id, order_id, external_reference, amount,
			 currency, provider, network, status, paid_at, reconciled_at, raw_webhook_payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		id, p.businessID, customerArg, orderArg, referenceArg, p.amount,
		p.currency, p.provider, networkArg, p.status,
		paidAtArg, reconciledAtArg, p.rawWebhookBody); err != nil {
		return "", err
	}
	return id, nil
}

type confirmationInput struct {
	businessID  string
	customerID  string
	currency    string
	amountMajor decimal.Decimal
}

func sendPaymentConfirmation(ctx context.Context, deps Deps, in confirmationInput) error {
	var phone string
	err := deps.Pool.QueryRow(ctx,
		`SELECT phone FROM customers WHERE id = $1`, in.customerID).Scan(&phone)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("payment-events: confirmation customer lookup: %w", err)
	}

	var conversationID string
	err = deps.Pool.QueryRow(ctx,
		`SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
		in.businessID, phone).Scan(&conversationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("payment-events: confirmation conversation lookup: %w", err)
	}

	formattedAmount := in.currency + " " + in.amountMajor.StringFixed(2)
	text := fmt.Sprintf(
		"✅ Payment of *%s* received for your order. Thank you! 🎉\n\nYour order is now being processed.",
		formattedAmount)

	rawPayload := []byte(fmt.Sprintf(
		`{"messaging_product":"whatsapp","to":%q,"type":"text","text":{"body":%q}}`,
		phone, text))

	outboundID := uuid.NewString()
	if _, err := deps.Pool.Exec(ctx, `
		INSERT INTO outbound_messages
			(id, business_id, conversation_id, recipient_phone, message_type,
			 text_content, raw_payload, status)
		VALUES ($1, $2, $3, $4, 'text', $5, $6, 'pending')`,
		outboundID, in.businessID, conversationID, phone, text, rawPayload); err != nil {
		return fmt.Errorf("payment-events: persist confirmation outbound: %w", err)
	}

	if deps.Publisher == nil {
		slog.WarnContext(ctx, "outbound publish skipped: no queue bridge wired (ADR 0001)",
			"outboundMessageId", outboundID)
		return nil
	}
	if err := queue.PublishOutbound(ctx, deps.Publisher, outboundID); err != nil {
		slog.ErrorContext(ctx, "Failed to enqueue payment confirmation (row kept for outbox sweep)",
			"outboundMessageId", outboundID,
			"error", err.Error())
		return nil
	}

	slog.InfoContext(ctx, "Payment confirmation message enqueued",
		"event", "payment_confirmation_sent",
		"customerId", in.customerID,
		"conversationId", conversationID)
	return nil
}

func logPaymentUnmatched(ctx context.Context, phone, customerID string, amount decimal.Decimal) {
	slog.WarnContext(ctx, "Payment received but no matching order found — manual reconciliation needed",
		"event", "payment_unmatched",
		"customerPhone", phone,
		"amount", amount.String(),
		"customerId", customerID)
}

// customerPhoneFromRaw reads data.customer.phone from the verbatim webhook
// body. Delta note vs Node: NormalisedPaymentEvent there carries
// customerPhone; the Go contract does not (yet), so Strategy B extracts it
// here until the contract gains the field.
func customerPhoneFromRaw(raw []byte) string {
	var body struct {
		Data struct {
			Customer struct {
				Phone string `json:"phone"`
			} `json:"customer"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return ""
	}
	return body.Data.Customer.Phone
}
