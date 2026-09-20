// Payment initiation: the order.created consumer that generates and delivers
// the checkout link, split out of the old CRM materialiser. Idempotent on the
// order's payment_url (first write wins), so an at-least-once redelivery never
// produces a second Paystack transaction.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/events"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// PaystackInitiator mirrors PaymentProvider.initiatePayment
// (libs/common/src/payments/payment-provider.interface.ts:122) — the single
// capability payment initiation needs from the payment factory.
type PaystackInitiator interface {
	InitiatePayment(ctx context.Context, req PaymentRequest) (PaymentLink, error)
}

// PaymentRequest mirrors InitiatePaymentParams. Amount stays in MAJOR units
// here; conversion to provider minor units belongs to the Paystack adapter.
type PaymentRequest struct {
	Amount        decimal.Decimal
	Currency      string
	CustomerPhone string
	Reference     string
	CallbackURL   string
	BusinessID    string
}

// PaymentLink mirrors InitiatePaymentResult.
type PaymentLink struct {
	ProviderReference string
	PaymentURL        string
	Status            string // "initiated" | "failed"
}

// PaymentInitDeps carries the payment-init collaborators.
type PaymentInitDeps struct {
	Pool      *pgxpool.Pool
	Publisher queue.Publisher
	Paystack  PaystackInitiator
}

// RegisterPaymentInit attaches the payment-init handler to the worker registrar.
func RegisterPaymentInit(reg queue.Registrar, deps PaymentInitDeps) {
	reg.Register(queue.TaskPaymentInit, func(ctx context.Context, payload []byte) error {
		var evt events.OrderCreated
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("payment-init: decode order.created: %w", err)
		}
		return HandlePaymentInit(ctx, deps, evt)
	})
}

// HandlePaymentInit generates the Paystack link for a confirmed order and
// enqueues the invoice message. Idempotent via orders.payment_url.
func HandlePaymentInit(ctx context.Context, deps PaymentInitDeps, evt events.OrderCreated) error {
	// Idempotency: skip when a link was already generated for this order.
	var existing string
	err := deps.Pool.QueryRow(ctx,
		`SELECT COALESCE(payment_url, '') FROM orders WHERE id = $1`, evt.OrderID).Scan(&existing)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		slog.Warn("Payment init for missing order — skipping", "orderId", evt.OrderID)
		return nil
	case err != nil:
		return err
	}
	if existing != "" {
		slog.Info("Payment already initiated — skipping", "orderId", evt.OrderID)
		return nil
	}

	var phone, callbackURL, waPhoneNumberID string
	err = deps.Pool.QueryRow(ctx, `
		SELECT c.phone, COALESCE(b.payment_callback_url, ''), b.whatsapp_phone_number_id
		  FROM customers c
		  JOIN businesses b ON b.id = c.business_id
		 WHERE c.id = $1`, evt.CustomerID).Scan(&phone, &callbackURL, &waPhoneNumberID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		slog.Warn("Payment init: customer/business missing — skipping", "customerId", evt.CustomerID)
		return nil
	case err != nil:
		return err
	}
	if callbackURL == "" {
		callbackURL = "https://novoapex.com/success"
	}

	total, err := decimal.NewFromString(evt.TotalAmount)
	if err != nil {
		return fmt.Errorf("payment-init: parse total %q: %w", evt.TotalAmount, err)
	}

	result, err := deps.Paystack.InitiatePayment(ctx, PaymentRequest{
		Amount:        total,
		Currency:      evt.Currency,
		CustomerPhone: phone,
		Reference:     evt.OrderID,
		CallbackURL:   callbackURL,
		BusinessID:    evt.BusinessID,
	})
	if err != nil {
		return err // transient -> retry
	}
	if result.Status != "initiated" || result.PaymentURL == "" {
		slog.Warn("Payment initiation returned no link", "orderId", evt.OrderID, "status", result.Status)
		return errors.New("payment-init: no payment link returned")
	}

	// Persist the link (first write wins) so a racing redelivery skips.
	tag, err := deps.Pool.Exec(ctx,
		`UPDATE orders SET payment_url = $2, updated_at = CURRENT_TIMESTAMP
		  WHERE id = $1 AND payment_url IS NULL`,
		evt.OrderID, result.PaymentURL)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		slog.Info("Payment init lost the race — skipping", "orderId", evt.OrderID)
		return nil
	}

	slog.Info("Payment link generated", "orderId", evt.OrderID, "paymentUrl", result.PaymentURL)

	// Deliver the invoice through the outbound queue, gated on the same
	// identity fields as the source.
	if phone == "" || waPhoneNumberID == "" {
		slog.Warn("Skipped sending payment link — customer phone or whatsappPhoneNumberId missing",
			"orderId", evt.OrderID)
		return nil
	}
	if err := enqueuePaymentInvoice(ctx, deps, evt, phone, total, result.PaymentURL); err != nil {
		return err
	}
	return nil
}

func enqueuePaymentInvoice(ctx context.Context, deps PaymentInitDeps, evt events.OrderCreated, phone string, total decimal.Decimal, paymentURL string) error {
	methods := GetCurrencyConfig(evt.Currency).PaymentMethods
	paymentText := fmt.Sprintf(
		"Thank you for confirming your order!\n\nYour total is *%s %s*.\n\nTap the link below to pay securely with %s:\n%s",
		evt.Currency, total.StringFixed(2), methods, paymentURL)

	rawPayload, err := json.Marshal(map[string]any{
		"messaging_product": "whatsapp",
		"to":                phone,
		"type":              "text",
		"text":              map[string]string{"body": paymentText},
	})
	if err != nil {
		return err
	}

	outboundID := crmNewID()
	if _, err := deps.Pool.Exec(ctx, `
		INSERT INTO outbound_messages
			(id, recipient_phone, message_type, text_content, raw_payload, status,
			 business_id, conversation_id)
		VALUES ($1, $2, 'text', $3, $4, 'pending', $5, NULLIF($6, ''))`,
		outboundID, phone, paymentText, rawPayload, evt.BusinessID, evt.ConversationID); err != nil {
		return fmt.Errorf("payment-init: persist invoice outbound: %w", err)
	}

	if deps.Publisher != nil {
		if err := queue.PublishOutbound(ctx, deps.Publisher, outboundID); err != nil {
			return err
		}
	}
	slog.Info("Payment link enqueued to outbound queue",
		"orderId", evt.OrderID, "outboundMessageId", outboundID)
	return nil
}
