package workers_test

import (
	"testing"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

func s7bPaymentDeps() workers.Deps {
	return workers.Deps{Pool: env.dbw.pool, Publisher: env.publisher}
}

func s7bChargeEvent(orderID, phone, bizID string, status string) paystack.NormalisedPaymentEvent {
	eventName := "charge.success"
	if status == "failed" {
		eventName = "charge.failed"
	}
	return paystack.NormalisedPaymentEvent{
		Provider:          "paystack",
		Event:             eventName,
		Reference:         orderID,
		AmountMinor:       5100, // 51.00 major units
		Currency:          "GHS",
		Status:            status,
		BusinessID:        bizID,
		AuthorizationBank: "MTN",
		RawJSON: []byte(`{"event":"` + eventName + `","data":{"reference":"` + orderID +
			`","amount":5100,"currency":"GHS","customer":{"phone":"` + phone + `"}` +
			`,"authorization":{"bank":"MTN"},"metadata":{"businessId":"` + bizID + `"}` +
			`,"paid_at":"2026-08-22T09:30:00.000Z"}}`),
	}
}

func TestS7b_PaymentSuccessClosesConversationToPaid(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	orderID := s7bCheckoutOrder(t, biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.pay-suc", prod.ID[:8])
	if state := s7bScalarString(t, `SELECT state::text FROM conversations WHERE id = $1`, conv.ID); state != "INVOICING" {
		t.Fatalf("precondition: conversation state = %s, want INVOICING", state)
	}

	if err := workers.HandlePaymentEvent(ctx(), s7bPaymentDeps(), s7bChargeEvent(orderID, cust.Phone, biz.ID, "success")); err != nil {
		t.Fatalf("HandlePaymentEvent: %v", err)
	}

	if status := s7bScalarString(t, `SELECT status::text FROM orders WHERE id = $1`, orderID); status != "PAID" {
		t.Errorf("order status = %s, want PAID", status)
	}
	if state := s7bScalarString(t, `SELECT state::text FROM conversations WHERE id = $1`, conv.ID); state != "PAID" {
		t.Errorf("conversation state = %s, want PAID", state)
	}
}

func TestS7b_PaymentFailedCancelsOrderAndConversation(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	orderID := s7bCheckoutOrder(t, biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.pay-fail", prod.ID[:8])

	if err := workers.HandlePaymentEvent(ctx(), s7bPaymentDeps(), s7bChargeEvent(orderID, cust.Phone, biz.ID, "failed")); err != nil {
		t.Fatalf("HandlePaymentEvent: %v", err)
	}

	if status := s7bScalarString(t, `SELECT status::text FROM orders WHERE id = $1`, orderID); status != "CANCELLED" {
		t.Errorf("order status = %s, want CANCELLED", status)
	}
	if state := s7bScalarString(t, `SELECT state::text FROM conversations WHERE id = $1`, conv.ID); state != "CANCELLED" {
		t.Errorf("conversation state = %s, want CANCELLED", state)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND event_type = 'order.cancelled'`, orderID); got != 1 {
		t.Errorf("order.cancelled events = %d, want 1", got)
	}
}

// TestS7b_FailedPaymentDoesNotCancelAlreadyPaidOrder guards the out-of-order
// case: a success webhook lands first, then a stale failed webhook must not
// roll a PAID order back to CANCELLED.
func TestS7b_FailedPaymentDoesNotCancelAlreadyPaidOrder(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	orderID := s7bCheckoutOrder(t, biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.pay-oof", prod.ID[:8])

	if err := workers.HandlePaymentEvent(ctx(), s7bPaymentDeps(), s7bChargeEvent(orderID, cust.Phone, biz.ID, "success")); err != nil {
		t.Fatalf("success event: %v", err)
	}
	if err := workers.HandlePaymentEvent(ctx(), s7bPaymentDeps(), s7bChargeEvent(orderID, cust.Phone, biz.ID, "failed")); err != nil {
		t.Fatalf("failed event: %v", err)
	}

	if status := s7bScalarString(t, `SELECT status::text FROM orders WHERE id = $1`, orderID); status != "PAID" {
		t.Errorf("order status = %s, want PAID (failed must not roll back)", status)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND event_type = 'order.cancelled'`, orderID); got != 0 {
		t.Errorf("order.cancelled events = %d, want 0 (already paid)", got)
	}
}
