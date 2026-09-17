package workers_test

import (
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/events"
	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

// F.26 — regression for the original duplicate-order bug: a customer confirms,
// pays, then the LLM re-emits order_confirmed on a follow-up. Checkout's CAS
// guard must reject the re-emit so exactly ONE order and ONE payment link exist.
func TestRegression_DuplicateOrderProducesOneOrderOneLink(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	s7bExec(t, `UPDATE businesses SET payment_callback_url = 'https://shop.example.com/success' WHERE id = $1`, biz.ID)
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(10), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	// Confirm the order.
	s7bRunCheckout(t, s7bCheckoutJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.dup-1",
		[]workers.DetectedItem{{ProductID: prod.ID[:8], Quantity: 2}}))
	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)

	// Customer pays.
	if err := workers.HandlePaymentEvent(ctx(), s7bPaymentDeps(), s7bChargeEvent(orderID, cust.Phone, biz.ID, "success")); err != nil {
		t.Fatalf("payment success: %v", err)
	}
	if state := s7bScalarString(t, `SELECT state::text FROM conversations WHERE id = $1`, conv.ID); state != "PAID" {
		t.Fatalf("precondition: state = %s, want PAID", state)
	}

	// Follow-up: the LLM re-emits order_confirmed with a NEW source message id.
	s7bRunCheckout(t, s7bCheckoutJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.dup-2",
		[]workers.DetectedItem{{ProductID: prod.ID[:8], Quantity: 2}}))

	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE conversation_id = $1`, conv.ID); got != 1 {
		t.Fatalf("orders = %d, want exactly 1 (duplicate must be rejected)", got)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM domain_events WHERE event_type = 'order.created' AND payload->>'conversationId' = $1`, conv.ID); got != 1 {
		t.Fatalf("order.created events = %d, want exactly 1", got)
	}

	// Dispatch + payment-init: one link, one Paystack call.
	paystack := &paystackFake{}
	dispatcher := &events.Dispatcher{
		Pool:      env.dbw.pool,
		Publisher: env.publisher,
		Subscriptions: map[string][]events.Subscription{
			events.TypeOrderCreated: {{Queue: "payment-init", TaskType: "payment-init:initiate"}},
		},
	}
	if _, err := dispatcher.Drain(ctx()); err != nil {
		t.Fatalf("dispatcher.Drain: %v", err)
	}
	evt := s7bReadOrderCreatedEvent(t, orderID)
	if err := workers.HandlePaymentInit(ctx(), workers.PaymentInitDeps{Pool: env.dbw.pool, Publisher: env.publisher, Paystack: paystack}, evt); err != nil {
		t.Fatalf("HandlePaymentInit: %v", err)
	}

	if paystack.count() != 1 {
		t.Errorf("paystack calls = %d, want 1", paystack.count())
	}
	if url := s7bScalarString(t, `SELECT COALESCE(payment_url,'') FROM orders WHERE id = $1`, orderID); url == "" {
		t.Error("payment_url not set")
	}
}

// F.27 — outbox atomicity: the order and its order.created event commit (or
// roll back) together, so an order can never exist without its event, or vice
// versa.
func TestOutboxAtomicity_NoOrphanOrderOrEvent(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)

	// Success: order + event both exist.
	convOK := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	orderOK := s7bCheckoutOrder(t, biz.ID, cust.ID, convOK.ID, cust.Phone, "wamid.atomic-ok", prod.ID[:8])
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE id = $1`, orderOK); got != 1 {
		t.Fatalf("order rows = %d, want 1", got)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND event_type = 'order.created'`, orderOK); got != 1 {
		t.Fatalf("order.created events = %d, want 1 (no orphan order)", got)
	}

	// Rollback (insufficient stock): neither order nor event may remain.
	prodLow := env.dbw.factory.Product(biz.ID, harness.WithStock(1), harness.WithPrice("25.50"))
	convShort := env.dbw.factory.Conversation(biz.ID, cust.Phone+"-x", harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	s7bRunCheckout(t, s7bCheckoutJob(biz.ID, cust.ID, convShort.ID, cust.Phone+"-x", "wamid.atomic-short",
		[]workers.DetectedItem{{ProductID: prodLow.ID[:8], Quantity: 5}}))

	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE conversation_id = $1`, convShort.ID); got != 0 {
		t.Fatalf("orders after rollback = %d, want 0", got)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM domain_events WHERE event_type = 'order.created' AND payload->>'conversationId' = $1`, convShort.ID); got != 0 {
		t.Fatalf("order.created events after rollback = %d, want 0 (no orphan event)", got)
	}
}
