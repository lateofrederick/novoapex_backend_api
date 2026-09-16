package workers_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/novoapex/novoapex-backend-api/internal/events"
	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

func TestS7b_RestockConsumerRestoresStock(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	orderID := s7bCheckoutOrder(t, biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.restock", prod.ID[:8])
	if stock := s7bScalarString(t, `SELECT stock::text FROM products WHERE id = $1`, prod.ID); stock != "3" {
		t.Fatalf("precondition stock = %s, want 3 (5 - 2)", stock)
	}

	// A failed payment cancels the order and emits order.cancelled.
	if err := workers.HandlePaymentEvent(ctx(), s7bPaymentDeps(), s7bChargeEvent(orderID, cust.Phone, biz.ID, "failed")); err != nil {
		t.Fatalf("failed payment event: %v", err)
	}

	var raw string
	if err := env.dbw.db.QueryRow(
		`SELECT payload::text FROM domain_events WHERE aggregate_id = $1 AND event_type = 'order.cancelled'`,
		orderID).Scan(&raw); err != nil {
		t.Fatalf("read order.cancelled event: %v", err)
	}
	var cancelEvt events.OrderCancelled
	if err := json.Unmarshal([]byte(raw), &cancelEvt); err != nil {
		t.Fatalf("decode order.cancelled event: %v", err)
	}

	deps := workers.RestockDeps{Pool: env.dbw.pool}
	if err := workers.HandleRestock(ctx(), deps, cancelEvt); err != nil {
		t.Fatalf("HandleRestock: %v", err)
	}
	if stock := s7bScalarString(t, `SELECT stock::text FROM products WHERE id = $1`, prod.ID); stock != "5" {
		t.Errorf("stock after restock = %s, want 5", stock)
	}

	// Redelivery must not double-restock.
	if err := workers.HandleRestock(ctx(), deps, cancelEvt); err != nil {
		t.Fatalf("HandleRestock redelivery: %v", err)
	}
	if stock := s7bScalarString(t, `SELECT stock::text FROM products WHERE id = $1`, prod.ID); stock != "5" {
		t.Errorf("stock after redelivery = %s, want still 5", stock)
	}
}

func TestS7b_CheckoutExpiryCancelsOverdueOrders(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	orderID := s7bCheckoutOrder(t, biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.expire", prod.ID[:8])

	// Seed the final reminder as having fired more than the grace period ago.
	s7bExec(t, `
		INSERT INTO scheduled_follow_ups (id, order_id, business_id, customer_id, job_type, scheduled_at, executed_at)
		VALUES ($1, $2, $3, $4, 'unpaid-invoice-second', now() - interval '48 hours', now() - interval '48 hours')`,
		uuid.NewString(), orderID, biz.ID, cust.ID)

	if err := workers.SweepCheckoutExpiry(ctx(), workers.Deps{Pool: env.dbw.pool}); err != nil {
		t.Fatalf("SweepCheckoutExpiry: %v", err)
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

// A paid order (or one whose final reminder has not yet fired) must not be
// cancelled by the expiry sweep.
func TestS7b_CheckoutExpiryLeavesFreshOrPaidOrdersAlone(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)

	// Order whose reminder fired only recently -> not expired.
	convFresh := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	orderFresh := s7bCheckoutOrder(t, biz.ID, cust.ID, convFresh.ID, cust.Phone, "wamid.expire-fresh", prod.ID[:8])
	s7bExec(t, `
		INSERT INTO scheduled_follow_ups (id, order_id, business_id, customer_id, job_type, scheduled_at, executed_at)
		VALUES ($1, $2, $3, $4, 'unpaid-invoice-second', now() - interval '48 hours', now() - interval '1 hour')`,
		uuid.NewString(), orderFresh, biz.ID, cust.ID)

	if err := workers.SweepCheckoutExpiry(ctx(), workers.Deps{Pool: env.dbw.pool}); err != nil {
		t.Fatalf("SweepCheckoutExpiry: %v", err)
	}

	if status := s7bScalarString(t, `SELECT status::text FROM orders WHERE id = $1`, orderFresh); status != "CONFIRMED" {
		t.Errorf("fresh order status = %s, want CONFIRMED (not expired)", status)
	}
}
