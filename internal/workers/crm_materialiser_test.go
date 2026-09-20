package workers_test

import (
	"fmt"
	"testing"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

func s7bScalarString(t *testing.T, query string, args ...any) string {
	t.Helper()
	var s string
	if err := env.dbw.db.QueryRow(query, args...).Scan(&s); err != nil {
		t.Fatalf("scalar query %q: %v", query, err)
	}
	return s
}

func s7bScalarInt(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := env.dbw.db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("scalar query %q: %v", query, err)
	}
	return n
}

// TestS7b_TwoTurnHappyPath ports the checkout half of
// TestT006_OrderHappyPathThroughOrchestrator: order 51.00 + item 25.50x2 +
// stock 5->3 + INVOICING. (Payment, stats and follow-ups are separate
// order.created consumers, not part of checkout.)
func TestS7b_TwoTurnHappyPath(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	// The orchestrator moves LEAD -> BROWSING -> CHECKOUT before an order is
	// confirmed; checkout accepts BROWSING or CHECKOUT.
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	shortID := prod.ID[:8]

	// Turn 1: inquiry (no order signals yet) -> CRM enrichment only.
	turn1 := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-happy-1")
	turn1.CRMSignals.DetectedPreferences = []string{"organic"}
	if err := workers.HandleCRMSignals(ctx(), env.deps, turn1); err != nil {
		t.Fatalf("turn 1: %v", err)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE conversation_id = $1`, conv.ID); got != 0 {
		t.Fatalf("inquiry turn created %d orders, want 0", got)
	}

	// Turn 2: confirmation with 2 x [short].
	s7bRunCheckout(t, s7bCheckoutJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-happy-2",
		[]workers.DetectedItem{{ProductID: shortID, Quantity: 2}}))
	// Enrichment still lands via CRM (delivery area + sentiment).
	turn2 := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-happy-2")
	turn2.CRMSignals.DeliveryArea = s7bPtr("Osu")
	turn2.CRMSignals.Sentiment = s7bPtr("positive")
	if err := workers.HandleCRMSignals(ctx(), env.deps, turn2); err != nil {
		t.Fatalf("turn 2: %v", err)
	}

	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)

	if total := s7bScalarString(t, `SELECT total_amount::text FROM orders WHERE id = $1`, orderID); total != "51.00" {
		t.Errorf("total_amount = %s, want 51.00", total)
	}
	if status := s7bScalarString(t, `SELECT status::text FROM orders WHERE id = $1`, orderID); status != "CONFIRMED" {
		t.Errorf("status = %s, want CONFIRMED", status)
	}
	if cur := s7bScalarString(t, `SELECT currency FROM orders WHERE id = $1`, orderID); cur != "GHS" {
		t.Errorf("currency = %s, want GHS (tenant-stamped)", cur)
	}
	if key := s7bScalarString(t, `SELECT COALESCE(idempotency_key,'') FROM orders WHERE id = $1`, orderID); key != "wamid.s7b-happy-2" {
		t.Errorf("idempotency_key = %q, want sourceMessageId", key)
	}

	items := s7bScalarInt(t,
		`SELECT COUNT(*) FROM order_items WHERE order_id = $1 AND product_id = $2 AND quantity = 2 AND unit_price::text = '25.50' AND product_name = $3`,
		orderID, prod.ID, prod.Name)
	if items != 1 {
		t.Errorf("ledger rows (qty 2 @ 25.50, name snapshotted) = %d, want 1", items)
	}

	if stock := s7bScalarString(t, `SELECT stock::text FROM products WHERE id = $1`, prod.ID); stock != "3" {
		t.Errorf("stock after decrement = %s, want 3 (5 - 2)", stock)
	}

	if state := s7bScalarString(t, `SELECT state::text FROM conversations WHERE id = $1`, conv.ID); state != "INVOICING" {
		t.Errorf("conversation state = %s, want INVOICING", state)
	}

	// The order.created event was emitted in the same transaction.
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND event_type = 'order.created'`, orderID); got != 1 {
		t.Errorf("domain_events order.created rows = %d, want 1", got)
	}
}

// TestS7b_DuplicateLineItemsAggregatedDecrement ports TestT007b: two ledger
// rows qty 1&2 BUT a single aggregate decrement of 3.
func TestS7b_DuplicateLineItemsAggregatedDecrement(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(10), harness.WithPrice("20.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	shortID := prod.ID[:8]

	s7bRunCheckout(t, s7bCheckoutJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-dup",
		[]workers.DetectedItem{
			{ProductID: shortID, Quantity: 1},
			{ProductID: shortID, Quantity: 2},
		}))

	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)

	rows := s7bScalarInt(t, `SELECT COUNT(*) FROM order_items WHERE order_id = $1 AND product_id = $2`, orderID, prod.ID)
	if rows != 2 {
		t.Errorf("ledger rows = %d, want 2 (one per detected entry)", rows)
	}
	qtyList := s7bScalarString(t,
		`SELECT COALESCE(string_agg(quantity::text, ',' ORDER BY quantity), '') FROM order_items WHERE order_id = $1 AND product_id = $2`,
		orderID, prod.ID)
	if qtyList != "1,2" {
		t.Errorf("per-row quantities = %q, want \"1,2\" (each duplicate kept verbatim)", qtyList)
	}
	if total := s7bScalarString(t, `SELECT total_amount::text FROM orders WHERE id = $1`, orderID); total != "60.00" {
		t.Errorf("total_amount = %s, want 60.00 (aggregated 3 x 20.00)", total)
	}
	if stock := s7bScalarString(t, `SELECT stock::text FROM products WHERE id = $1`, prod.ID); stock != "7" {
		t.Errorf("stock = %s, want 7 (single aggregate decrement of 3, not a double-decrement)", stock)
	}
}

func s7bExec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := env.dbw.db.Exec(query, args...); err != nil {
		t.Fatalf("fixture exec %q: %v", query, err)
	}
}

type s7bRegistrar struct {
	handlers map[string]queue.Handler
}

func (r *s7bRegistrar) Register(taskType string, h queue.Handler) {
	if r.handlers == nil {
		r.handlers = map[string]queue.Handler{}
	}
	r.handlers[taskType] = h
}

// TestS7b_RegisterCheckoutDispatches pins the checkout registration: TaskCheckout
// is wired and the asynq wrapper decodes CheckoutJob's wire keys before
// delegating to HandleCheckout.
func TestS7b_RegisterCheckoutDispatches(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(4), harness.WithPrice("12.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	reg := &s7bRegistrar{}
	workers.RegisterCheckout(reg, env.checkout)
	h, ok := reg.handlers[queue.TaskCheckout]
	if !ok {
		t.Fatalf("TaskCheckout not registered; got %v", reg.handlers)
	}

	payload := fmt.Sprintf(`{
		"businessId": %q, "customerId": %q, "conversationId": %q,
		"customerPhone": %q, "sourceMessageId": "wamid.s7b-reg",
		"detectedItems": [{"product_id": %q, "quantity": 2}]
	}`, biz.ID, cust.ID, conv.ID, cust.Phone, prod.ID[:8])

	if err := h(ctx(), []byte(payload)); err != nil {
		t.Fatalf("registered handler failed: %v", err)
	}
	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)
	if total := s7bScalarString(t, `SELECT total_amount::text FROM orders WHERE id = $1`, orderID); total != "25.00" {
		t.Errorf("total_amount = %s, want 25.00 (2 x 12.50 via wire-format payload)", total)
	}
}
