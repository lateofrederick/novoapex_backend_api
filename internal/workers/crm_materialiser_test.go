package workers_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/shopspring/decimal"

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

// TestS7b_TwoTurnHappyPath ports the materialiser half of
// TestT006_OrderHappyPathThroughOrchestrator: Order 51.00 + item 25.50x2 +
// stock 5->3 + INVOICING + outbound containing the stub URL and GHS copy.
func TestS7b_TwoTurnHappyPath(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	// Turn 1's inquiry would have moved the conversation LEAD -> BROWSING via
	// the orchestrator (not this worker); start there for the materialiser half.
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("BROWSING"))
	shortID := prod.ID[:8]

	// Turn 1: inquiry (no order signals yet).
	turn1 := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-happy-1")
	turn1.CRMSignals.DetectedPreferences = []string{"organic"}
	if err := workers.HandleCRMSignals(ctx(), env.deps, turn1); err != nil {
		t.Fatalf("turn 1: %v", err)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE conversation_id = $1`, conv.ID); got != 0 {
		t.Fatalf("inquiry turn created %d orders, want 0", got)
	}

	// Turn 2: confirmation with 2 x [short].
	turn2 := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-happy-2")
	turn2.CRMSignals.OrderConfirmed = true
	turn2.CRMSignals.DetectedItems = []workers.DetectedItem{{ProductID: shortID, Quantity: 2}}
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

	text := s7bScalarString(t,
		`SELECT text_content FROM outbound_messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1`,
		conv.ID)
	if !strings.Contains(text, "checkout.paystack.test") || !strings.Contains(text, "GHS 51.00") {
		t.Errorf("payment message missing stub URL/total:\n%s", text)
	}

	var rawPayload map[string]any
	raw := s7bScalarString(t,
		`SELECT raw_payload::text FROM outbound_messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1`,
		conv.ID)
	if err := json.Unmarshal([]byte(raw), &rawPayload); err != nil {
		t.Fatalf("raw_payload not valid JSON: %v", err)
	}
	if rawPayload["messaging_product"] != "whatsapp" || rawPayload["to"] != cust.Phone {
		t.Errorf("raw_payload = %v, want whatsapp payload to %s", rawPayload, cust.Phone)
	}

	jobs := env.publisher.snapshot()
	if len(jobs) != 1 {
		t.Fatalf("enqueued jobs = %d, want 1 (TaskOutboundSend)", len(jobs))
	}
	outboundID := s7bScalarString(t,
		`SELECT id FROM outbound_messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1`, conv.ID)
	if jobs[0].Queue != queue.QOutbound || jobs[0].TaskType != queue.TaskOutboundSend {
		t.Errorf("enqueue target = %s/%s, want %s/%s", jobs[0].Queue, jobs[0].TaskType, queue.QOutbound, queue.TaskOutboundSend)
	}
	if jobs[0].Payload["outboundMessageId"] != outboundID {
		t.Errorf("outboundMessageId = %v, want persisted row id %s", jobs[0].Payload["outboundMessageId"], outboundID)
	}

	if got := s7bScalarInt(t, `SELECT total_orders FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != 1 {
		t.Errorf("total_orders = %d, want 1", got)
	}
	if got := s7bScalarString(t, `SELECT total_spent::text FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != "51.00" {
		t.Errorf("total_spent = %s, want 51.00", got)
	}
	if got := s7bScalarString(t, `SELECT average_order_value::text FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != "51.00" {
		t.Errorf("average_order_value = %s, want 51.00", got)
	}
	var freq sql.NullFloat64
	if err := env.dbw.db.QueryRow(`SELECT order_frequency_days FROM customer_profiles WHERE customer_id = $1`, cust.ID).Scan(&freq); err != nil {
		t.Fatalf("read frequency: %v", err)
	}
	if freq.Valid {
		t.Errorf("order_frequency_days = %v on first order, want NULL", freq.Float64)
	}

	reqs := env.paystack.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("paystack calls = %d, want 1", len(reqs))
	}
	if !reqs[0].Amount.Equal(decimal.RequireFromString("51.00")) {
		t.Errorf("payment amount = %s, want 51.00 major units", reqs[0].Amount.String())
	}
	if reqs[0].Reference != orderID || reqs[0].Currency != "GHS" || reqs[0].BusinessID != biz.ID {
		t.Errorf("payment request reference/currency/businessId = %s/%s/%s, want %s/GHS/%s",
			reqs[0].Reference, reqs[0].Currency, reqs[0].BusinessID, orderID, biz.ID)
	}
	if reqs[0].CallbackURL != "https://novoapex.com/success" {
		t.Errorf("callback_url = %q, want defaulted https://novoapex.com/success", reqs[0].CallbackURL)
	}
	if reqs[0].CustomerPhone != cust.Phone {
		t.Errorf("customer_phone = %q, want %q", reqs[0].CustomerPhone, cust.Phone)
	}

	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM scheduled_follow_ups WHERE order_id = $1`, orderID); got != 2 {
		t.Errorf("scheduled follow-ups = %d, want 2", got)
	}
}

// TestS7b_DuplicateLineItemsAggregatedDecrement ports TestT007b: two ledger
// rows qty 1&2 BUT a single aggregate decrement of 3.
func TestS7b_DuplicateLineItemsAggregatedDecrement(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(10), harness.WithPrice("20.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))
	shortID := prod.ID[:8]

	job := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-dup")
	job.CRMSignals.OrderConfirmed = true
	job.CRMSignals.DetectedItems = []workers.DetectedItem{
		{ProductID: shortID, Quantity: 1},
		{ProductID: shortID, Quantity: 2},
	}
	if err := workers.HandleCRMSignals(ctx(), env.deps, job); err != nil {
		t.Fatalf("handle: %v", err)
	}

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

// TestS7b_RegisterCRMDispatches pins T7.9 registration: TaskCRMProcess is
// wired and the asynq wrapper decodes CrmSignalJobData's wire keys before
// delegating to HandleCRMSignals.
func TestS7b_RegisterCRMDispatches(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(4), harness.WithPrice("12.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("BROWSING"))

	reg := &s7bRegistrar{}
	workers.RegisterCRM(reg, env.deps)
	h, ok := reg.handlers["crm-materialiser:process-crm-signals"]
	if !ok {
		t.Fatalf("TaskCRMProcess not registered; got %v", reg.handlers)
	}

	payload := fmt.Sprintf(`{
		"businessId": %q, "customerId": %q, "conversationId": %q,
		"customerPhone": %q, "sourceMessageId": "wamid.s7b-reg",
		"crmSignals": {"order_confirmed": true,
			"detected_items": [{"product_id": %q, "quantity": 2}],
			"delivery_area": null, "customer_name": null,
			"detected_preferences": [], "sentiment": "neutral"},
		"llmIntent": "checkout_request", "referencedProductIds": [%q],
		"timestamp": "2026-08-22T00:00:00Z"
	}`, biz.ID, cust.ID, conv.ID, cust.Phone, prod.ID[:8], prod.ID[:8])

	if err := h(ctx(), []byte(payload)); err != nil {
		t.Fatalf("registered handler failed: %v", err)
	}
	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)
	if total := s7bScalarString(t, `SELECT total_amount::text FROM orders WHERE id = $1`, orderID); total != "25.00" {
		t.Errorf("total_amount = %s, want 25.00 (2 x 12.50 via wire-format payload)", total)
	}
}
