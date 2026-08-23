package workers_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

// TestS7b_InsufficientStockRollbackZeroRows ports TestT007c's materialiser
// core: ZERO order rows, stock intact, nil error (warn + swallow).
func TestS7b_InsufficientStockRollbackZeroRows(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(1), harness.WithPrice("30.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))
	shortID := prod.ID[:8]

	job := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-short")
	job.CRMSignals.OrderConfirmed = true
	job.CRMSignals.DetectedItems = []workers.DetectedItem{{ProductID: shortID, Quantity: 5}}

	if err := workers.HandleCRMSignals(ctx(), env.deps, job); err != nil {
		t.Fatalf("insufficient stock must be swallowed (job completes), got error: %v", err)
	}

	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE conversation_id = $1`, conv.ID); got != 0 {
		t.Errorf("orders = %d, want 0 (transaction rolled back)", got)
	}
	if got := s7bScalarInt(t,
		`SELECT COUNT(*) FROM order_items oi JOIN orders o ON o.id = oi.order_id WHERE o.conversation_id = $1`, conv.ID); got != 0 {
		t.Errorf("order_items = %d, want 0", got)
	}
	if stock := s7bScalarString(t, `SELECT stock::text FROM products WHERE id = $1`, prod.ID); stock != "1" {
		t.Errorf("stock = %s, want intact 1", stock)
	}
	if got := s7bScalarInt(t,
		`SELECT COUNT(*) FROM scheduled_follow_ups sfu JOIN orders o ON o.id = sfu.order_id WHERE o.conversation_id = $1`, conv.ID); got != 0 {
		t.Errorf("follow-ups scheduled after rollback = %d, want 0", got)
	}
}

// TestS7b_DuplicateSourceMessageIdSkipped ports TestT008's unique-index
// backstop through the handler: a repeated sourceMessageId is a warn+return.
func TestS7b_DuplicateSourceMessageIdSkipped(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))
	shortID := prod.ID[:8]

	s7bExec(t, `INSERT INTO orders
		(id, business_id, customer_id, status, total_amount, currency, idempotency_key, updated_at)
		VALUES ('ord_s7b_dup', $1, $2, 'PENDING', 10.00, 'GHS', 'wamid.s7b-repeat', NOW())`,
		biz.ID, cust.ID)

	job := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-repeat")
	job.CRMSignals.OrderConfirmed = true
	job.CRMSignals.DetectedItems = []workers.DetectedItem{{ProductID: shortID, Quantity: 2}}

	if err := workers.HandleCRMSignals(ctx(), env.deps, job); err != nil {
		t.Fatalf("duplicate sourceMessageId must skip (warn+nil), got error: %v", err)
	}

	got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE idempotency_key = 'wamid.s7b-repeat'`)
	if got != 1 {
		t.Errorf("orders with duplicated key = %d, want exactly the pre-existing 1", got)
	}
	if stock := s7bScalarString(t, `SELECT stock::text FROM products WHERE id = $1`, prod.ID); stock != "5" {
		t.Errorf("stock = %s, want untouched 5", stock)
	}
	if stats := s7bScalarInt(t, `SELECT total_orders FROM customer_profiles WHERE customer_id = $1`, cust.ID); stats != 0 {
		t.Errorf("total_orders = %d, want 0 (skip happens before profile stats)", stats)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, conv.ID); got != 0 {
		t.Errorf("outbound messages = %d, want 0 (no invoice on duplicate)", got)
	}
	if got := s7bScalarInt(t,
		`SELECT COUNT(*) FROM scheduled_follow_ups sfu JOIN orders o ON o.id = sfu.order_id WHERE o.conversation_id = $1`, conv.ID); got != 0 {
		t.Errorf("follow-ups = %d, want 0 (no scheduling on duplicate)", got)
	}
}

// TestS7b_ProfileBuilderSignals ports profile-builder.handler.spec.ts
// expectations: case-insensitive preference dedup capped at 50, delivery-area
// latest-wins, sentiment change-only writes, and the defensive bare-profile
// create when the row is missing (updates skipped that turn).
func TestS7b_ProfileBuilderSignals(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))

	newJob := func(prefs []string, area string, sentiment string) workers.CRMSignalJob {
		job := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-pb")
		job.CRMSignals.DetectedPreferences = prefs
		if area != "" {
			job.CRMSignals.DeliveryArea = s7bPtr(area)
		}
		job.CRMSignals.Sentiment = s7bPtr(sentiment)
		return job
	}
	prefsJSON := func() string {
		return s7bScalarString(t, `SELECT preferences::text FROM customer_profiles WHERE customer_id = $1`, cust.ID)
	}
	prefsEqual := func(want string) bool { // jsonb-normalized comparison (spacing-insensitive)
		var eq bool
		if err := env.dbw.db.QueryRow(
			`SELECT preferences = $2::jsonb FROM customer_profiles WHERE customer_id = $1`, cust.ID, want).Scan(&eq); err != nil {
			t.Fatalf("compare preferences: %v", err)
		}
		return eq
	}

	// Case-insensitive dedup keeps the FIRST casing.
	first := newJob([]string{"Organic", "organic", "SPICY"}, "", "positive")
	if err := workers.HandleCRMSignals(ctx(), env.deps, first); err != nil {
		t.Fatalf("profile turn 1: %v", err)
	}
	if !prefsEqual(`["Organic","SPICY"]`) {
		t.Errorf("preferences = %s, want [\"Organic\",\"SPICY\"]", prefsJSON())
	}

	// Delivery area latest-wins.
	areaA := newJob(nil, "Osu", "positive")
	if err := workers.HandleCRMSignals(ctx(), env.deps, areaA); err != nil {
		t.Fatalf("area turn A: %v", err)
	}
	areaB := newJob(nil, "Tema ", "positive") // trailing space must be trimmed
	if err := workers.HandleCRMSignals(ctx(), env.deps, areaB); err != nil {
		t.Fatalf("area turn B: %v", err)
	}
	if got := s7bScalarString(t, `SELECT delivery_area FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != "Tema" {
		t.Errorf("delivery_area = %q, want latest-wins trimmed \"Tema\"", got)
	}

	// Sentiment change-only write: same value must not touch updated_at.
	before := s7bScalarString(t, `SELECT updated_at::text FROM customer_profiles WHERE customer_id = $1`, cust.ID)
	time.Sleep(50 * time.Millisecond)
	sameSentiment := newJob(nil, "", "positive")
	if err := workers.HandleCRMSignals(ctx(), env.deps, sameSentiment); err != nil {
		t.Fatalf("same-sentiment turn: %v", err)
	}
	if after := s7bScalarString(t, `SELECT updated_at::text FROM customer_profiles WHERE customer_id = $1`, cust.ID); after != before {
		t.Errorf("unchanged sentiment still wrote updated_at (%s -> %s); change-only contract broken", before, after)
	}
	negTurn := newJob(nil, "", "negative")
	if err := workers.HandleCRMSignals(ctx(), env.deps, negTurn); err != nil {
		t.Fatalf("negative sentiment turn: %v", err)
	}
	if got := s7bScalarString(t, `SELECT sentiment FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != "negative" {
		t.Errorf("sentiment = %q, want \"negative\" after change", got)
	}

	// Cap at 50: 49 existing + 3 candidates where one dedups -> merged sliced
	// to exactly 50, keeping earliest entries first.
	existing := make([]string, 49)
	for i := range existing {
		existing[i] = "pref-" + strings.Repeat("a", i+1)
	}
	s7bExec(t,
		`UPDATE customer_profiles SET preferences = $2::jsonb WHERE customer_id = $1`,
		cust.ID, mustJSON(t, existing))
	capped := newJob([]string{"PREF-AAAA", "fresh-1", "fresh-2"}, "", "negative")
	if err := workers.HandleCRMSignals(ctx(), env.deps, capped); err != nil {
		t.Fatalf("capped turn: %v", err)
	}
	count := s7bScalarInt(t, `SELECT jsonb_array_length(preferences) FROM customer_profiles WHERE customer_id = $1`, cust.ID)
	if count != 50 {
		t.Fatalf("preferences length = %d, want capped 50", count)
	}
	last := s7bScalarString(t,
		`SELECT preferences->49 FROM customer_profiles WHERE customer_id = $1`, cust.ID)
	if last != `"fresh-1"` {
		t.Errorf("element 49 = %s, want \"fresh-1\" (the first new pref that fit before the cap)", last)
	}

	// Missing-profile defensive create: bare profile, updates skipped THIS
	// turn (profile-builder.handler.ts:36-43), applied on the next turn.
	s7bExec(t, `DELETE FROM customer_profiles WHERE customer_id = $1`, cust.ID)
	orphan := newJob([]string{"x"}, "", "neutral")
	if err := workers.HandleCRMSignals(ctx(), env.deps, orphan); err != nil {
		t.Fatalf("defensive-create turn: %v", err)
	}
	if !prefsEqual(`[]`) {
		t.Errorf("preferences right after defensive create = %s, want bare [] (updates skipped this turn)", prefsJSON())
	}
	if got := s7bScalarString(t, `SELECT COALESCE(delivery_area,'') FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != "" {
		t.Errorf("delivery_area right after defensive create = %q, want empty", got)
	}
	retry := newJob([]string{"x"}, "", "neutral")
	if err := workers.HandleCRMSignals(ctx(), env.deps, retry); err != nil {
		t.Fatalf("post-create retry turn: %v", err)
	}
	if !prefsEqual(`["x"]`) {
		t.Errorf("preferences after retry = %s, want [\"x\"]", prefsJSON())
	}
}

// TestS7b_CustomerNameUpdate ports customer-capture.updateCustomerName:
// longest-wins strategy with trimming and blank-input no-ops.
func TestS7b_CustomerNameUpdate(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))

	nameInDB := func() string {
		var name *string
		if err := env.dbw.db.QueryRow(`SELECT name FROM customers WHERE id = $1`, cust.ID).Scan(&name); err != nil {
			t.Fatalf("read name: %v", err)
		}
		if name == nil {
			return ""
		}
		return *name
	}

	run := func(name string) {
		t.Helper()
		if err := workers.HandleCRMSignals(ctx(), env.deps, withCustomerName(biz.ID, cust.ID, conv.ID, cust.Phone, name)); err != nil {
			t.Fatalf("name update %q: %v", name, err)
		}
	}

	// Start from a NULL name: the fixture default is longer than every probe.
	s7bExec(t, `UPDATE customers SET name = NULL WHERE id = $1`, cust.ID)

	// NULL current + meaningful extraction -> stored trimmed.
	run("  Ama Mensah  ")
	if got := nameInDB(); got != "Ama Mensah" {
		t.Errorf("name = %q, want trimmed \"Ama Mensah\"", got)
	}

	// Shorter extraction never overwrites a longer stored name.
	run("Ama")
	if got := nameInDB(); got != "Ama Mensah" {
		t.Errorf("name = %q, longest-wins must keep \"Ama Mensah\"", got)
	}

	// Equal-after-trim does not rewrite.
	run(" Ama Mensah ")
	if got := nameInDB(); got != "Ama Mensah" {
		t.Errorf("name = %q, equal-length extraction must not change it", got)
	}

	// Blank/whitespace input is ignored entirely.
	run("   ")
	if got := nameInDB(); got != "Ama Mensah" {
		t.Errorf("name = %q, blank input must be a no-op", got)
	}
}

// TestS7b_FollowUpOffsetsPrecision pins ScheduledFollowUp.createMany offsets:
// abandoned-cart at +2h, unpaid-invoice-first at +24h (order-ledger.handler.ts:352, :359).
func TestS7b_FollowUpOffsetsPrecision(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("10.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))

	job := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-fu")
	job.CRMSignals.OrderConfirmed = true
	job.CRMSignals.DetectedItems = []workers.DetectedItem{{ProductID: prod.ID[:8], Quantity: 1}}

	started := time.Now().UTC().Add(-5 * time.Second)
	finished := time.Now().UTC().Add(5 * time.Second)
	if err := workers.HandleCRMSignals(ctx(), env.deps, job); err != nil {
		t.Fatalf("handle: %v", err)
	}
	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)

	assertOffset := func(jobType string, offset time.Duration) {
		t.Helper()
		var scheduled time.Time
		if err := env.dbw.db.QueryRow(
			`SELECT scheduled_at FROM scheduled_follow_ups WHERE order_id = $1 AND job_type = $2`,
			orderID, jobType).Scan(&scheduled); err != nil {
			t.Fatalf("follow-up %s missing: %v", jobType, err)
		}
		low := started.Add(offset).Add(-time.Minute)
		high := finished.Add(offset).Add(time.Minute)
		if scheduled.Before(low) || scheduled.After(high) {
			t.Errorf("%s scheduled_at = %s, want ~now+%s (window %s..%s)",
				jobType, scheduled.Format(time.RFC3339), offset, low.Format(time.RFC3339), high.Format(time.RFC3339))
		}
	}
	assertOffset("abandoned-cart", 2*time.Hour)
	assertOffset("unpaid-invoice-first", 24*time.Hour)
}

// TestS7b_PaymentCopyPerCurrencyAndBrandFree checks getCurrencyConfig-driven
// invoice copy: market-specific methods list, GHS fallback for unknown codes,
// and never the provider brand in prose (currency.config.ts:6-16).
func TestS7b_PaymentCopyPerCurrencyAndBrandFree(t *testing.T) {
	if got := workers.GetCurrencyConfig("GHS").PaymentMethods; got != "Mobile Money, card, or bank transfer" {
		t.Errorf("GHS methods = %q, want verbatim characterization copy", got)
	}
	if got := workers.GetCurrencyConfig("NGN").PaymentMethods; got != "card, bank transfer, or USSD" {
		t.Errorf("NGN methods = %q, want market-specific list", got)
	}
	if got := workers.GetCurrencyConfig("USD").PaymentMethods; got != "card" {
		t.Errorf("USD methods = %q, want card-only list", got)
	}
	if got := workers.GetCurrencyConfig("XYZ").PaymentMethods; got != "Mobile Money, card, or bank transfer" {
		t.Errorf("unknown-currency methods = %q, want GHS fallback", got)
	}
	if got := workers.GetCurrencyConfig("").Symbol; got != "GH₵" {
		t.Errorf("empty-currency symbol = %q, want GH₵ fallback", got)
	}

	// DB-driven prose check on an NGN order.
	env = s7bNewEnv(t)
	biz := env.dbw.factory.Business()
	s7bExec(t, `UPDATE businesses SET currency = 'NGN' WHERE id = $1`, biz.ID)
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("40.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))

	job := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-ngn")
	job.CRMSignals.OrderConfirmed = true
	job.CRMSignals.DetectedItems = []workers.DetectedItem{{ProductID: prod.ID[:8], Quantity: 1}}
	if err := workers.HandleCRMSignals(ctx(), env.deps, job); err != nil {
		t.Fatalf("handle NGN order: %v", err)
	}

	text := s7bScalarString(t,
		`SELECT text_content FROM outbound_messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1`,
		conv.ID)
	urlIdx := strings.Index(text, "https://checkout.paystack.test")
	if urlIdx < 0 {
		t.Fatalf("invoice text missing payment URL:\n%s", text)
	}
	prose := text[:urlIdx]
	if !strings.Contains(text, "*NGN 40.00*") {
		t.Errorf("invoice total copy missing *NGN 40.00*:\n%s", text)
	}
	if !strings.Contains(prose, "card, bank transfer, or USSD") {
		t.Errorf("prose missing NGN payment methods:\n%s", prose)
	}
	if strings.Contains(strings.ToLower(prose), "paystack") {
		t.Errorf("brand name leaked into invoice prose:\n%s", prose)
	}
	if !strings.HasPrefix(text, "Thank you for confirming your order!") {
		t.Errorf("paymentText template prefix drifted:\n%s", text)
	}
}

// TestS7b_PaystackFailureLeavesOrderIntact: initiatePayment failure means NO
// invoice message but the order itself stays CONFIRMED (order-ledger.handler.ts:261-267).
func TestS7b_PaystackFailureLeavesOrderIntact(t *testing.T) {
	env = s7bNewEnv(t)
	env.paystack.fail = true

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))

	job := s7bBaseJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-payfail")
	job.CRMSignals.OrderConfirmed = true
	job.CRMSignals.DetectedItems = []workers.DetectedItem{{ProductID: prod.ID[:8], Quantity: 1}}
	if err := workers.HandleCRMSignals(ctx(), env.deps, job); err != nil {
		t.Fatalf("paystack failure must not fail the job: %v", err)
	}

	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE conversation_id = $1`, conv.ID); got != 1 {
		t.Errorf("orders = %d, want 1 despite failed payment initiation", got)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, conv.ID); got != 0 {
		t.Errorf("outbound messages = %d, want 0 when payment URL is empty", got)
	}
	if state := s7bScalarString(t, `SELECT state::text FROM conversations WHERE id = $1`, conv.ID); state == "INVOICING" {
		t.Error("conversation must not reach INVOICING without a payment link")
	}
	if amount := s7bScalarString(t,
		`SELECT total_amount::text FROM orders WHERE conversation_id = $1`, conv.ID); amount != "25.50" {
		t.Errorf("total = %s, want 25.50", amount)
	}
	if !decimalRequireEqual(env.paystack.snapshot()[0].Amount, decimal.RequireFromString("25.50")) {
		t.Errorf("paystack amount = %s, want 25.50", env.paystack.snapshot()[0].Amount.String())
	}
}

// ---------------------------------------------------------------------------
// tiny local helpers
// ---------------------------------------------------------------------------

func withCustomerName(bizID, custID, convID, phone, name string) workers.CRMSignalJob {
	job := s7bBaseJob(bizID, custID, convID, phone, "wamid.s7b-name")
	job.CRMSignals.CustomerName = s7bPtr(name)
	return job
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture json: %v", err)
	}
	return string(b)
}

func decimalRequireEqual(a decimal.Decimal, b decimal.Decimal) bool {
	return a.Equal(b)
}
