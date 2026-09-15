package workers_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

// TestS7b_InsufficientStockRollbackZeroRows ports TestT007c's checkout core:
// ZERO order rows, stock intact, nil error (warn + swallow).
func TestS7b_InsufficientStockRollbackZeroRows(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(1), harness.WithPrice("30.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	shortID := prod.ID[:8]

	s7bRunCheckout(t, s7bCheckoutJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-short",
		[]workers.DetectedItem{{ProductID: shortID, Quantity: 5}}))

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
	if state := s7bScalarString(t, `SELECT state::text FROM conversations WHERE id = $1`, conv.ID); state != "CHECKOUT" {
		t.Errorf("state = %s, want CHECKOUT (rollback leaves the order slot unclaimed)", state)
	}
}

// TestS7b_DuplicateSourceMessageIdSkipped ports TestT008's unique-index
// backstop through checkout: a repeated sourceMessageId is a warn+skip.
func TestS7b_DuplicateSourceMessageIdSkipped(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	shortID := prod.ID[:8]

	s7bExec(t, `INSERT INTO orders
		(id, business_id, customer_id, status, total_amount, currency, idempotency_key, updated_at)
		VALUES ('ord_s7b_dup', $1, $2, 'PENDING', 10.00, 'GHS', 'wamid.s7b-repeat', NOW())`,
		biz.ID, cust.ID)

	s7bRunCheckout(t, s7bCheckoutJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.s7b-repeat",
		[]workers.DetectedItem{{ProductID: shortID, Quantity: 2}}))

	got := s7bScalarInt(t, `SELECT COUNT(*) FROM orders WHERE idempotency_key = 'wamid.s7b-repeat'`)
	if got != 1 {
		t.Errorf("orders with duplicated key = %d, want exactly the pre-existing 1", got)
	}
	if stock := s7bScalarString(t, `SELECT stock::text FROM products WHERE id = $1`, prod.ID); stock != "5" {
		t.Errorf("stock = %s, want untouched 5", stock)
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

// TestS7b_CurrencyConfig pins the currency display config (currency.config.ts),
// used by the invoice copy path. Brand names never appear in prose.
func TestS7b_CurrencyConfig(t *testing.T) {
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
