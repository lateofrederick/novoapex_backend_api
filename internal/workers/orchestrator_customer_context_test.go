package workers_test

// orchestrator_customer_context_test.go covers the customer-in-prompt
// personalisation port (this session's Node work: buildCustomerContext /
// getFavoriteProduct in conversation-orchestrator.service.ts), including the
// SystemPrompt() wiring fix — orchGenerateAndHandleLlmResponse now actually
// renders the full system prompt instead of a 3-line stub, so this is also
// the regression test for that wiring.

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
)

// businessContextPortion isolates ${businessContext} — everything after
// "Current Conversation State: ..." in the rendered prompt. The static
// CUSTOMER CONTEXT RULES section (always present) quotes the literal
// "=== CUSTOMER CONTEXT ===" marker as an example, so presence checks must
// only look here, where the dynamic per-customer block actually lands.
func businessContextPortion(system string) string {
	const marker = "Current Conversation State:"
	if i := strings.Index(system, marker); i >= 0 {
		return system[i+len(marker):]
	}
	return system
}

func TestOpipeCustomerContext_NewLeadOmitsSection(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := "+233200099001"

	e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-cc-1", "hello")
	opipeRun(t, e, biz, sender)

	if e.llm.callCount() != 1 {
		t.Fatalf("llm calls = %d, want 1", e.llm.callCount())
	}
	system := e.llm.calls[0].System
	if strings.Contains(businessContextPortion(system), "=== CUSTOMER CONTEXT ===") {
		t.Errorf("brand-new lead must not get a CUSTOMER CONTEXT section:\n%s", system)
	}
	// Regression guard for the SystemPrompt() wiring: the full prompt (not the
	// old 3-line stub) must actually reach the LLM.
	if !strings.Contains(system, orchestrator.SectionFulfillmentRules) {
		t.Error("system prompt missing FULFILLMENT RULES — SystemPrompt() not wired into orchGenerateAndHandleLlmResponse")
	}
}

func TestOpipeCustomerContext_NamedLeadGetsNameOnly(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	cust := e.factory.Customer(biz.ID)
	if _, err := e.db.Exec(`UPDATE customers SET name = 'Kwame' WHERE id = $1`, cust.ID); err != nil {
		t.Fatalf("set name: %v", err)
	}

	e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, cust.Phone, "wamid-cc-2", "hello")
	opipeRun(t, e, biz, cust.Phone)

	system := e.llm.calls[e.llm.callCount()-1].System
	if !strings.Contains(businessContextPortion(system), "=== CUSTOMER CONTEXT ===") {
		t.Fatalf("expected a CUSTOMER CONTEXT section for a named lead:\n%s", system)
	}
	if !strings.Contains(system, "Name: Kwame.") {
		t.Errorf("missing name line:\n%s", system)
	}
	if strings.Contains(system, "Returning customer") {
		t.Errorf("a lead with zero orders must not get the returning-customer line:\n%s", system)
	}
}

func TestOpipeCustomerContext_ReturningCustomerFullBlock(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	cust := e.factory.Customer(biz.ID)
	prod := e.factory.Product(biz.ID)

	if _, err := e.db.Exec(`UPDATE customers SET name = 'Ama' WHERE id = $1`, cust.ID); err != nil {
		t.Fatalf("set name: %v", err)
	}
	if _, err := e.db.Exec(
		`UPDATE customer_profiles SET total_orders = 3, delivery_area = 'Accra Central' WHERE customer_id = $1`,
		cust.ID); err != nil {
		t.Fatalf("set profile stats: %v", err)
	}
	// Order history for the favorite-product query — two orders, product
	// bought 5 total units.
	orderID1, orderID2 := uuid.NewString(), uuid.NewString()
	for _, o := range []struct {
		id  string
		qty int
	}{{orderID1, 3}, {orderID2, 2}} {
		if _, err := e.db.Exec(`
			INSERT INTO orders (id, business_id, customer_id, status, total_amount, currency, updated_at)
			VALUES ($1, $2, $3, 'PAID', 10.00, 'GHS', NOW())`,
			o.id, biz.ID, cust.ID); err != nil {
			t.Fatalf("seed order: %v", err)
		}
		if _, err := e.db.Exec(`
			INSERT INTO order_items (id, order_id, product_id, product_name, quantity, unit_price)
			VALUES ($1, $2, $3, $4, $5, 10.00)`,
			uuid.NewString(), o.id, prod.ID, prod.Name, o.qty); err != nil {
			t.Fatalf("seed order item: %v", err)
		}
	}

	e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, cust.Phone, "wamid-cc-3", "hello")
	opipeRun(t, e, biz, cust.Phone)

	system := e.llm.calls[e.llm.callCount()-1].System
	if !strings.Contains(system, "Name: Ama.") {
		t.Errorf("missing name line:\n%s", system)
	}
	if !strings.Contains(system, "Returning customer — 3 previous order(s).") {
		t.Errorf("missing returning-customer line:\n%s", system)
	}
	if !strings.Contains(system, "Most frequently ordered: "+prod.Name+".") {
		t.Errorf("missing favorite-product line:\n%s", system)
	}
	if !strings.Contains(system, "Known delivery area: Accra Central.") {
		t.Errorf("missing delivery-area line:\n%s", system)
	}
	if !strings.Contains(system, orchestrator.SectionCustomerContextRules) {
		t.Error("system prompt missing CUSTOMER CONTEXT RULES section")
	}
}
