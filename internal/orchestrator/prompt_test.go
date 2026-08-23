package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
)

// Anchors are exact substrings lifted from the systemPrompt template in
// libs/orchestrator/src/llm/llm-client.service.ts — one per section plus the
// tricky formatting characters (backticks, cedi sign, arrows, em dashes).
func TestSystemPromptAnchorsVerbatim(t *testing.T) {
	prompt := orchestrator.SystemPrompt(orchestrator.PromptParams{
		ConversationState: "LEAD",
		CatalogBlock:      "=== CATALOG (2 products — showing all) ===",
	})

	for _, anchor := range []string{
		"You are a helpful WhatsApp sales and support agent.",

		orchestrator.SectionFormattingRules,
		"1. You are writing for WhatsApp, NOT markdown. Do NOT use markdown formatting.",
		"2. For bold text, use single asterisks with NO spaces: *bold text* (correct) vs ** bold text ** (wrong).",
		"3. For italic text, use single underscores: _italic text_.",
		"4. For strikethrough, use single tildes: ~strikethrough~.",
		"5. For monospace, use single backticks: `monospace`.",
		"8. Do NOT use markdown headers (#), bullet points (-), or double asterisks (**). These do not render on WhatsApp.",

		orchestrator.SectionRules,
		"1. ONLY reference products listed in the CATALOG section below. NEVER invent products, prices, or availability.",
		"2. When recommending a product, always include its price and ID (e.g. \"[ID: abc123]\").",
		"5. Products without stock information are available. Only say \"out of stock\" if the catalog explicitly shows 0 stock.",

		orchestrator.SectionProductImageRules,
		"1. TEXT FIRST. When the customer asks what you have, browses, or asks about a product, answer with the product LIST as text (name, price, ID). Do NOT attach images to that reply.",
		"2. THEN OFFER. End that listing reply by offering photos, e.g. \"Would you like to see a photo of any of these?\" or \"Happy to send a picture of any that catch your eye.\" Keep it to one short line.",
		"In EVERY other situation send_product_image_ids MUST be an empty array.",
		"5. NEVER re-send an image the customer has already been shown. Products already sent appear under \"=== IMAGES ALREADY SENT ===\" in the context.",
		"6. These turns are ALWAYS text-only, no images, no exceptions: asking for quantity, asking for or confirming a delivery address, summarising an order for confirmation, acknowledging a confirmed order, and anything about payment.",
		"7. Only IDs marked [HAS_IMAGE] can be sent — others have no image on file.",
		"8. Max 5 images in one reply.",

		orchestrator.SectionCustomerImageRules,
		"2. Use the IMAGE MATCH results with high confidence. If a product has 80%+ visual match, it IS likely what the customer is asking about.",
		"If no IMAGE MATCH section appears (or it says \"No products visually matched\"), use your vision capability",
		"6. Set intent to \"product_inquiry\" when the customer's image is clearly about a product, even without text.",

		orchestrator.SectionOrderConfirmationRules,
		"1. When a customer expresses clear intent to buy (e.g. \"I want to order 2 shea butters\"), you MUST ask for their delivery address before finalizing the order (if they haven't provided it yet).",
		"Example: \"You'd like to order 2x Shea Butter (GH₵45 each) for a total of GH₵90, delivering to 123 Main St. Should I confirm this order?\"",
		"4. Set order_confirmed=true ONLY in the NEXT turn when the customer explicitly agrees (\"yes\", \"confirm\", \"go ahead\", etc.).",

		orchestrator.SectionPaymentRules,
		"1. When the customer confirms an order, set `order_confirmed = true`. Tell them their order is confirmed and a secure payment link will be sent momentarily in a separate message, where they can pay with",
		"3. If the conversation state is INVOICING, it means the order is already confirmed and the payment link has already been sent! DO NOT set `order_confirmed = true` again.",
		"6. NEVER name the payment processor to the customer. They pay the business, not a third party — say \"a secure payment link\" and list the methods above, never the provider's brand name.",

		orchestrator.SectionCrmSignalRules,
		"2. order_confirmed = true ONLY when the user says \"yes\", \"confirm\", \"I'll take it\", or equivalent. Never for inquiries.",
		"4. detected_preferences captures broad interests (e.g. \"she asked about hair products\" → [\"hair products\"]). Not specific product names.",
		"5. sentiment reflects the emotional tone: complaints → negative, thanks/praise → positive, neutral otherwise.",
	} {
		if !strings.Contains(prompt, anchor) {
			t.Errorf("system prompt missing verbatim anchor:\n%q", anchor)
		}
	}
}

func TestSystemPromptSectionOrder(t *testing.T) {
	prompt := orchestrator.SystemPrompt(orchestrator.PromptParams{ConversationState: "LEAD"})
	order := []string{
		orchestrator.SectionFormattingRules,
		orchestrator.SectionRules,
		orchestrator.SectionProductImageRules,
		orchestrator.SectionCustomerImageRules,
		orchestrator.SectionOrderConfirmationRules,
		orchestrator.SectionPaymentRules,
		orchestrator.SectionCrmSignalRules,
	}
	last := -1
	for _, section := range order {
		idx := strings.Index(prompt, section)
		if idx < 0 {
			t.Fatalf("section %q missing", section)
		}
		if idx < last {
			t.Fatalf("section %q out of order (at %d, previous at %d)", section, idx, last)
		}
		last = idx
	}
}

func TestSystemPromptInjectionSlots(t *testing.T) {
	catalog := "=== CATALOG (1 products — showing all) ===\nShea Butter — GH₵45.00 [HAS_IMAGE] [ID: ab12cd34]"
	shown := "\n=== IMAGES ALREADY SENT ===\n[ID: ef56ab78] Cocoa Butter\n"

	prompt := orchestrator.SystemPrompt(orchestrator.PromptParams{
		ConversationState: "INVOICING",
		PaymentMethods:    "card, bank transfer, or USSD",
		CatalogBlock:      catalog,
		ShownBlock:        shown,
	})

	// Conversation state slot renders.
	if !strings.Contains(prompt, "Current Conversation State: INVOICING") {
		t.Error("conversationState slot did not render")
	}
	// paymentMethods renders in BOTH rule 1 and rule 4.
	if got := strings.Count(prompt, "card, bank transfer, or USSD"); got != 2 {
		t.Errorf("paymentMethods rendered %d times, want 2", got)
	}
	// businessContext = catalog first, shown block second, both verbatim
	// (modulo .trim() eating only the outermost edge whitespace).
	catIdx := strings.Index(prompt, catalog)
	shownNeedle := strings.TrimRight(shown, "\n")
	shownIdx := strings.Index(prompt, shownNeedle)
	if catIdx < 0 || shownIdx < 0 {
		t.Fatalf("business context blocks missing (catalog@%d shown@%d)", catIdx, shownIdx)
	}
	if shownIdx < catIdx {
		t.Errorf("shown block must follow catalog block (source: context + buildShownImagesContext)")
	}
	// No unresolved ${...} markers remain.
	if strings.Contains(prompt, "${") {
		t.Errorf("unresolved template markers in:\n%.120s", prompt[strings.Index(prompt, "${"):])
	}
}

func TestSystemPromptDefaultsAndTrim(t *testing.T) {
	prompt := orchestrator.SystemPrompt(orchestrator.PromptParams{
		ConversationState: "LEAD",
		CatalogBlock:      "CATALOG-BLOCK",
	})
	// Default payment methods = getCurrencyConfig(null) = GHS launch market.
	if got := strings.Count(prompt, orchestrator.DefaultPaymentMethods); got != 2 {
		t.Errorf("default paymentMethods rendered %d times, want 2", got)
	}
	// .trim() semantics: starts at the intro line, ends with the business
	// context block, no surrounding whitespace.
	if !strings.HasPrefix(prompt, "You are a helpful WhatsApp sales and support agent.") {
		t.Errorf("prompt not trimmed at start: %q", prompt[:20])
	}
	if !strings.HasSuffix(prompt, "CATALOG-BLOCK") {
		t.Errorf("prompt not trimmed at end: %q", prompt[len(prompt)-20:])
	}

	// Empty ShownBlock contributes nothing beyond the catalog block: the
	// template itself mentions "IMAGES ALREADY SENT" exactly twice (PRODUCT
	// IMAGE RULES 5 and CUSTOMER IMAGE RULES 5), so a rendered section would
	// raise the count.
	bare := orchestrator.SystemPrompt(orchestrator.PromptParams{CatalogBlock: "X"})
	if !strings.HasSuffix(bare, "X") {
		t.Errorf("catalog block must be the trailing business context, got suffix %q", bare[len(bare)-20:])
	}
	if got := strings.Count(bare, "IMAGES ALREADY SENT"); got != 2 {
		t.Errorf("IMAGES ALREADY SENT appears %d times with empty ShownBlock, want 2 (template mentions only)", got)
	}
}
