package harness

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	stmPollTimeout   = 45 * time.Second
	stmStabilityHold = 6 * time.Second
)

type stmRow struct {
	from    string
	trigger string
	toWant  string
	class   string
}

func stmBaseReply() map[string]any {
	return map[string]any{
		"reply_text":                "Sure, I can help with that.",
		"internal_confidence":       0.95,
		"intent":                    "product_inquiry",
		"customer_requested_images": false,
		"send_product_image_ids":    []any{},
		"referenced_product_ids":    []any{},
		"crm_signals": map[string]any{
			"order_confirmed":      false,
			"detected_items":       []any{},
			"delivery_area":        nil,
			"customer_name":        nil,
			"detected_preferences": []any{},
			"sentiment":            "neutral",
		},
	}
}

func stmIntentReply(intent string) map[string]any {
	r := stmBaseReply()
	r["intent"] = intent
	switch intent {
	case "checkout_request":
		r["reply_text"] = "Great, let me start a checkout for you."
	case "support_faq":
		r["reply_text"] = "We deliver Monday to Saturday, 9am to 7pm."
	case "complaint":
		r["reply_text"] = "I am sorry about that, someone from the team will reach out."
	}
	return r
}

func stmLowConfidenceReply() map[string]any {
	r := stmBaseReply()
	r["internal_confidence"] = 0.4
	r["intent"] = "unknown"
	r["reply_text"] = "Sorry, I am not sure I understood that."
	return r
}

func stmInboundTextFor(trigger, shortID string) string {
	switch trigger {
	case "product_inquiry":
		return fmt.Sprintf("Do you have [ID:%s] in stock?", shortID)
	case "checkout_request":
		return "I would like to buy this one, how do we proceed?"
	case "support_faq":
		return "What are your delivery days and hours?"
	case "complaint":
		return "My parcel arrived late and the box was damaged"
	case "order_confirmed":
		return fmt.Sprintf("Yes please confirm my order: 2 of [ID:%s]", shortID)
	default:
		return "Hmm maybe something else entirely"
	}
}

func stmConversationState(t *testing.T, db *sql.DB, convID string) string {
	t.Helper()
	var s string
	if err := db.QueryRow(`SELECT state::text FROM conversations WHERE id = $1`, convID).Scan(&s); err != nil {
		t.Fatalf("read conversation state %s: %v", convID, err)
	}
	return s
}

func stmWaitForPipelineReply(t *testing.T, db *sql.DB, stack *NodeStack, convID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, convID).Scan(&n); err != nil {
			t.Fatalf("count outbound for %s: %v", convID, err)
		}
		if n >= 1 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("no outbound reply for conversation %s within %s\n--- node logs ---\n%s", convID, timeout, stack.DumpLogs())
}

func stmAssertStateStable(t *testing.T, db *sql.DB, convID, want string, hold time.Duration) {
	t.Helper()
	deadline := time.Now().Add(hold)
	for time.Now().Before(deadline) {
		if got := stmConversationState(t, db, convID); got != want {
			t.Fatalf("conversation %s drifted %q -> %q during %s stability hold", convID, want, got, hold)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func stmWaitEscalationFlag(t *testing.T, db *sql.DB, convID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var escalated bool
		var reason string
		if err := db.QueryRow(
			`SELECT is_escalated_to_human, COALESCE(escalation_reason,'') FROM conversations WHERE id = $1`,
			convID).Scan(&escalated, &reason); err != nil {
			t.Fatalf("read escalation flag for %s: %v", convID, err)
		}
		if escalated && reason == "LOW_CONFIDENCE" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Errorf("conversation %s never reached is_escalated_to_human=true + LOW_CONFIDENCE within %s", convID, timeout)
}

func TestT012_StateMachineTransitionTable(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	prod := factory.Product(biz.ID, WithStock(50), WithPrice("25.50"))
	shortID := prod.ID[:8]

	rows := []stmRow{
		{from: "LEAD", trigger: "product_inquiry", toWant: "BROWSING", class: "legal-edge"},
		{from: "LEAD", trigger: "checkout_request", toWant: "CHECKOUT", class: "legal-edge"},
		{from: "LEAD", trigger: "support_faq", toWant: "SUPPORT", class: "legal-edge"},
		{from: "BROWSING", trigger: "checkout_request", toWant: "CHECKOUT", class: "legal-edge"},
		{from: "BROWSING", trigger: "complaint", toWant: "SUPPORT", class: "legal-edge"},
		{from: "BROWSING", trigger: "order_confirmed", toWant: "INVOICING", class: "legal-edge"},
		{from: "CHECKOUT", trigger: "order_confirmed", toWant: "INVOICING", class: "legal-edge"},
		{from: "LEAD", trigger: "order_confirmed", toWant: "LEAD", class: "table-rejects"},
		{from: "SUPPORT", trigger: "order_confirmed", toWant: "SUPPORT", class: "table-rejects"},
		{from: "CHECKOUT", trigger: "support_faq", toWant: "CHECKOUT", class: "guard-blocked"},
		{from: "INVOICING", trigger: "support_faq", toWant: "INVOICING", class: "guard-blocked"},
		{from: "INVOICING", trigger: "product_inquiry", toWant: "INVOICING", class: "guard-blocked"},
		{from: "SUPPORT", trigger: "product_inquiry", toWant: "SUPPORT", class: "guard-blocked"},
		{from: "ESCALATED", trigger: "product_inquiry", toWant: "ESCALATED", class: "terminal-guard-blocked"},
		{from: "ESCALATED", trigger: "order_confirmed", toWant: "ESCALATED", class: "terminal-guard-blocked"},
		{from: "LEAD", trigger: "low_confidence", toWant: "LEAD", class: "escalation-flag-only"},
		{from: "CHECKOUT", trigger: "low_confidence", toWant: "CHECKOUT", class: "escalation-flag-only"},
	}

	results := make([]string, len(rows))
	var resultsMu sync.Mutex

	var current string
	var currentMu sync.Mutex
	stack.SetChatReply(func(_ map[string]any) map[string]any {
		currentMu.Lock()
		trigger := current
		currentMu.Unlock()
		switch trigger {
		case "checkout_request":
			return stmIntentReply("checkout_request")
		case "support_faq":
			return stmIntentReply("support_faq")
		case "complaint":
			return stmIntentReply("complaint")
		case "order_confirmed":
			return orderConfirmedReply(shortID, 2)
		case "low_confidence":
			return stmLowConfidenceReply()
		default:
			return inquiryReplyForTest(shortID)
		}
	})

	for idx, row := range rows {
		t.Run(fmt.Sprintf("%02d_%s_via_%s_to_%s", idx+1, row.from, row.trigger, row.toWant), func(t *testing.T) {
			defer func() {
				resultsMu.Lock()
				defer resultsMu.Unlock()
				status := "PASS"
				if t.Failed() {
					status = "FAIL"
				}
				if row.toWant == row.from {
					results[idx] = fmt.Sprintf("%-9s -> %-9s via %-16s => stays %-9s [%s] %s", row.from, row.toWant, row.trigger, row.from, row.class, status)
				} else {
					results[idx] = fmt.Sprintf("%-9s -> %-9s via %-16s => %-9s [%s] %s", row.from, row.toWant, row.trigger, row.toWant, row.class, status)
				}
			}()

			currentMu.Lock()
			current = row.trigger
			currentMu.Unlock()

			phone := fmt.Sprintf("+2337%09d", idx)
			conv := factory.Conversation(biz.ID, phone, WithState(row.from))

			messageID := fmt.Sprintf("wamid.T0.12-%02d-%d", idx+1, time.Now().UnixNano())
			payload := buildWhatsAppPayload(
				"waba-t012",
				biz.WhatsAppPhoneNumberID,
				messageID,
				phone,
				stmInboundTextFor(row.trigger, shortID),
			)
			postWebhookExpectOK(t, stack, payload)

			switch {
			case row.trigger == "low_confidence":
				stmWaitEscalationFlag(t, db, conv.ID, stmPollTimeout)
				if got := stmConversationState(t, db, conv.ID); got != row.from {
					t.Errorf("low-confidence escalation moved state %q -> %q; pipeline escalates by flag only, state must stay %q", row.from, got, row.from)
				}
			case row.toWant != row.from:
				waitForInt(t, db,
					`SELECT COUNT(*) FROM conversations WHERE id = $1 AND state = $2`,
					1, stmPollTimeout, conv.ID, row.toWant)
			default:
				stmWaitForPipelineReply(t, db, stack, conv.ID, stmPollTimeout)
				stmAssertStateStable(t, db, conv.ID, row.from, stmStabilityHold)
				if got := stmConversationState(t, db, conv.ID); got != row.from {
					t.Errorf("expected conversation to stay in %q, got %q (transition must be rejected)", row.from, got)
				}
			}
		})
	}

	resultsMu.Lock()
	defer resultsMu.Unlock()
	for i, line := range results {
		if line == "" {
			line = fmt.Sprintf("row %02d did not complete", i+1)
		}
		t.Logf("TABLE | %s", line)
	}
}
