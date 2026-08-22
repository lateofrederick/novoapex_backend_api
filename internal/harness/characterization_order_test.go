package harness

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func orderConfirmedReply(shortID string, quantity float64) map[string]any {
	return map[string]any{
		"reply_text":                "Great! Your order is confirmed. Total: GHS 51.00.",
		"internal_confidence":       0.95,
		"intent":                    "product_inquiry",
		"customer_requested_images": false,
		"send_product_image_ids":    []any{},
		"referenced_product_ids":    []any{shortID},
		"crm_signals": map[string]any{
			"order_confirmed": true,
			"detected_items": []any{
				map[string]any{
					"product_id": shortID,
					"quantity":   quantity,
				},
			},
			"delivery_area":        "Osu",
			"customer_name":        nil,
			"detected_preferences": []any{},
			"sentiment":            "positive",
		},
	}
}

func scalarString(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var s string
	if err := db.QueryRow(query, args...).Scan(&s); err != nil {
		t.Fatalf("scalar query %q: %v", query, err)
	}
	return s
}

func waitForInt(t *testing.T, db *sql.DB, query string, want int, timeout time.Duration, args ...any) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int
	for time.Now().Before(deadline) {
		if err := db.QueryRow(query, args...).Scan(&got); err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		if got == want {
			return got
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Errorf("query %q stuck at %d, want %d within %s", query, got, want, timeout)
	return got
}

func TestT006_OrderHappyPathThroughOrchestrator(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	prod := factory.Product(biz.ID, WithStock(5), WithPrice("25.50"))
	shortID := prod.ID[:8]

	turn := 0
	stack.SetChatReply(func(_ map[string]any) map[string]any {
		turn++
		if turn == 1 {
			return inquiryReplyForTest(shortID)
		}
		return orderConfirmedReply(shortID, 2)
	})

	customerPhone := biz.OwnerPhone
	messageID := fmt.Sprintf("wamid.T0.6a-%d", time.Now().UnixNano())
	payload := buildWhatsAppPayload(
		"waba-t006",
		biz.WhatsAppPhoneNumberID,
		messageID,
		customerPhone,
		fmt.Sprintf("Do you have [ID:%s] in stock?", shortID),
	)
	postWebhookExpectOK(t, stack, payload)

	waitForInt(t, db,
		`SELECT COUNT(*) FROM conversations WHERE business_id = $1 AND customer_phone = $2 AND state = 'BROWSING'`,
		1, 45*time.Second, biz.ID, customerPhone)

	messageID2 := fmt.Sprintf("wamid.T0.6b-%d", time.Now().UnixNano())
	payload2 := buildWhatsAppPayload(
		"waba-t006",
		biz.WhatsAppPhoneNumberID,
		messageID2,
		customerPhone,
		fmt.Sprintf("Yes please confirm my order: 2 of [ID:%s]", shortID),
	)
	postWebhookExpectOK(t, stack, payload2)

	convQuery := `SELECT COUNT(*) FROM conversations WHERE business_id = $1 AND customer_phone = $2 AND state = 'INVOICING'`
	waitForInt(t, db, convQuery, 1, 45*time.Second, biz.ID, customerPhone)

	orderCountQuery := `SELECT COUNT(*) FROM orders
		WHERE business_id = $1
		  AND conversation_id = (SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2)`
	waitForInt(t, db, orderCountQuery, 1, 30*time.Second, biz.ID, customerPhone)

	var orderID string
	if err := db.QueryRow(
		`SELECT id FROM orders WHERE business_id = $1 AND conversation_id = (SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2)`,
		biz.ID, customerPhone).Scan(&orderID); err != nil {
		t.Fatalf("fetch order id: %v", err)
	}

	total := scalarString(t, db, `SELECT total_amount::text FROM orders WHERE id = $1`, orderID)
	if total != "51.00" {
		t.Errorf("total_amount = %s, want 51.00", total)
	}

	idemKey := scalarString(t, db, `SELECT COALESCE(idempotency_key,'') FROM orders WHERE id = $1`, orderID)
	if idemKey == "" {
		t.Error("idempotency_key must be populated on created orders")
	} else {
		var inboundMatches int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM inbound_messages WHERE id = $1 OR whatsapp_message_id = $1`,
			idemKey).Scan(&inboundMatches); err != nil || inboundMatches != 1 {
			t.Errorf("idempotency_key %q does not resolve to the source inbound message (matches=%d err=%v)", idemKey, inboundMatches, err)
		}
	}

	itemCount := waitForInt(t, db,
		`SELECT COUNT(*) FROM order_items WHERE order_id = $1 AND product_id = $2 AND unit_price::text = '25.50' AND quantity = 2`,
		1, 15*time.Second, orderID, prod.ID)
	if itemCount != 1 {
		return
	}

	stock := scalarString(t, db, `SELECT stock::text FROM products WHERE id = $1`, prod.ID)
	if stock != "3" {
		t.Errorf("stock after decrement = %s, want 3", stock)
	}

	payURL := scalarString(t, db, `SELECT text_content FROM outbound_messages WHERE conversation_id = (SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2) ORDER BY created_at DESC LIMIT 1`, biz.ID, customerPhone)
	if payURL == "" {
		t.Fatal("expected an outbound message for the payment link")
	}
	var rawText string
	if err := json.Unmarshal([]byte(payURL), &rawText); err == nil {
		payURL = rawText
	}
	if !strings.Contains(payURL, "checkout.paystack.test") || !strings.Contains(payURL, "GHS 51.00") {
		t.Errorf("payment message missing stub URL/total:\n%s", payURL)
	}
}

func postWebhookExpectOK(t *testing.T, stack *NodeStack, payload []byte) {
	t.Helper()
	code := postWebhook(t, stack, payload)
	if code != 200 {
		t.Fatalf("webhook POST status = %d\n--- node logs ---\n%s", code, stack.DumpLogs())
	}
}

func TestT008_DuplicateOrderSourceRejectedByUniqueIndex(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)

	biz := factory.Business()
	cust := factory.Customer(biz.ID)

	factory.exec("seed-order", `INSERT INTO orders
		(id, business_id, customer_id, status, total_amount, currency, idempotency_key, updated_at)
		VALUES ('ord_dup', $1, $2, 'PENDING', 10.00, 'GHS', 'wamid-dup-1', NOW())`,
		biz.ID, cust.ID)

	_, dupErr := db.Exec(`INSERT INTO orders
		(id, business_id, customer_id, status, total_amount, currency, idempotency_key, updated_at)
		VALUES ('ord_dup2', $1, $2, 'PENDING', 10.00, 'GHS', 'wamid-dup-1', NOW())`,
		biz.ID, cust.ID)
	if dupErr == nil {
		t.Error("second order with same idempotency_key must violate unique index")
	}
}

func inquiryReplyForTest(shortID string) map[string]any {
	r := orderConfirmedReply(shortID, 0)
	r["intent"] = "product_inquiry"
	r["reply_text"] = "Yes! We have that in stock."
	crm := r["crm_signals"].(map[string]any)
	crm["order_confirmed"] = false
	crm["detected_items"] = []any{}
	return r
}
