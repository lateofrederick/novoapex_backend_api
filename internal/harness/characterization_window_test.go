package harness

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func windowWhatsAppPayloadOldTS(wabaID, phoneNumberID, messageID, fromPhone, body string, tsUnix int64) []byte {
	m := newTextMessage(messageID, fromPhone, body)
	m.Timestamp = strconv.FormatInt(tsUnix, 10)
	p := waPayload{
		Object: "whatsapp_business_account",
		Entry: []waEntry{{
			ID: wabaID,
			Changes: []waChange{{
				Field: "messages",
				Value: waValue{
					MessagingProduct: "whatsapp",
					Metadata: waMetadata{
						DisplayPhoneNumber: "233200000000",
						PhoneNumberID:      phoneNumberID,
					},
					Messages: []waTextMessage{m},
				},
			}},
		}},
	}
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return b
}

func windowInquiryOnlyReply(marker string) map[string]any {
	return map[string]any{
		"reply_text":                "Sure, one moment please. " + marker,
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

func TestT014_TwentyFourHourWindowClosedBlocksFreeFormReply(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	customerPhone := factory.nextPhone()

	const freeFormMarker = "WINDOWCLOSEDFREEREPLY"
	stack.SetChatReply(func(_ map[string]any) map[string]any {
		r := windowInquiryOnlyReply(freeFormMarker)
		r["reply_text"] = "The sun is out great for deliveries today. " + freeFormMarker
		return r
	})

	oldTS := time.Now().Add(-26 * time.Hour).Unix()
	msgID := fmt.Sprintf("wamid.t014-a-%d", time.Now().UnixNano())
	payload := windowWhatsAppPayloadOldTS(
		"waba-t014",
		biz.WhatsAppPhoneNumberID,
		msgID,
		customerPhone,
		"Any updates on my delivery?",
		oldTS,
	)
	postWebhookExpectOK(t, stack, payload)

	var conversationID string
	deadline := time.Now().Add(60 * time.Second)
	for {
		err := db.QueryRow(
			`SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
			biz.ID, customerPhone).Scan(&conversationID)
		if err == nil {
			break
		}
		if err != sql.ErrNoRows {
			t.Fatalf("query conversation: %v", err)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("conversation never created\n--- logs ---\n%s", stack.DumpLogs())
		}
		time.Sleep(500 * time.Millisecond)
	}

	var inboundCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM inbound_messages WHERE whatsapp_message_id = $1`, msgID).Scan(&inboundCount); err != nil {
		t.Fatalf("count inbound: %v", err)
	}
	if inboundCount != 1 {
		t.Fatalf("inbound message missing")
	}

	waitForInt(t, db,
		`SELECT COUNT(*) FROM outbound_messages
		 WHERE conversation_id = $1 AND status = 'failed_24h_window_closed'`,
		1, 60*time.Second, conversationID)

	var rowType, rowRaw, rowText, rowRecipient string
	if err := db.QueryRow(
		`SELECT message_type, raw_payload::text, COALESCE(text_content,''), recipient_phone
		 FROM outbound_messages
		 WHERE conversation_id = $1 AND status = 'failed_24h_window_closed'
		 ORDER BY created_at DESC LIMIT 1`,
		conversationID).Scan(&rowType, &rowRaw, &rowText, &rowRecipient); err != nil {
		t.Fatalf("fetch window-blocked outbound row: %v", err)
	}

	if rowType != "text" {
		t.Errorf("blocked row message_type = %q, want text", rowType)
	}
	if strings.TrimSpace(rowRaw) != "{}" {
		t.Errorf("blocked row raw_payload = %s, want empty object {} (reality: no WhatsApp payload is built for a blocked send)", rowRaw)
	}
	if !strings.Contains(rowText, freeFormMarker) {
		t.Errorf("blocked row text_content missing scripted reply marker:\n%s", rowText)
	}
	if rowRecipient != customerPhone {
		t.Errorf("blocked row recipient = %s, want %s", rowRecipient, customerPhone)
	}
	t.Logf("characterized: gate reads latest inbound_messages.timestamp (%ds old), NOT conversations.last_contact_at (column does not exist)", oldTS-time.Now().Unix())

	var freshDeliverable int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM outbound_messages
		 WHERE conversation_id = $1 AND status IN ('pending','sent') AND created_at > NOW() - INTERVAL '3 minutes'`,
		conversationID).Scan(&freshDeliverable); err != nil {
		t.Fatalf("count post-window outbound rows: %v", err)
	}
	if freshDeliverable != 0 {
		t.Errorf("%d deliverable outbound rows appeared after the window closed; only the failed_24h_window_closed row is allowed", freshDeliverable)
	}
}

func windowFetchImageRows(t *testing.T, db *sql.DB, conversationID string) []map[string]string {
	t.Helper()
	rows, err := db.Query(
		`SELECT id, message_type, COALESCE(image_url,''), COALESCE(product_id,''), COALESCE(text_content,''), status
		 FROM outbound_messages WHERE conversation_id = $1 AND message_type = 'image'
		 ORDER BY created_at ASC`,
		conversationID)
	if err != nil {
		t.Fatalf("query image rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := []map[string]string{}
	for rows.Next() {
		var id, mtype, url, pid, caption, status string
		if err := rows.Scan(&id, &mtype, &url, &pid, &caption, &status); err != nil {
			t.Fatalf("scan image row: %v", err)
		}
		out = append(out, map[string]string{
			"id": id, "message_type": mtype, "image_url": url,
			"product_id": pid, "caption": caption, "status": status,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate image rows: %v", err)
	}
	return out
}

func windowWaitOutboundTextMarker(t *testing.T, db *sql.DB, conversationID, marker string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1 AND text_content LIKE '%' || $2 || '%'`,
			conversationID, marker).Scan(&n); err != nil {
			t.Fatalf("count marker rows: %v", err)
		}
		if n >= 1 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("outbound text containing marker %q never appeared within %s", marker, timeout)
}

func windowConversationID(t *testing.T, db *sql.DB, businessID, phone string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var id string
		err := db.QueryRow(
			`SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
			businessID, phone).Scan(&id)
		if err == nil {
			return id
		}
		if err != sql.ErrNoRows {
			t.Fatalf("query conversation: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("conversation for phone %s never created within %s", phone, timeout)
	return ""
}

func TestT013_ProductImageRules(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	prod := factory.ProductWithImages(biz.ID, 1, WithStock(9), WithPrice("25.50"))
	shortID := prod.ID[:8]

	var imageURL, productName string
	if err := db.QueryRow(
		`SELECT url FROM product_images WHERE product_id = $1 ORDER BY position ASC LIMIT 1`, prod.ID).Scan(&imageURL); err != nil {
		t.Fatalf("fetch product image url: %v", err)
	}
	if err := db.QueryRow(`SELECT name FROM products WHERE id = $1`, prod.ID).Scan(&productName); err != nil {
		t.Fatalf("fetch product name: %v", err)
	}

	const suppressedMarker = "IMAGESUPPRESSEDREPLY"
	const requestedMarker = "IMAGEREQUESTEDREPLY"

	phoneA := factory.nextPhone()
	phoneB := factory.nextPhone()

	turn := 0
	stack.SetChatReply(func(_ map[string]any) map[string]any {
		turn++
		switch turn {
		case 1:
			return inquiryReplyForTest(shortID)
		case 2:
			r := inquiryReplyForTest(shortID)
			r["customer_requested_images"] = false
			r["send_product_image_ids"] = []any{shortID}
			r["reply_text"] = suppressedMarker
			return r
		case 3:
			return inquiryReplyForTest(shortID)
		default:
			r := inquiryReplyForTest(shortID)
			r["customer_requested_images"] = true
			r["send_product_image_ids"] = []any{shortID}
			r["reply_text"] = requestedMarker
			return r
		}
	})

	sendMsg := func(phone, tag, body string) {
		t.Helper()
		id := fmt.Sprintf("wamid.t013-%s-%d", tag, time.Now().UnixNano())
		payload := buildWhatsAppPayload("waba-t013", biz.WhatsAppPhoneNumberID, id, phone, body)
		postWebhookExpectOK(t, stack, payload)
	}

	sendMsg(phoneA, "a1", fmt.Sprintf("Do you have [ID:%s] in stock?", shortID))
	convAID := windowConversationID(t, db, biz.ID, phoneA, 60*time.Second)

	sendMsg(phoneA, "a2", fmt.Sprintf("Please send a photo of [ID:%s]", shortID))
	windowWaitOutboundTextMarker(t, db, convAID, suppressedMarker, 60*time.Second)

	time.Sleep(6 * time.Second)

	imageRowsA := windowFetchImageRows(t, db, convAID)
	if len(imageRowsA) != 0 {
		t.Errorf("scenario A: expected ZERO image rows when customer_requested_images=false even though send_product_image_ids was non-empty; got %d: %+v",
			len(imageRowsA), imageRowsA)
	} else {
		t.Logf("characterized: unsolicited image suppressed (gateProductImageIds returns []) while text reply still sent")
	}

	sendMsg(phoneB, "b1", fmt.Sprintf("Do you have [ID:%s] in stock?", shortID))
	convBID := windowConversationID(t, db, biz.ID, phoneB, 60*time.Second)

	sendMsg(phoneB, "b2", fmt.Sprintf("Please send me the photo of [ID:%s]", shortID))

	var imgRow map[string]string
	found := false
	deadline := time.Now().Add(75 * time.Second)
	for time.Now().Before(deadline) {
		rows := windowFetchImageRows(t, db, convBID)
		if len(rows) > 0 {
			imgRow = rows[0]
			found = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !found {
		t.Fatalf("scenario B: no image-type outbound row although customer_requested_images=true and the product has an image\n--- logs ---\n%s", stack.DumpLogs())
	}

	if imgRow["image_url"] != imageURL {
		t.Errorf("image row url = %q, want %q", imgRow["image_url"], imageURL)
	}
	if imgRow["product_id"] != prod.ID {
		t.Errorf("image row product_id = %q, want full UUID %q", imgRow["product_id"], prod.ID)
	}
	wantCaption := fmt.Sprintf("%s — GH₵%s", productName, "25.50")
	if imgRow["caption"] != wantCaption {
		t.Errorf("image row caption = %q, want %q", imgRow["caption"], wantCaption)
	}
	t.Logf("characterized: allowed image row status observed as %q (final delivery state may be failed against real graph API)", imgRow["status"])

	windowWaitOutboundTextMarker(t, db, convBID, requestedMarker, 30*time.Second)
}
