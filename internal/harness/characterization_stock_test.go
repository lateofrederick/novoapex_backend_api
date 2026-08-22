package harness

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func stockDetectedItem(shortID string, quantity int) map[string]any {
	return map[string]any{"product_id": shortID, "quantity": quantity}
}

func stockConfirmedReply(items ...map[string]any) map[string]any {
	detected := make([]any, 0, len(items))
	refs := make([]any, 0, len(items))
	seen := map[string]bool{}
	for _, it := range items {
		detected = append(detected, it)
		id, _ := it["product_id"].(string)
		if !seen[id] {
			seen[id] = true
			refs = append(refs, id)
		}
	}
	return map[string]any{
		"reply_text":                "Great! Your order is confirmed.",
		"internal_confidence":       0.95,
		"intent":                    "product_inquiry",
		"customer_requested_images": false,
		"send_product_image_ids":    []any{},
		"referenced_product_ids":    refs,
		"crm_signals": map[string]any{
			"order_confirmed":      true,
			"detected_items":       detected,
			"delivery_area":        "Osu",
			"customer_name":        nil,
			"detected_preferences": []any{},
			"sentiment":            "positive",
		},
	}
}

func stockScriptedReplies(stack *NodeStack, replies ...map[string]any) {
	var calls atomic.Int64
	stack.SetChatReply(func(_ map[string]any) map[string]any {
		i := int(calls.Add(1)) - 1
		if i >= len(replies) {
			i = len(replies) - 1
		}
		return replies[i]
	})
}

func stockPostWebhook(stack *NodeStack, payload []byte) (int, error) {
	req, err := http.NewRequest(http.MethodPost, stack.BaseURL+"/webhooks/whatsapp", bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hub-Signature-256", signWhatsAppBody(testWhatsAppAppSecret, payload))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	drainAndClose(resp)
	return resp.StatusCode, nil
}

func stockRedisCmd(t *testing.T, addr, password string, args ...string) any {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("redis dial %s: %v", addr, err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("redis deadline: %v", err)
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(password), password)
	fmt.Fprintf(&buf, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("redis write: %v", err)
	}
	rd := bufio.NewReader(conn)
	if _, err := stockRedisReply(rd); err != nil {
		t.Fatalf("redis AUTH: %v", err)
	}
	reply, err := stockRedisReply(rd)
	if err != nil {
		t.Fatalf("redis %v: %v", args, err)
	}
	return reply
}

func stockRedisReply(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	switch prefix {
	case '+':
		return line, nil
	case '-':
		return nil, fmt.Errorf("redis error: %s", line)
	case ':':
		return strconv.Atoi(line)
	case '$':
		n, convErr := strconv.Atoi(line)
		if convErr != nil {
			return nil, convErr
		}
		if n < 0 {
			return nil, nil
		}
		payload := make([]byte, n+2)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		return string(payload[:n]), nil
	case '*':
		n, convErr := strconv.Atoi(line)
		if convErr != nil {
			return nil, convErr
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, elemErr := stockRedisReply(r)
			if elemErr != nil {
				return nil, elemErr
			}
			out = append(out, v)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported resp prefix %q", string(prefix))
	}
}

func stockDumpFailedMaterialiserJobs(t *testing.T, h *Harness) {
	t.Helper()
	reply := stockRedisCmd(t, h.RedisAddr, RedisPassword, "ZRANGE", "bull:crm-materialiser:failed", "0", "-1")
	ids, _ := reply.([]any)
	t.Logf("stock forensics: %d failed crm-materialiser job(s)", len(ids))
	for _, idAny := range ids {
		id, ok := idAny.(string)
		if !ok {
			continue
		}
		reason := stockRedisCmd(t, h.RedisAddr, RedisPassword, "HGET", "bull:crm-materialiser:"+id, "failedReason")
		t.Logf("stock forensics: job %s failedReason=%v", id, reason)
	}
}

func stockWaitForLogLine(t *testing.T, stack *NodeStack, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(stack.DumpLogs(), substr) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("node logs never contained %q within %s\n--- logs ---\n%s", substr, timeout, stack.DumpLogs())
}

func stockCountRemains(t *testing.T, db *sql.DB, query string, want int, window time.Duration, args ...any) {
	t.Helper()
	deadline := time.Now().Add(window)
	last := -1
	for time.Now().Before(deadline) {
		if err := db.QueryRow(query, args...).Scan(&last); err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		if last != want {
			t.Fatalf("query %q drifted to %d, must remain %d", query, last, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

const (
	ordersForConversationQuery = `SELECT COUNT(*) FROM orders
		WHERE business_id = $1
		  AND conversation_id = (SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2)`
	stateForConversationQuery = `SELECT COUNT(*) FROM conversations WHERE business_id = $1 AND customer_phone = $2 AND state = $3`
)

func stockSendTwoTurnOrder(t *testing.T, stack *NodeStack, db *sql.DB, biz Business, wabaTag, confirmText string) {
	t.Helper()
	first := buildWhatsAppPayload(
		wabaTag,
		biz.WhatsAppPhoneNumberID,
		fmt.Sprintf("wamid.%s-t1-%d", wabaTag, time.Now().UnixNano()),
		biz.OwnerPhone,
		"hello, what do you have?",
	)
	postWebhookExpectOK(t, stack, first)

	waitForInt(t, db, stateForConversationQuery, 1, 45*time.Second, biz.ID, biz.OwnerPhone, "BROWSING")

	second := buildWhatsAppPayload(
		wabaTag,
		biz.WhatsAppPhoneNumberID,
		fmt.Sprintf("wamid.%s-t2-%d", wabaTag, time.Now().UnixNano()),
		biz.OwnerPhone,
		confirmText,
	)
	postWebhookExpectOK(t, stack, second)
}

func TestT007a_MultiItemOrder(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	prodA := factory.Product(biz.ID, WithStock(5), WithPrice("25.50"))
	prodB := factory.Product(biz.ID, WithStock(7), WithPrice("10.00"))
	shortA := prodA.ID[:8]
	shortB := prodB.ID[:8]

	stockScriptedReplies(stack,
		inquiryReplyForTest(shortA),
		stockConfirmedReply(stockDetectedItem(shortA, 2), stockDetectedItem(shortB, 3)),
	)

	stockSendTwoTurnOrder(t, stack, db, biz, "waba-t07a",
		fmt.Sprintf("Confirm my order: 2 x [ID:%s] and 3 x [ID:%s]", shortA, shortB))

	waitForInt(t, db, stateForConversationQuery, 1, 45*time.Second, biz.ID, biz.OwnerPhone, "INVOICING")
	waitForInt(t, db, ordersForConversationQuery, 1, 60*time.Second, biz.ID, biz.OwnerPhone)

	var orderID string
	if err := db.QueryRow(
		`SELECT id FROM orders WHERE business_id = $1 AND conversation_id = (SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2)`,
		biz.ID, biz.OwnerPhone).Scan(&orderID); err != nil {
		stockDumpFailedMaterialiserJobs(t, h)
		t.Fatalf("fetch order id: %v\n--- logs ---\n%s", err, stack.DumpLogs())
	}

	total := scalarString(t, db, `SELECT total_amount::text FROM orders WHERE id = $1`, orderID)
	if total != "81.00" {
		t.Errorf("total_amount = %s, want 81.00 (2x25.50 + 3x10.00)", total)
	}

	waitForInt(t, db, `SELECT COUNT(*) FROM order_items WHERE order_id = $1`, 2, 15*time.Second, orderID)

	itemsA := waitForInt(t, db,
		`SELECT COUNT(*) FROM order_items WHERE order_id = $1 AND product_id = $2 AND quantity = 2 AND unit_price::text = '25.50'`,
		1, 15*time.Second, orderID, prodA.ID)
	if itemsA != 1 {
		t.Errorf("expected exactly 1 OrderItem row for product A (qty 2 @ 25.50), got %d", itemsA)
	}
	itemsB := waitForInt(t, db,
		`SELECT COUNT(*) FROM order_items WHERE order_id = $1 AND product_id = $2 AND quantity = 3 AND unit_price::text = '10.00'`,
		1, 15*time.Second, orderID, prodB.ID)
	if itemsB != 1 {
		t.Errorf("expected exactly 1 OrderItem row for product B (qty 3 @ 10.00), got %d", itemsB)
	}

	stockA := scalarString(t, db, `SELECT stock::text FROM products WHERE id = $1`, prodA.ID)
	if stockA != "3" {
		t.Errorf("product A stock = %s, want 3 (5 - 2)", stockA)
	}
	stockB := scalarString(t, db, `SELECT stock::text FROM products WHERE id = $1`, prodB.ID)
	if stockB != "4" {
		t.Errorf("product B stock = %s, want 4 (7 - 3)", stockB)
	}
}

func TestT007b_DuplicateLineItemsSameProduct(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	prod := factory.Product(biz.ID, WithStock(10), WithPrice("20.00"))
	shortID := prod.ID[:8]

	stockScriptedReplies(stack,
		inquiryReplyForTest(shortID),
		stockConfirmedReply(stockDetectedItem(shortID, 1), stockDetectedItem(shortID, 2)),
	)

	stockSendTwoTurnOrder(t, stack, db, biz, "waba-t07b",
		fmt.Sprintf("I will take 1 of [ID:%s], and actually make it 2 more of [ID:%s]", shortID, shortID))

	waitForInt(t, db, stateForConversationQuery, 1, 45*time.Second, biz.ID, biz.OwnerPhone, "INVOICING")
	waitForInt(t, db, ordersForConversationQuery, 1, 60*time.Second, biz.ID, biz.OwnerPhone)

	var orderID string
	if err := db.QueryRow(
		`SELECT id FROM orders WHERE business_id = $1 AND conversation_id = (SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2)`,
		biz.ID, biz.OwnerPhone).Scan(&orderID); err != nil {
		stockDumpFailedMaterialiserJobs(t, h)
		t.Fatalf("fetch order id: %v\n--- logs ---\n%s", err, stack.DumpLogs())
	}

	itemRows := waitForInt(t, db,
		`SELECT COUNT(*) FROM order_items WHERE order_id = $1 AND product_id = $2`,
		2, 15*time.Second, orderID, prod.ID)
	if itemRows != 2 {
		t.Errorf("characterization: duplicate detected_items produced %d OrderItem rows, observed expectation is 2 (one row per detected entry)", itemRows)
	}

	qtyList := scalarString(t, db,
		`SELECT COALESCE(string_agg(quantity::text, ',' ORDER BY quantity), '') FROM order_items WHERE order_id = $1 AND product_id = $2`,
		orderID, prod.ID)
	if qtyList != "1,2" {
		t.Errorf("per-row quantities = %q, want \"1,2\" (each duplicate kept verbatim)", qtyList)
	}

	sumQty := scalarString(t, db,
		`SELECT SUM(quantity)::text FROM order_items WHERE order_id = $1 AND product_id = $2`,
		orderID, prod.ID)
	if sumQty != "3" {
		t.Errorf("summed quantity = %s, want 3 (1 + 2)", sumQty)
	}

	total := scalarString(t, db, `SELECT total_amount::text FROM orders WHERE id = $1`, orderID)
	if total != "60.00" {
		t.Errorf("total_amount = %s, want 60.00 (3 x 20.00 aggregated)", total)
	}

	stock := scalarString(t, db, `SELECT stock::text FROM products WHERE id = $1`, prod.ID)
	if stock != "7" {
		t.Errorf("stock after decrement = %s, want 7 (10 - aggregated 3, not 10 - 2)", stock)
	}
}

func TestT007c_InsufficientStockRollback(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	prod := factory.Product(biz.ID, WithStock(1), WithPrice("30.00"))
	shortID := prod.ID[:8]

	stockScriptedReplies(stack,
		inquiryReplyForTest(shortID),
		stockConfirmedReply(stockDetectedItem(shortID, 5)),
	)

	stockSendTwoTurnOrder(t, stack, db, biz, "waba-t07c",
		fmt.Sprintf("I want 5 of [ID:%s]", shortID))

	waitForInt(t, db, stateForConversationQuery, 1, 45*time.Second, biz.ID, biz.OwnerPhone, "INVOICING")

	stockWaitForLogLine(t, stack, "order_insufficient_stock", 60*time.Second)

	stockCountRemains(t, db, ordersForConversationQuery, 0, 15*time.Second, biz.ID, biz.OwnerPhone)

	stockCountRemains(t, db, `SELECT stock FROM products WHERE id = $1`, 1, 5*time.Second, prod.ID)
	finalStock := scalarString(t, db, `SELECT stock::text FROM products WHERE id = $1`, prod.ID)
	if finalStock != "1" {
		t.Errorf("stock after rollback = %s, want unchanged 1", finalStock)
	}

	invoicing := waitForInt(t, db, stateForConversationQuery, 1, 10*time.Second, biz.ID, biz.OwnerPhone, "INVOICING")
	if invoicing != 1 {
		t.Errorf("characterization: conversation state after rollback expected INVOICING, matches=%d", invoicing)
	}

	reply := stockRedisCmd(t, h.RedisAddr, RedisPassword, "LLEN", "bull:crm-materialiser:failed")
	failedLen, _ := reply.(int)
	if failedLen != 0 {
		stockDumpFailedMaterialiserJobs(t, h)
		t.Errorf("characterization: InsufficientStockError is swallowed by handler so job should NOT land in failed list, but failed=%d", failedLen)
	}
}

func TestT007d_ConcurrentOrdersRace(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	prod := factory.Product(biz.ID, WithStock(3), WithPrice("12.00"))
	shortID := prod.ID[:8]

	stockScriptedReplies(stack,
		inquiryReplyForTest(shortID),
		stockConfirmedReply(stockDetectedItem(shortID, 2)),
	)

	now := time.Now().UnixNano()
	payloadA := buildWhatsAppPayload(
		"waba-t07d",
		biz.WhatsAppPhoneNumberID,
		fmt.Sprintf("wamid.T07d-a-%d", now),
		biz.OwnerPhone,
		fmt.Sprintf("I want 2 of [ID:%s]", shortID),
	)
	payloadB := buildWhatsAppPayload(
		"waba-t07d",
		biz.WhatsAppPhoneNumberID,
		fmt.Sprintf("wamid.T07d-b-%d", now),
		biz.OwnerPhone,
		fmt.Sprintf("I also want 2 of [ID:%s]", shortID),
	)

	var wg sync.WaitGroup
	statuses := make(chan int, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		code, err := stockPostWebhook(stack, payloadA)
		if err != nil {
			t.Errorf("webhook A transport error: %v", err)
			return
		}
		statuses <- code
	}()
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Second)
		code, err := stockPostWebhook(stack, payloadB)
		if err != nil {
			t.Errorf("webhook B transport error: %v", err)
			return
		}
		statuses <- code
	}()
	wg.Wait()
	close(statuses)

	for code := range statuses {
		if code != http.StatusOK {
			stockDumpFailedMaterialiserJobs(t, h)
			t.Fatalf("webhook status = %d\n--- node logs ---\n%s", code, stack.DumpLogs())
		}
	}

	waitForInt(t, db, stateForConversationQuery, 1, 60*time.Second, biz.ID, biz.OwnerPhone, "INVOICING")
	waitForInt(t, db, ordersForConversationQuery, 1, 75*time.Second, biz.ID, biz.OwnerPhone)

	stockCountRemains(t, db, ordersForConversationQuery, 1, 12*time.Second, biz.ID, biz.OwnerPhone)

	var orderID string
	if err := db.QueryRow(
		`SELECT id FROM orders WHERE business_id = $1 AND conversation_id = (SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2)`,
		biz.ID, biz.OwnerPhone).Scan(&orderID); err != nil {
		stockDumpFailedMaterialiserJobs(t, h)
		t.Fatalf("fetch surviving order id: %v\n--- logs ---\n%s", err, stack.DumpLogs())
	}

	winnerItems := waitForInt(t, db,
		`SELECT COUNT(*) FROM order_items WHERE order_id = $1 AND product_id = $2 AND quantity = 2`,
		1, 15*time.Second, orderID, prod.ID)
	if winnerItems != 1 {
		t.Errorf("surviving order should hold exactly 1 OrderItem (qty 2), got %d", winnerItems)
	}

	finalStock := scalarString(t, db, `SELECT stock::text FROM products WHERE id = $1`, prod.ID)
	if finalStock != "1" {
		t.Errorf("final stock = %s, want 1 (3 - winning 2; losing decrement rolled back)", finalStock)
	}

	reply := stockRedisCmd(t, h.RedisAddr, RedisPassword, "LLEN", "bull:crm-materialiser:failed")
	failedLen, _ := reply.(int)
	if failedLen != 0 {
		stockDumpFailedMaterialiserJobs(t, h)
		t.Errorf("losing job rolls back inside handler (not a BullMQ failure), but failed list has %d entries", failedLen)
	}
}
