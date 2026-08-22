package harness

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

const paystackNodeEnvSecretKey = "sk_test_dummy"

type paystackLoggedRequest = struct {
	Path string
	Body map[string]any
}

var paystackURLRe = regexp.MustCompile(`(?i)https?://[^\s]+`)

var paystackNonDigitsRe = regexp.MustCompile(`\D`)

func paystackRequestLog(stack *NodeStack) []paystackLoggedRequest {
	stack.Paystack.mu.Lock()
	defer stack.Paystack.mu.Unlock()
	out := make([]paystackLoggedRequest, len(stack.Paystack.requestLog))
	copy(out, stack.Paystack.requestLog)
	return out
}

func paystackInitializeBodies(stack *NodeStack) []map[string]any {
	var out []map[string]any
	for _, r := range paystackRequestLog(stack) {
		if r.Path == "/transaction/initialize" {
			out = append(out, r.Body)
		}
	}
	return out
}

func paystackSignBody(secret string, body []byte) string {
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func paystackPostWebhook(t *testing.T, stack *NodeStack, payload []byte) apiResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, stack.BaseURL+"/webhooks/payments/paystack", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("build paystack webhook request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-paystack-signature", paystackSignBody(paystackNodeEnvSecretKey, payload))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /webhooks/payments/paystack: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read paystack webhook response: %v", err)
	}
	return apiResponse{Status: resp.StatusCode, Body: raw}
}

func paystackChargeSuccessPayload(t *testing.T, reference string, amountMinor int64, currency, customerPhone, businessID, bank, paidAtISO string) []byte {
	t.Helper()
	obj := map[string]any{
		"event": "charge.success",
		"data": map[string]any{
			"id":        1234567,
			"reference": reference,
			"amount":    amountMinor,
			"currency":  currency,
			"customer": map[string]any{
				"phone": customerPhone,
				"email": paystackNonDigitsRe.ReplaceAllString(customerPhone, "") + "@customers.novoapex.com",
			},
			"authorization": map[string]any{
				"bank": bank,
			},
			"metadata": map[string]any{
				"businessId":     businessID,
				"customer_phone": customerPhone,
			},
			"paid_at": paidAtISO,
		},
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal paystack webhook payload: %v", err)
	}
	return raw
}

func paystackRedisCommand(addr string, parts ...string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return "", err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(RedisPassword), RedisPassword)
	fmt.Fprintf(&b, "*%d\r\n", len(parts))
	for _, p := range parts {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(p), p)
	}
	if _, err := io.WriteString(conn, b.String()); err != nil {
		return "", err
	}

	rd := bufio.NewReader(conn)
	readLine := func() (string, error) {
		line, err := rd.ReadString('\n')
		if err != nil {
			return "", err
		}
		return strings.TrimRight(line, "\r\n"), nil
	}
	var readReply func() (string, error)
	readReply = func() (string, error) {
		line, err := readLine()
		if err != nil {
			return "", err
		}
		if line == "" {
			return "", fmt.Errorf("empty redis reply")
		}
		switch line[0] {
		case '+', ':':
			return line[1:], nil
		case '-':
			return "", fmt.Errorf("redis error: %s", line[1:])
		case '$':
			n, err := strconv.Atoi(line[1:])
			if err != nil {
				return "", fmt.Errorf("bad bulk length %q", line)
			}
			if n < 0 {
				return "", nil
			}
			buf := make([]byte, n+2)
			if _, err := io.ReadFull(rd, buf); err != nil {
				return "", err
			}
			return string(buf[:n]), nil
		case '*':
			n, err := strconv.Atoi(line[1:])
			if err != nil {
				return "", fmt.Errorf("bad array length %q", line)
			}
			items := make([]string, 0, n)
			for i := 0; i < n; i++ {
				item, err := readReply()
				if err != nil {
					return "", err
				}
				items = append(items, item)
			}
			return strings.Join(items, "\n"), nil
		default:
			return "", fmt.Errorf("unexpected redis reply %q", line)
		}
	}

	if authReply, err := readLine(); err != nil || !strings.HasPrefix(authReply, "+OK") {
		return "", fmt.Errorf("redis auth failed: %q (err=%v)", authReply, err)
	}
	return readReply()
}

func paystackQueueFailureDump(h *Harness, queue string) string {
	zset := "bull:" + queue + ":failed"
	idsRaw, err := paystackRedisCommand(h.RedisAddr, "ZRANGE", zset, "0", "-1")
	if err != nil {
		return fmt.Sprintf("redis forensics unavailable: %v", err)
	}
	var out []string
	for _, id := range strings.Split(idsRaw, "\n") {
		if id == "" {
			continue
		}
		reason, err := paystackRedisCommand(h.RedisAddr, "HGET", "bull:"+queue+":"+id, "failedReason")
		if err != nil {
			reason = fmt.Sprintf("hget failed: %v", err)
		}
		out = append(out, fmt.Sprintf("job %s: %s", id, reason))
	}
	if len(out) == 0 {
		return "no jobs in " + zset
	}
	return strings.Join(out, "; ")
}

type paystackOrderFlow struct {
	Biz            Business
	Prod           Product
	ShortID        string
	CustomerPhone  string
	ConversationID string
	OrderID        string
	Total          string
}

func paystackRunTwoTurnOrder(t *testing.T, factory *Factory, db *sql.DB, stack *NodeStack, tag, price string, stock int, qty float64) paystackOrderFlow {
	t.Helper()

	biz := factory.Business()
	prod := factory.Product(biz.ID, WithStock(stock), WithPrice(price))
	shortID := prod.ID[:8]

	priceF, err := strconv.ParseFloat(price, 64)
	if err != nil {
		t.Fatalf("parse price %q: %v", price, err)
	}
	total := fmt.Sprintf("%.2f", priceF*qty)

	turn := 0
	stack.SetChatReply(func(_ map[string]any) map[string]any {
		turn++
		if turn == 1 {
			return inquiryReplyForTest(shortID)
		}
		return orderConfirmedReply(shortID, qty)
	})

	customerPhone := biz.OwnerPhone
	msg1 := fmt.Sprintf("wamid.%s-a-%d", tag, time.Now().UnixNano())
	payload1 := buildWhatsAppPayload("waba-"+tag, biz.WhatsAppPhoneNumberID, msg1, customerPhone,
		fmt.Sprintf("Do you have [ID:%s] in stock?", shortID))
	postWebhookExpectOK(t, stack, payload1)

	waitForInt(t, db,
		`SELECT COUNT(*) FROM conversations WHERE business_id = $1 AND customer_phone = $2 AND state = 'BROWSING'`,
		1, 60*time.Second, biz.ID, customerPhone)

	msg2 := fmt.Sprintf("wamid.%s-b-%d", tag, time.Now().UnixNano())
	payload2 := buildWhatsAppPayload("waba-"+tag, biz.WhatsAppPhoneNumberID, msg2, customerPhone,
		fmt.Sprintf("Yes please confirm my order: %s of [ID:%s]", strings.TrimRight(strings.TrimRight(strconv.FormatFloat(qty, 'f', -1, 64), "0"), "."), shortID))
	postWebhookExpectOK(t, stack, payload2)

	waitForInt(t, db,
		`SELECT COUNT(*) FROM conversations WHERE business_id = $1 AND customer_phone = $2 AND state = 'INVOICING'`,
		1, 60*time.Second, biz.ID, customerPhone)

	waitForInt(t, db,
		`SELECT COUNT(*) FROM orders WHERE business_id = $1 AND customer_id = (SELECT id FROM customers WHERE business_id = $1 AND phone = $2)`,
		1, 45*time.Second, biz.ID, customerPhone)

	var orderID string
	if err := db.QueryRow(
		`SELECT id FROM orders WHERE business_id = $1 AND customer_id = (SELECT id FROM customers WHERE business_id = $1 AND phone = $2) ORDER BY created_at DESC LIMIT 1`,
		biz.ID, customerPhone).Scan(&orderID); err != nil {
		t.Fatalf("fetch order id: %v", err)
	}

	gotTotal := scalarString(t, db, `SELECT total_amount::text FROM orders WHERE id = $1`, orderID)
	if gotTotal != total {
		t.Errorf("order total = %s, want %s", gotTotal, total)
	}

	var conversationID string
	if err := db.QueryRow(
		`SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
		biz.ID, customerPhone).Scan(&conversationID); err != nil {
		t.Fatalf("fetch conversation id: %v", err)
	}

	return paystackOrderFlow{
		Biz:            biz,
		Prod:           prod,
		ShortID:        shortID,
		CustomerPhone:  customerPhone,
		ConversationID: conversationID,
		OrderID:        orderID,
		Total:          total,
	}
}

func TestT010d_PaystackMinorMajorConversion(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	flow := paystackRunTwoTurnOrder(t, factory, db, stack, "t010d", "25.50", 9, 2)

	inits := paystackInitializeBodies(stack)
	if len(inits) == 0 {
		t.Fatalf("no /transaction/initialize request captured on stub\n--- logs ---\n%s", stack.DumpLogs())
	}
	body := inits[len(inits)-1]

	amt, ok := body["amount"].(float64)
	if !ok {
		t.Fatalf("initialize body amount missing/non-number: %v", body["amount"])
	}
	if amt != 5100 {
		t.Errorf("initialize amount = %v, want 5100 (characterized reality: initiatePayment converts major to MINOR units via Math.round(amount*100))", amt)
	}
	if body["currency"] != "GHS" {
		t.Errorf("initialize currency = %v, want GHS", body["currency"])
	}
	if body["reference"] != flow.OrderID {
		t.Errorf("initialize reference = %v, want order id %s", body["reference"], flow.OrderID)
	}
	if body["callback_url"] != "https://novoapex.com/success" {
		t.Errorf("initialize callback_url = %v, want default https://novoapex.com/success", body["callback_url"])
	}

	meta, ok := body["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("initialize metadata missing: %v", body["metadata"])
	}
	if meta["businessId"] != flow.Biz.ID {
		t.Errorf("metadata.businessId = %v, want %s", meta["businessId"], flow.Biz.ID)
	}
	if meta["customer_phone"] != flow.CustomerPhone {
		t.Errorf("metadata.customer_phone = %v, want %s", meta["customer_phone"], flow.CustomerPhone)
	}

	email, _ := body["email"].(string)
	digits := paystackNonDigitsRe.ReplaceAllString(flow.CustomerPhone, "")
	if !strings.HasSuffix(email, "@customers.novoapex.com") || !strings.HasPrefix(email, digits) {
		t.Errorf("initialize email = %q, want %q + @customers.novoapex.com placeholder", email, digits)
	}
}

func paystackWaitPaymentRow(t *testing.T, h *Harness, db *sql.DB, query string, args []any, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int
	for time.Now().Before(deadline) {
		if err := db.QueryRow(query, args...).Scan(&got); err != nil {
			t.Fatalf("payment row query: %v", err)
		}
		if got == 1 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("payment row not found within %s (query=%q)\nredis failures: %s", timeout, query, paystackQueueFailureDump(h, "payment-events"))
}

func TestT011a_ReconciliationStrategyA_ReferenceMatch(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	flow := paystackRunTwoTurnOrder(t, factory, db, stack, "t011a", "25.50", 9, 2)

	preStatus := scalarString(t, db, `SELECT status::text FROM orders WHERE id = $1`, flow.OrderID)
	if preStatus != "CONFIRMED" {
		t.Errorf("order status before webhook = %s, want CONFIRMED", preStatus)
	}

	payload := paystackChargeSuccessPayload(t, flow.OrderID, 5100, "GHS", flow.CustomerPhone, flow.Biz.ID, "MTN", "2026-08-22T09:30:00.000Z")
	r := paystackPostWebhook(t, stack, payload)
	if r.Status != http.StatusOK {
		t.Fatalf("paystack webhook status = %d body=%s", r.Status, r.Body)
	}
	if parsed := r.JSON(t); parsed["status"] != "ok" {
		t.Errorf("webhook response status field = %v, want \"ok\"", parsed["status"])
	}

	paystackWaitPaymentRow(t, h, db,
		`SELECT COUNT(*) FROM payments
		 WHERE external_reference = $1 AND order_id = $1 AND business_id = $2
		   AND amount::text = '51.00' AND currency = 'GHS' AND provider = 'paystack'
		   AND status = 'SUCCESS' AND network = 'MTN'
		   AND reconciled_at IS NOT NULL AND paid_at IS NOT NULL
		   AND customer_id = (SELECT id FROM customers WHERE business_id = $2 AND phone = $3)`,
		[]any{flow.OrderID, flow.Biz.ID, flow.CustomerPhone}, 90*time.Second)

	deadline := time.Now().Add(90 * time.Second)
	var orderStatus string
	for time.Now().Before(deadline) {
		orderStatus = scalarString(t, db, `SELECT status::text FROM orders WHERE id = $1`, flow.OrderID)
		if orderStatus == "PAID" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if orderStatus != "PAID" {
		t.Errorf("order status after webhook stuck at %s, want PAID\nredis failures: %s", orderStatus, paystackQueueFailureDump(h, "payment-events"))
	}

	waitForInt(t, db,
		`SELECT COUNT(*) FROM outbound_messages
		 WHERE conversation_id = $1 AND message_type = 'text' AND text_content LIKE '%Payment of%GHS 51.00%received%'`,
		1, 60*time.Second, flow.ConversationID)

	waitForInt(t, db,
		`SELECT COUNT(*) FROM scheduled_follow_ups WHERE order_id = $1 AND cancelled_at IS NOT NULL`,
		2, 45*time.Second, flow.OrderID)
}

func paystackExpectNoPaymentRow(t *testing.T, db *sql.DB, externalReference string, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		var got int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM payments WHERE external_reference = $1`, externalReference).Scan(&got); err != nil {
			t.Fatalf("count payments: %v", err)
		}
		if got != 0 {
			t.Fatalf("ambiguous webhook unexpectedly created a payment row for reference %s", externalReference)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestT011b_ReconciliationStrategyB_PhoneAmountAndAmbiguity(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	flow := paystackRunTwoTurnOrder(t, factory, db, stack, "t011b", "25.50", 9, 2)

	var customerID string
	if err := db.QueryRow(
		`SELECT id FROM customers WHERE business_id = $1 AND phone = $2`,
		flow.Biz.ID, flow.CustomerPhone).Scan(&customerID); err != nil {
		t.Fatalf("fetch orchestrator-created customer: %v", err)
	}

	ambRef := fmt.Sprintf("unknown-ref-amb-%d", time.Now().UnixNano())
	factory.exec("seed-ambiguous-order", `INSERT INTO orders
		(id, business_id, customer_id, status, total_amount, currency, updated_at)
		VALUES ($1, $2, $3, 'CONFIRMED', 51.00, 'GHS', NOW())`,
		"ord_amb_"+fmt.Sprint(time.Now().UnixNano()), flow.Biz.ID, customerID)

	ambPayload := paystackChargeSuccessPayload(t, ambRef, 5100, "GHS", flow.CustomerPhone, flow.Biz.ID, "MTN", "2026-08-22T09:40:00.000Z")
	ambResp := paystackPostWebhook(t, stack, ambPayload)
	if ambResp.Status != http.StatusOK {
		t.Fatalf("ambiguity probe webhook status = %d body=%s", ambResp.Status, ambResp.Body)
	}

	paystackExpectNoPaymentRow(t, db, ambRef, 15*time.Second)

	statusA := scalarString(t, db, `SELECT status::text FROM orders WHERE id = $1`, flow.OrderID)
	if statusA != "CONFIRMED" {
		t.Errorf("pipeline order changed to %s after ambiguous webhook, want CONFIRMED (reality: ambiguous matches reconcile nothing)", statusA)
	}
	var seededCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM orders WHERE business_id = $1 AND customer_id = $2 AND status = 'CONFIRMED' AND total_amount::text = '51.00'`,
		flow.Biz.ID, customerID).Scan(&seededCount); err != nil {
		t.Fatalf("count confirmed 51.00 orders: %v", err)
	}
	if seededCount != 2 {
		t.Errorf("confirmed 51.00 orders = %d, want 2 (pipeline order + seeded ambiguity twin)", seededCount)
	}

	matchRef := fmt.Sprintf("unknown-ref-match-%d", time.Now().UnixNano())
	unambID := "ord_unamb_" + fmt.Sprint(time.Now().UnixNano())
	factory.exec("seed-strategyb-order", `INSERT INTO orders
		(id, business_id, customer_id, status, total_amount, currency, updated_at)
		VALUES ($1, $2, $3, 'CONFIRMED', 12.34, 'GHS', NOW())`,
		unambID, flow.Biz.ID, customerID)

	matchPayload := paystackChargeSuccessPayload(t, matchRef, 1234, "GHS", flow.CustomerPhone, flow.Biz.ID, "VOD", "2026-08-22T09:50:00.000Z")
	matchResp := paystackPostWebhook(t, stack, matchPayload)
	if matchResp.Status != http.StatusOK {
		t.Fatalf("strategy-B webhook status = %d body=%s", matchResp.Status, matchResp.Body)
	}

	paystackWaitPaymentRow(t, h, db,
		`SELECT COUNT(*) FROM payments
		 WHERE external_reference = $1 AND status = 'SUCCESS' AND amount::text = '12.34'
		   AND provider = 'paystack' AND reconciled_at IS NOT NULL AND order_id = $2`,
		[]any{matchRef, unambID}, 90*time.Second)

	gotStatus := scalarString(t, db, `SELECT status::text FROM orders WHERE id = $1`, unambID)
	if gotStatus != "PAID" {
		t.Errorf("strategy-B matched order %s status = %s, want PAID\nredis failures: %s", unambID, gotStatus, paystackQueueFailureDump(h, "payment-events"))
	}
	if statusAfter := scalarString(t, db, `SELECT status::text FROM orders WHERE id = $1`, flow.OrderID); statusAfter != "CONFIRMED" {
		t.Errorf("untouched pipeline order moved to %s, want CONFIRMED", statusAfter)
	}

	waitForInt(t, db,
		`SELECT COUNT(*) FROM outbound_messages
		 WHERE conversation_id = $1 AND message_type = 'text' AND text_content LIKE '%Payment of%GHS 12.34%received%'`,
		1, 60*time.Second, flow.ConversationID)
}

func TestT015_PaymentCopyCurrencyMethods(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	flow := paystackRunTwoTurnOrder(t, factory, db, stack, "t015", "25.50", 9, 2)

	var invoiceText string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		err := db.QueryRow(
			`SELECT text_content FROM outbound_messages
			 WHERE conversation_id = $1 AND text_content LIKE '%checkout.paystack.test%'
			 ORDER BY created_at DESC LIMIT 1`,
			flow.ConversationID).Scan(&invoiceText)
		if err == nil {
			break
		}
		if err != sql.ErrNoRows {
			t.Fatalf("query invoice text: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if invoiceText == "" {
		t.Fatal("invoice outbound message with checkout link never appeared")
	}

	if !strings.Contains(invoiceText, "*GHS 51.00*") {
		t.Errorf("invoice copy missing formatted total *GHS 51.00*:\n%s", invoiceText)
	}
	if !strings.Contains(invoiceText, "Mobile Money, card, or bank transfer") {
		t.Errorf("invoice copy missing exact GHS paymentMethods string:\n%s", invoiceText)
	}

	stripped := paystackURLRe.ReplaceAllString(invoiceText, "")
	if strings.Contains(strings.ToLower(stripped), "paystack") {
		t.Errorf("processor brand name leaked into prose (outside URLs):\n%s", stripped)
	}
	if !strings.Contains(strings.ToLower(invoiceText), "paystack") {
		t.Errorf("expected brand to appear ONLY inside the checkout URL host; full text lacks it entirely — URL missing?\n%s", invoiceText)
	}
}

type moneySeedPayment struct {
	ID     string
	Amount string
	Status string
}

func moneySeedPayments(t *testing.T, db *sql.DB, businessID string, rows []moneySeedPayment) {
	t.Helper()
	for _, p := range rows {
		if _, err := db.Exec(
			`INSERT INTO payments (id, business_id, amount, currency, provider, status, created_at)
			 VALUES ($1, $2, $3, 'GHS', 'paystack', $4, NOW())`,
			p.ID, businessID, p.Amount, p.Status); err != nil {
			t.Fatalf("seed payment %s: %v", p.ID, err)
		}
	}
}

func moneyNear(got float64, want float64) bool {
	return math.Abs(got-want) <= 0.001
}

func moneyAssertBalance(t *testing.T, body []byte, revenue, committed, available float64, currency string) {
	t.Helper()
	var bal struct {
		TotalRevenue     float64 `json:"totalRevenue"`
		CommittedPayouts float64 `json:"committedPayouts"`
		AvailableBalance float64 `json:"availableBalance"`
		Currency         string  `json:"currency"`
	}
	if err := json.Unmarshal(body, &bal); err != nil {
		t.Fatalf("decode balance response: %v (%s)", err, body)
	}
	if !moneyNear(bal.TotalRevenue, revenue) {
		t.Errorf("totalRevenue = %v, want %v (reality: sum of SUCCESS payments only)", bal.TotalRevenue, revenue)
	}
	if !moneyNear(bal.CommittedPayouts, committed) {
		t.Errorf("committedPayouts = %v, want %v", bal.CommittedPayouts, committed)
	}
	if !moneyNear(bal.AvailableBalance, available) {
		t.Errorf("availableBalance = %v, want %v", bal.AvailableBalance, available)
	}
	if bal.Currency != currency {
		t.Errorf("balance currency = %q, want %q", bal.Currency, currency)
	}
}

func TestT010abc_MoneyAggregatesProfileAccumulation(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()
	prodA := factory.Product(biz.ID, WithStock(40), WithPrice("25.50"))
	prodB := factory.Product(biz.ID, WithStock(40), WithPrice("25.51"))
	shortA := prodA.ID[:8]
	shortB := prodB.ID[:8]

	customerPhone := biz.OwnerPhone

	turn := 0
	stack.SetChatReply(func(_ map[string]any) map[string]any {
		turn++
		switch turn {
		case 1:
			return inquiryReplyForTest(shortA)
		case 2:
			return orderConfirmedReply(shortA, 1)
		default:
			return orderConfirmedReply(shortB, 1)
		}
	})

	profileCounts := func() (orders int, spent string, avg string) {
		t.Helper()
		if err := db.QueryRow(
			`SELECT total_orders, COALESCE(total_spent::text,''), COALESCE(average_order_value::text,'')
			 FROM customer_profiles
			 WHERE customer_id = (SELECT id FROM customers WHERE business_id = $1 AND phone = $2)`,
			biz.ID, customerPhone).Scan(&orders, &spent, &avg); err != nil {
			t.Fatalf("read profile stats: %v", err)
		}
		return orders, spent, avg
	}

	sendMsg := func(suffix, body string) {
		t.Helper()
		id := fmt.Sprintf("wamid.t010abc-%s-%d", suffix, time.Now().UnixNano())
		payload := buildWhatsAppPayload("waba-t010abc", biz.WhatsAppPhoneNumberID, id, customerPhone, body)
		postWebhookExpectOK(t, stack, payload)
	}

	sendMsg("a", fmt.Sprintf("Do you have [ID:%s] in stock?", shortA))
	waitForInt(t, db,
		`SELECT COUNT(*) FROM conversations WHERE business_id = $1 AND customer_phone = $2 AND state = 'BROWSING'`,
		1, 60*time.Second, biz.ID, customerPhone)

	sendMsg("b", fmt.Sprintf("Confirm 1 of [ID:%s]", shortA))
	waitForInt(t, db, profileOrdersQuery(), 1, 60*time.Second, biz.ID, customerPhone)

	sendMsg("c", fmt.Sprintf("Also add 1 of [ID:%s]", shortB))

	deadline := time.Now().Add(90 * time.Second)
	var orders int
	var spent, avg string
	for time.Now().Before(deadline) {
		orders, spent, avg = profileCounts()
		if orders == 2 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if orders != 2 {
		t.Fatalf("profile total_orders stuck at %d, want 2 within timeout", orders)
	}

	if spent != "51.01" {
		t.Errorf("total_spent = %s, want 51.01 (25.50 + 25.51 exact Decimal accumulation)", spent)
	}
	if avg != "25.51" {
		t.Errorf("average_order_value = %s, want 25.51 (characterized reality: Decimal 51.01/2 = 25.505 rounds half-away-from-zero at DECIMAL(14,2) storage)", avg)
	}

	var lastOrderAt, freqDays sql.NullString
	if err := db.QueryRow(
		`SELECT last_order_at::text, order_frequency_days::text
		 FROM customer_profiles
		 WHERE customer_id = (SELECT id FROM customers WHERE business_id = $1 AND phone = $2)`,
		biz.ID, customerPhone).Scan(&lastOrderAt, &freqDays); err != nil {
		t.Fatalf("read nullable profile stats: %v", err)
	}
	if !lastOrderAt.Valid {
		t.Error("last_order_at must be set after an order")
	}
	if !freqDays.Valid {
		t.Error("order_frequency_days must be set once total_orders > 1")
	}

	var orderCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM orders WHERE business_id = $1 AND customer_id = (SELECT id FROM customers WHERE business_id = $1 AND phone = $2)`,
		biz.ID, customerPhone).Scan(&orderCount); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if orderCount != 2 {
		t.Errorf("orders in DB = %d, want 2 (one per confirmed turn)", orderCount)
	}
}

func profileOrdersQuery() string {
	return `SELECT total_orders FROM customer_profiles
		WHERE customer_id = (SELECT id FROM customers WHERE business_id = $1 AND phone = $2)`
}
