package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

type s7a_stack struct {
	h    *harness.Harness
	db   *sql.DB
	pool *pgxpool.Pool
}

func s7a_requireDocker(t *testing.T) {
	t.Helper()
	sock := os.Getenv("DOCKER_HOST")
	if sock == "" || !strings.HasPrefix(sock, "unix://") {
		sock = "unix:///var/run/docker.sock"
	}
	path := strings.TrimPrefix(sock, "unix://")
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Skipf("docker ping request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Skipf("docker daemon unavailable at %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
}

func s7a_startStack(t *testing.T) *s7a_stack {
	t.Helper()
	s7a_requireDocker(t)

	ctx := t.Context()
	h, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })

	repoDir, err := harness.NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	if err := harness.ApplyPrismaMigrations(ctx, repoDir, h.PostgresDSN); err != nil {
		t.Fatalf("prisma migrate deploy: %v", err)
	}

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	pool, err := pgxpool.New(ctx, h.PostgresDSN)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		_ = db.Close()
	})

	return &s7a_stack{h: h, db: db, pool: pool}
}

// s7a_fakePublisher records Enqueue calls. DedupTaskIDs mimics asynq's
// Task-ID uniqueness so double-enqueue attempts fail like the real bridge.
type s7a_fakePublisher struct {
	mu           sync.Mutex
	DedupTaskIDs bool
	Fail         error
	calls        []s7a_enqueueCall
	taskIDs      map[string]bool
}

type s7a_enqueueCall struct {
	Queue    string
	TaskType string
	Payload  []byte
	Opts     *queue.EnqueueOpts
}

func (f *s7a_fakePublisher) Enqueue(ctx context.Context, q, taskType string, payload any, opts *queue.EnqueueOpts) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Fail != nil {
		return f.Fail
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	call := s7a_enqueueCall{Queue: q, TaskType: taskType, Payload: raw, Opts: opts}
	if f.DedupTaskIDs && opts != nil && opts.TaskID != "" {
		if f.taskIDs == nil {
			f.taskIDs = map[string]bool{}
		}
		if f.taskIDs[opts.TaskID] {
			return fmt.Errorf("asynq: task ID conflicts: %s", opts.TaskID)
		}
		f.taskIDs[opts.TaskID] = true
	}
	f.calls = append(f.calls, call)
	return nil
}

func (f *s7a_fakePublisher) recorded() []s7a_enqueueCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]s7a_enqueueCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *s7a_fakePublisher) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func s7a_deps(s *s7a_stack, pub queue.Publisher) Deps {
	return Deps{Pool: s.pool, Publisher: pub}
}

func s7a_count(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func s7a_scalar(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var v sql.NullString
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("scalar query %q: %v", query, err)
	}
	return v.String
}

func s7a_seedOrder(t *testing.T, db *sql.DB, bizID, custID, convID, status, total string) string {
	t.Helper()
	id := "ord_" + uuid.NewString()
	if _, err := db.Exec(`
		INSERT INTO orders (id, business_id, customer_id, conversation_id, status, total_amount, currency, updated_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, 'GHS', NOW())`,
		id, bizID, custID, convID, status, total); err != nil {
		t.Fatalf("seed order: %v", err)
	}
	return id
}

func s7a_seedFollowUp(t *testing.T, db *sql.DB, orderID, bizID, custID, jobType string, scheduledAt time.Time) string {
	t.Helper()
	id := "sfu_" + uuid.NewString()
	if _, err := db.Exec(`
		INSERT INTO scheduled_follow_ups (id, order_id, business_id, customer_id, job_type, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id, orderID, bizID, custID, jobType, scheduledAt.UTC()); err != nil {
		t.Fatalf("seed follow-up: %v", err)
	}
	return id
}

func s7a_seedOutbound(t *testing.T, db *sql.DB, bizID, convID, phone, status string, createdAt time.Time) string {
	t.Helper()
	id := "om_" + uuid.NewString()
	if _, err := db.Exec(`
		INSERT INTO outbound_messages (id, business_id, conversation_id, recipient_phone, message_type, raw_payload, status, created_at)
		VALUES ($1, $2, NULLIF($3, ''), $4, 'text', '{}', $5, $6)`,
		id, bizID, convID, phone, status, createdAt.UTC()); err != nil {
		t.Fatalf("seed outbound: %v", err)
	}
	return id
}

func s7a_renameCustomer(t *testing.T, db *sql.DB, customerID, name string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE customers SET name = $2 WHERE id = $1`, customerID, name); err != nil {
		t.Fatalf("rename customer: %v", err)
	}
}

func s7a_chargeSuccessBody(reference string, amountMinor int64, currency, phone, businessID, bank, paidAtISO string) []byte {
	body := map[string]any{
		"event": "charge.success",
		"data": map[string]any{
			"id": 1234567, "reference": reference, "amount": amountMinor, "currency": currency,
			"customer":      map[string]any{"phone": phone},
			"authorization": map[string]any{"bank": bank},
			"metadata": map[string]any{
				"businessId":     businessID,
				"customer_phone": phone,
			},
			"paid_at": paidAtISO,
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return raw
}

func s7a_chargeSuccessEvent(reference string, amountMinor int64, currency, phone, businessID, bank string) paystack.NormalisedPaymentEvent {
	paidAt := time.Date(2026, 8, 22, 9, 30, 0, 0, time.UTC)
	return paystack.NormalisedPaymentEvent{
		Provider:          "paystack",
		Event:             "charge.success",
		Reference:         reference,
		AmountMinor:       amountMinor,
		Currency:          currency,
		Status:            "success",
		PaidAt:            paidAt,
		BusinessID:        businessID,
		AuthorizationBank: bank,
		RawJSON: s7a_chargeSuccessBody(reference, amountMinor, currency, phone, businessID, bank,
			"2026-08-22T09:30:00.000Z"),
	}
}

const s7a_confirmationTextWant = "✅ Payment of *GHS 51.00* received for your order. Thank you! 🎉\n\nYour order is now being processed."

func TestS7A_PaymentStrategyA_ReconcileConfirmCancelFollowUps(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	s7a_renameCustomer(t, s.db, cust.ID, "Amara")
	conv := factory.Conversation(biz.ID, cust.Phone)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, conv.ID, "CONFIRMED", "51.00")
	sfu1 := s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart", time.Now().Add(2*time.Hour))
	sfu2 := s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "unpaid-invoice-first", time.Now().Add(24*time.Hour))

	evt := s7a_chargeSuccessEvent(orderID, 5100, "GHS", cust.Phone, biz.ID, "MTN")
	if err := HandlePaymentEvent(t.Context(), s7a_deps(s, pub), evt); err != nil {
		t.Fatalf("HandlePaymentEvent: %v", err)
	}

	if got := s7a_count(t, s.db, `SELECT COUNT(*) FROM payments WHERE external_reference = $1`, orderID); got != 1 {
		t.Fatalf("payment rows = %d, want 1", got)
	}
	var payBiz, payCust, payOrder, payStatus, payNetwork, payAmount string
	var rawStored []byte
	if err := s.db.QueryRow(`
			SELECT business_id, customer_id, order_id, status::text, network, amount::text, raw_webhook_payload::text
			  FROM payments WHERE external_reference = $1`, orderID).
		Scan(&payBiz, &payCust, &payOrder, &payStatus, &payNetwork, &payAmount, &rawStored); err != nil {
		t.Fatalf("read payment: %v", err)
	}
	if payBiz != biz.ID || payCust != cust.ID || payOrder != orderID {
		t.Errorf("payment linkage biz=%s cust=%s order=%s", payBiz, payCust, payOrder)
	}
	if payStatus != "SUCCESS" || payNetwork != "MTN" || payAmount != "51.00" {
		t.Errorf("payment status=%s network=%s amount=%s, want SUCCESS/MTN/51.00 (major units)", payStatus, payNetwork, payAmount)
	}
	if recon := s7a_scalar(t, s.db, `SELECT COALESCE(reconciled_at::text,'') FROM payments WHERE external_reference = $1`, orderID); recon == "" {
		t.Error("reconciled_at not set on Strategy A reconcile")
	}
	if paid := s7a_scalar(t, s.db, `SELECT COALESCE(paid_at::text,'') FROM payments WHERE external_reference = $1`, orderID); paid == "" {
		t.Error("paid_at not persisted from event")
	}
	var storedAny, origAny any
	if err := json.Unmarshal(rawStored, &storedAny); err != nil {
		t.Fatalf("raw_webhook_payload not valid JSON: %v", err)
	}
	if err := json.Unmarshal(evt.RawJSON, &origAny); err != nil {
		t.Fatalf("event raw body not valid JSON: %v", err)
	}
	if !reflect.DeepEqual(storedAny, origAny) {
		t.Errorf("raw_webhook_payload altered:\n got %s\nwant %s", rawStored, evt.RawJSON)
	}

	if status := s7a_scalar(t, s.db, `SELECT status::text FROM orders WHERE id = $1`, orderID); status != "PAID" {
		t.Errorf("order status = %s, want PAID", status)
	}

	var omID, omText, omStatus string
	var omRaw []byte
	if err := s.db.QueryRow(`
			SELECT id, text_content, status, raw_payload::text FROM outbound_messages
			 WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1`, conv.ID).
		Scan(&omID, &omText, &omStatus, &omRaw); err != nil {
		t.Fatalf("confirmation outbound row: %v", err)
	}
	if omText != s7a_confirmationTextWant {
		t.Errorf("confirmation text:\n got %q\nwant %q", omText, s7a_confirmationTextWant)
	}
	if omStatus != "pending" {
		t.Errorf("confirmation status = %s, want pending", omStatus)
	}
	var rawDecoded struct {
		MessagingProduct string `json:"messaging_product"`
		To               string `json:"to"`
		Type             string `json:"type"`
		Text             struct {
			Body string `json:"body"`
		} `json:"text"`
	}
	if err := json.Unmarshal(omRaw, &rawDecoded); err != nil {
		t.Fatalf("decode raw_payload: %v", err)
	}
	if rawDecoded.MessagingProduct != "whatsapp" || rawDecoded.To != cust.Phone ||
		rawDecoded.Type != "text" || rawDecoded.Text.Body != s7a_confirmationTextWant {
		t.Errorf("raw_payload shape mismatch: %+v", rawDecoded)
	}

	calls := pub.recorded()
	if len(calls) != 1 {
		t.Fatalf("publisher calls = %d, want 1 (persist row THEN publish)", len(calls))
	}
	c := calls[0]
	if c.Queue != queue.QOutbound || c.TaskType != queue.TaskOutboundSend {
		t.Errorf("enqueue target = %s/%s, want %s/%s", c.Queue, c.TaskType, queue.QOutbound, queue.TaskOutboundSend)
	}
	var sendJob outboundSendJob
	if err := json.Unmarshal(c.Payload, &sendJob); err != nil {
		t.Fatalf("decode outbound job: %v", err)
	}
	if sendJob.OutboundMessageID != omID {
		t.Errorf("enqueued outboundMessageId = %s, want persisted row id %s", sendJob.OutboundMessageID, omID)
	}

	for _, sfu := range []string{sfu1, sfu2} {
		if cancelled := s7a_scalar(t, s.db, `SELECT COALESCE(cancelled_at::text,'') FROM scheduled_follow_ups WHERE id = $1`, sfu); cancelled == "" {
			t.Errorf("follow-up %s not cancelled after payment", sfu)
		}
	}

	golden, _ := json.Marshal(map[string]any{
		"textContent": omText,
		"rawPayload":  json.RawMessage(strings.ReplaceAll(string(omRaw), cust.Phone, "{{phone}}")),
	})
	harness.AssertJSONGolden(t, "s7a_payment_confirmation", golden)
}

func TestS7A_PaymentDuplicateEvent_IsIdempotentSingleRow(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	conv := factory.Conversation(biz.ID, cust.Phone)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, conv.ID, "CONFIRMED", "51.00")

	evt := s7a_chargeSuccessEvent(orderID, 5100, "GHS", cust.Phone, biz.ID, "MTN")
	if err := HandlePaymentEvent(t.Context(), s7a_deps(s, pub), evt); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if err := HandlePaymentEvent(t.Context(), s7a_deps(s, pub), evt); err != nil {
		t.Fatalf("duplicate handle: %v", err)
	}

	if got := s7a_count(t, s.db, `SELECT COUNT(*) FROM payments WHERE external_reference = $1`, orderID); got != 1 {
		t.Errorf("payment rows after duplicate = %d, want 1", got)
	}
	if got := s7a_count(t, s.db, `SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, conv.ID); got != 1 {
		t.Errorf("outbound rows after duplicate = %d, want 1", got)
	}
	if len(pub.recorded()) != 1 {
		t.Errorf("publisher calls after duplicate = %d, want 1", len(pub.recorded()))
	}
}

func TestS7A_PaymentConcurrentDuplicates_NeverTwoRows(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	conv := factory.Conversation(biz.ID, cust.Phone)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, conv.ID, "CONFIRMED", "51.00")

	evt := s7a_chargeSuccessEvent(orderID, 5100, "GHS", cust.Phone, biz.ID, "MTN")

	const racers = 8
	errs := make(chan error, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- HandlePaymentEvent(t.Context(), s7a_deps(s, pub), evt)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent duplicate returned err: %v (P2002/unique race must be swallowed)", err)
		}
	}
	if got := s7a_count(t, s.db, `SELECT COUNT(*) FROM payments WHERE external_reference = $1`, orderID); got != 1 {
		t.Errorf("payment rows after %d racers = %d, want exactly 1", racers, got)
	}
}

func TestS7A_PaymentStrategyB_SingleCandidateReconciles(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	conv := factory.Conversation(biz.ID, cust.Phone)
	matchID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, conv.ID, "PAYMENT_PENDING", "12.34")
	unrelatedID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "77.77")

	evt := s7a_chargeSuccessEvent("unknown-ref-match", 1234, "GHS", cust.Phone, biz.ID, "VOD")
	if err := HandlePaymentEvent(t.Context(), s7a_deps(s, pub), evt); err != nil {
		t.Fatalf("HandlePaymentEvent: %v", err)
	}

	if got := s7a_count(t, s.db, `SELECT COUNT(*) FROM payments WHERE external_reference = $1`, "unknown-ref-match"); got != 1 {
		t.Fatalf("payment rows = %d, want 1", got)
	}
	payOrder := s7a_scalar(t, s.db, `SELECT COALESCE(order_id,'') FROM payments WHERE external_reference = $1`, "unknown-ref-match")
	if payOrder != matchID {
		t.Errorf("payment reconciled to order %s, want %s", payOrder, matchID)
	}
	if amount := s7a_scalar(t, s.db, `SELECT amount::text FROM payments WHERE external_reference = $1`, "unknown-ref-match"); amount != "12.34" {
		t.Errorf("amount = %s, want 12.34", amount)
	}
	if network := s7a_scalar(t, s.db, `SELECT COALESCE(network,'') FROM payments WHERE external_reference = $1`, "unknown-ref-match"); network != "VOD" {
		t.Errorf("network = %s, want VOD", network)
	}
	if status := s7a_scalar(t, s.db, `SELECT status::text FROM orders WHERE id = $1`, matchID); status != "PAID" {
		t.Errorf("matched order status = %s, want PAID", status)
	}
	if status := s7a_scalar(t, s.db, `SELECT status::text FROM orders WHERE id = $1`, unrelatedID); status != "CONFIRMED" {
		t.Errorf("untouched order moved to %s, want CONFIRMED", status)
	}
	if recon := s7a_scalar(t, s.db, `SELECT COALESCE(reconciled_at::text,'') FROM payments WHERE external_reference = $1`, "unknown-ref-match"); recon == "" {
		t.Error("reconciled_at not set on Strategy B match")
	}
}

func TestS7A_PaymentAmbiguousMatch_CreatesNoPaymentRow(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	bizA := factory.Business()
	custA := factory.Customer(bizA.ID)
	orderA := s7a_seedOrder(t, s.db, bizA.ID, custA.ID, "", "CONFIRMED", "51.00")

	bizB := factory.Business()
	custB := factory.Customer(bizB.ID)
	if _, err := s.db.Exec(`UPDATE customers SET phone = $2 WHERE id = $1`, custB.ID, custA.Phone); err != nil {
		t.Fatalf("share phone across tenants: %v", err)
	}
	orderB := s7a_seedOrder(t, s.db, bizB.ID, custB.ID, "", "CONFIRMED", "51.00")

	ref := fmt.Sprintf("unknown-ref-amb-%d", time.Now().UnixNano())
	evt := s7a_chargeSuccessEvent(ref, 5100, "GHS", custA.Phone, "", "MTN")
	if err := HandlePaymentEvent(t.Context(), s7a_deps(s, pub), evt); err != nil {
		t.Fatalf("HandlePaymentEvent: %v (ambiguous must abandon quietly)", err)
	}

	if got := s7a_count(t, s.db, `SELECT COUNT(*) FROM payments`); got != 0 {
		t.Errorf("payments created = %d, want 0 for ambiguous phone+amount match", got)
	}
	for _, id := range []string{orderA, orderB} {
		if status := s7a_scalar(t, s.db, `SELECT status::text FROM orders WHERE id = $1`, id); status != "CONFIRMED" {
			t.Errorf("order %s moved to %s, want CONFIRMED (ambiguous reconciles nothing)", id, status)
		}
	}
	if len(pub.recorded()) != 0 {
		t.Errorf("publisher calls = %d, want 0", len(pub.recorded()))
	}
}

func TestS7A_PaymentAmountMismatch_WarnsOnlyNoRow(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "51.00")

	evt := s7a_chargeSuccessEvent(orderID, 5000, "GHS", "+233209999999", biz.ID, "MTN")
	if err := HandlePaymentEvent(t.Context(), s7a_deps(s, pub), evt); err != nil {
		t.Fatalf("HandlePaymentEvent: %v (mismatch must warn and fall through)", err)
	}

	if got := s7a_count(t, s.db, `SELECT COUNT(*) FROM payments`); got != 0 {
		t.Errorf("payments created = %d, want 0 when no tenant matches the phone", got)
	}
	if status := s7a_scalar(t, s.db, `SELECT status::text FROM orders WHERE id = $1`, orderID); status != "CONFIRMED" {
		t.Errorf("order moved to %s on amount mismatch, want CONFIRMED", status)
	}
	if len(pub.recorded()) != 0 {
		t.Errorf("publisher calls = %d, want 0", len(pub.recorded()))
	}
}

func TestS7A_PaymentUnmatchedSingleTenant_AttributesCustomer(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "99.99")

	ref := fmt.Sprintf("unknown-ref-unmatched-%d", time.Now().UnixNano())
	evt := s7a_chargeSuccessEvent(ref, 1234, "GHS", cust.Phone, biz.ID, "MTN")
	if err := HandlePaymentEvent(t.Context(), s7a_deps(s, pub), evt); err != nil {
		t.Fatalf("HandlePaymentEvent: %v", err)
	}

	if got := s7a_count(t, s.db, `SELECT COUNT(*) FROM payments WHERE external_reference = $1`, ref); got != 1 {
		t.Fatalf("payment rows = %d, want 1 attributed payment", got)
	}
	var payCust, payOrder string
	if err := s.db.QueryRow(`
			SELECT COALESCE(customer_id,''), COALESCE(order_id,'')
			  FROM payments WHERE external_reference = $1`, ref).
		Scan(&payCust, &payOrder); err != nil {
		t.Fatalf("read attributed payment: %v", err)
	}
	if payCust != cust.ID {
		t.Errorf("customer_id = %q, want %q", payCust, cust.ID)
	}
	if payOrder != "" {
		t.Errorf("order_id = %q, want empty (no matching order)", payOrder)
	}
	if recon := s7a_scalar(t, s.db, `SELECT COALESCE(reconciled_at::text,'') FROM payments WHERE external_reference = $1`, ref); recon != "" {
		t.Errorf("reconciled_at = %q, want NULL for unmatched attribution", recon)
	}
	if status := s7a_scalar(t, s.db, `SELECT status::text FROM payments WHERE external_reference = $1`, ref); status != "SUCCESS" {
		t.Errorf("status = %s, want SUCCESS", status)
	}
}

func TestS7A_RegisterPaymentEvents_DecodesAndParsesPayload(t *testing.T) {
	reg := &s7a_fakeRegistrar{}
	deps := Deps{}
	RegisterPaymentEvents(reg, deps)

	if reg.taskType != queue.TaskPaymentProcess {
		t.Fatalf("registered task type = %q, want %q", reg.taskType, queue.TaskPaymentProcess)
	}

	if err := reg.handler(t.Context(), []byte(`{not-json`)); err == nil {
		t.Error("malformed payload must error")
	} else if !strings.Contains(err.Error(), "decode") && !strings.Contains(err.Error(), "invalid") {
		t.Errorf("decode error should mention decoding: %v", err)
	}

	raw := s7a_chargeSuccessBody("ref-1", 5100, "GHS", "+233200000001", "biz", "MTN", "2026-08-22T09:30:00.000Z")
	payload, _ := json.Marshal(PaymentEventJob{ProviderName: "paystack", RawPayload: raw})
	err := reg.handler(t.Context(), payload)
	// Stage 5 (T5.13) implemented paystack.ParseWebhookEvent, so a valid
	// payload now parses and flows into HandlePaymentEvent — which fails fast
	// on this test's deliberately empty Deps{} ("nil pool"). Both that
	// fail-fast and the pre-Stage-5 sentinel are acceptable here.
	if err != nil &&
		!errors.Is(err, paystack.ErrParseNotImplemented) &&
		!strings.Contains(err.Error(), "nil pool") {
		t.Errorf("valid payload: unexpected error %v (parse pending Stage 5 or nil-pool dep failure acceptable)", err)
	}
}

type s7a_fakeRegistrar struct {
	taskType string
	handler  queue.Handler
}

func (r *s7a_fakeRegistrar) Register(taskType string, h queue.Handler) {
	r.taskType = taskType
	r.handler = h
}
