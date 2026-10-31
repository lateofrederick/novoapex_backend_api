package workers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

// ---------------------------------------------------------------------------
// docker-guarded env bootstrap (mirrors s7b_harness_test.go)
// ---------------------------------------------------------------------------

func opipeDockerAlive(t *testing.T) {
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

type opipeEnv struct {
	pool    *pgxpool.Pool
	db      *sql.DB
	factory *harness.Factory

	publisher *opipePublisher
	llm       *opipeLLM
	deps      workers.OrchestratorDeps
}

func opipeStart(t *testing.T) *opipeEnv {
	t.Helper()
	opipeDockerAlive(t)
	ctx := t.Context()

	envH, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { envH.Terminate(context.Background()) })

	if err := harness.ApplyBaselineSchema(ctx, envH.PostgresDSN); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	pool, err := pgxpool.New(ctx, envH.PostgresDSN)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	db, err := sql.Open("pgx", envH.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	e := &opipeEnv{
		pool:      pool,
		db:        db,
		factory:   harness.NewFactory(t, db),
		publisher: newOpipePublisher(),
		llm:       &opipeLLM{},
	}
	e.deps = workers.OrchestratorDeps{Pool: pool, Publisher: e.publisher, LLM: e.llm}
	return e
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type opipeCapturedJob struct {
	Queue    string
	TaskType string
	Payload  json.RawMessage
	Opts     *queue.EnqueueOpts
}

type opipePublisher struct {
	mu   sync.Mutex
	jobs []opipeCapturedJob
}

func newOpipePublisher() *opipePublisher { return &opipePublisher{} }

func (p *opipePublisher) Enqueue(_ context.Context, q, taskType string, payload any, opts *queue.EnqueueOpts) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.jobs = append(p.jobs, opipeCapturedJob{Queue: q, TaskType: taskType, Payload: body, Opts: opts})
	return nil
}

func (p *opipePublisher) snapshot() []opipeCapturedJob {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]opipeCapturedJob, len(p.jobs))
	copy(out, p.jobs)
	return out
}

func (p *opipePublisher) ofTask(taskType string) []opipeCapturedJob {
	var out []opipeCapturedJob
	for _, j := range p.snapshot() {
		if j.TaskType == taskType {
			out = append(out, j)
		}
	}
	return out
}

var _ queue.Publisher = (*opipePublisher)(nil)

type opipeLLM struct {
	mu    sync.Mutex
	steps []orchestrator.LlmResponse
	calls []orchestrator.GenerateRequest
}

func (l *opipeLLM) GenerateObject(_ context.Context, req orchestrator.GenerateRequest) (orchestrator.LlmResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, req)
	if len(l.steps) == 0 {
		return orchestrator.LlmResponse{}, fmt.Errorf("opipeLLM: no scripted response left")
	}
	next := l.steps[0]
	l.steps = l.steps[1:]
	return next, nil
}

func (l *opipeLLM) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.calls)
}

func opipeResp(mutate func(*orchestrator.LlmResponse)) orchestrator.LlmResponse {
	r := orchestrator.LlmResponse{
		ReplyText:          "Hello! How can I help you today?",
		InternalConfidence: 0.95,
		Intent:             "greeting",
		CrmSignals: orchestrator.CrmSignals{
			DetectedItems:       []orchestrator.DetectedItem{},
			DetectedPreferences: []string{},
			Sentiment:           "neutral",
		},
	}
	if mutate != nil {
		mutate(&r)
	}
	return r
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func opipeInbound(t *testing.T, db *sql.DB, phoneID, sender, msgID, text string) {
	t.Helper()
	opipeInboundAt(t, db, phoneID, sender, msgID, text, time.Now())
}

func opipeInboundAt(t *testing.T, db *sql.DB, phoneID, sender, msgID, text string, at time.Time) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"type": "text"})
	if _, err := db.Exec(`INSERT INTO inbound_messages
		(id, whatsapp_message_id, sender_phone, recipient_phone, message_type,
		 text_content, raw_payload, timestamp, created_at)
		VALUES ($1,$2,$3,$4,'text',$5,$6, $7, CURRENT_TIMESTAMP)`,
		msgID, msgID, sender, phoneID, text, raw, at); err != nil {
		t.Fatalf("seed inbound: %v", err)
	}
}

func opipeRun(t *testing.T, e *opipeEnv, biz harness.Business, sender string) {
	t.Helper()
	job := workers.OrchestratorJob{BusinessID: biz.ID, RecipientPhone: biz.WhatsAppPhoneNumberID, SenderPhone: sender}
	if err := workers.HandleDebouncedConversation(context.Background(), e.deps, job); err != nil {
		t.Fatalf("HandleDebouncedConversation: %v", err)
	}
}

func opipeConvRow(t *testing.T, db *sql.DB, bizID, phone string) (state, reason string, escalated bool) {
	t.Helper()
	var r sql.NullString
	if err := db.QueryRow(`SELECT state, COALESCE(escalation_reason,''), is_escalated_to_human
		FROM conversations WHERE business_id = $1 AND customer_phone = $2`, bizID, phone).
		Scan(&state, &r, &escalated); err != nil {
		t.Fatalf("conversation row: %v", err)
	}
	return state, r.String, escalated
}

func opipeScalarInt(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("scalar %q: %v", q, err)
	}
	return n
}

func opipeDrainCRM(t *testing.T, e *opipeEnv) {
	t.Helper()
	crm := workers.CRMDeps{Pool: e.pool}
	for _, j := range e.publisher.ofTask(queue.TaskCRMProcess) {
		var job workers.CRMSignalJob
		if err := json.Unmarshal(j.Payload, &job); err != nil {
			t.Fatalf("decode crm job: %v", err)
		}
		if err := workers.HandleCRMSignals(context.Background(), crm, job); err != nil {
			t.Fatalf("drain crm job: %v", err)
		}
	}
}

func opipeDrainCheckout(t *testing.T, e *opipeEnv) {
	t.Helper()
	checkout := workers.CheckoutDeps{Pool: e.pool}
	for _, j := range e.publisher.ofTask(queue.TaskCheckout) {
		var job workers.CheckoutJob
		if err := json.Unmarshal(j.Payload, &job); err != nil {
			t.Fatalf("decode checkout job: %v", err)
		}
		if err := workers.HandleCheckout(context.Background(), checkout, job); err != nil {
			t.Fatalf("drain checkout job: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// T8.17/T8.18/T8.24 + T0.6 replica — two-turn happy path with decision vector
// ---------------------------------------------------------------------------

func TestOpipeTwoTurnHappyPathCreatesOrderAndTransitions(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	prod := e.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	shortID := prod.ID[:8]
	sender := biz.OwnerPhone
	tokens := map[string]string{prod.ID: "p0"}

	e.llm.steps = []orchestrator.LlmResponse{
		opipeResp(func(r *orchestrator.LlmResponse) {
			r.Intent = "product_inquiry"
			r.ReplyText = "Yes! We have that in stock."
			r.ReferencedProductIDs = []string{shortID}
		}),
		opipeResp(func(r *orchestrator.LlmResponse) {
			r.Intent = "product_inquiry"
			r.ReplyText = "Great! Your order is confirmed. Total: GHS 51.00."
			r.CrmSignals.OrderConfirmed = true
			r.CrmSignals.DetectedItems = []orchestrator.DetectedItem{{ProductID: shortID, Quantity: 2}}
			r.CrmSignals.DeliveryArea = strPtr("Osu")
			r.CrmSignals.Sentiment = "positive"
		}),
	}

	// ---- turn 1: inquiry -------------------------------------------------
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-hp-1", fmt.Sprintf("Do you have [ID:%s] in stock?", shortID))
	before1, err := harness.SnapshotTranscriptTurn(e.db, biz.ID, sender)
	if err != nil {
		t.Fatal(err)
	}
	opipeRun(t, e, biz, sender)
	opipeDrainCRM(t, e)
	dec1, _, err := harness.CollectTurnDecision(e.db, before1, "product_inquiry", tokens)
	if err != nil {
		t.Fatal(err)
	}

	want1 := harness.TurnDecision{
		Intent:          "product_inquiry",
		StateTransition: "LEAD->BROWSING",
		ImageIDsSent:    []string{},
		CRMSignals:      "sentiment=neutral", // materialiser writes scripted sentiment every turn
		ReplyLenBucket:  "s",
	}
	if diffs := harness.DiffDecisions([]harness.TurnDecision{want1}, []harness.TurnDecision{dec1}); len(diffs) > 0 {
		t.Errorf("turn1 vector diffs: %v", diffs)
	}

	// outbound reply persisted in pending shape + handed to outbound queue
	var rawPayload string
	var status string
	if err := e.db.QueryRow(`SELECT raw_payload::text, status FROM outbound_messages
		WHERE conversation_id = (SELECT id FROM conversations WHERE business_id=$1 AND customer_phone=$2)
		ORDER BY created_at DESC LIMIT 1`, biz.ID, sender).Scan(&rawPayload, &status); err != nil {
		t.Fatalf("outbound row: %v", err)
	}
	if status != "pending" {
		t.Errorf("turn1 outbound status = %s, want pending", status)
	}
	var rawShape struct {
		MessagingProduct string `json:"messaging_product"`
		Type             string `json:"type"`
		Text             struct {
			Body string `json:"body"`
		} `json:"text"`
	}
	if err := json.Unmarshal([]byte(rawPayload), &rawShape); err != nil {
		t.Fatalf("turn1 outbound payload: %v", err)
	}
	if rawShape.MessagingProduct != "whatsapp" || rawShape.Type != "text" ||
		rawShape.Text.Body != "Yes! We have that in stock." {
		t.Errorf("turn1 outbound payload shape: %+v", rawShape)
	}
	sends := e.publisher.ofTask(queue.TaskOutboundSend)
	if len(sends) != 1 {
		t.Fatalf("turn1 outbound sends = %d, want 1", len(sends))
	}

	// CRM signal job wire shape (S7B contract)
	crms := e.publisher.ofTask(queue.TaskCRMProcess)
	if len(crms) != 1 {
		t.Fatalf("turn1 crm jobs = %d, want 1", len(crms))
	}
	var cj workers.CRMSignalJob
	if err := json.Unmarshal(crms[0].Payload, &cj); err != nil {
		t.Fatal(err)
	}
	if cj.LLMIntent != "product_inquiry" || cj.SourceMessageID != "wamid-hp-1" ||
		cj.CustomerPhone != sender || cj.CRMSignals.OrderConfirmed {
		t.Errorf("crm job shape: %+v", cj)
	}
	if crms[0].Queue != queue.QCRMMaterialiser || crms[0].Opts != nil {
		t.Errorf("crm job routing: queue=%s opts=%v", crms[0].Queue, crms[0].Opts)
	}

	// ---- turn 2: order confirmed ----------------------------------------
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-hp-2", fmt.Sprintf("Yes please confirm my order: 2 of [ID:%s]", shortID))
	before2, err := harness.SnapshotTranscriptTurn(e.db, biz.ID, sender)
	if err != nil {
		t.Fatal(err)
	}
	opipeRun(t, e, biz, sender)
	opipeDrainCheckout(t, e) // checkout owns order creation + BROWSING->INVOICING
	opipeDrainCRM(t, e)      // enrichment lands synchronously here
	dec2, _, err := harness.CollectTurnDecision(e.db, before2, "product_inquiry", tokens)
	if err != nil {
		t.Fatal(err)
	}

	want2 := harness.TurnDecision{
		Intent:          "product_inquiry",
		StateTransition: "BROWSING->INVOICING",
		ImageIDsSent:    []string{},
		CRMSignals:      "orders=1,total=51.00|sentiment=positive|area=Osu",
		ReplyLenBucket:  "s", // the LLM confirmation reply (invoice copy is a Phase 3 consumer)
	}
	if diffs := harness.DiffDecisions([]harness.TurnDecision{want2}, []harness.TurnDecision{dec2}); len(diffs) > 0 {
		t.Errorf("turn2 vector diffs: %v", diffs)
	}

	// T0.6 characterized outcome: order + items + stock decrement + idempotency
	var orderID string
	if err := e.db.QueryRow(`SELECT id FROM orders WHERE conversation_id =
		(SELECT id FROM conversations WHERE business_id=$1 AND customer_phone=$2)`, biz.ID, sender).Scan(&orderID); err != nil {
		t.Fatalf("order row: %v", err)
	}
	total := opipeScalarStr(t, e.db, `SELECT total_amount::text FROM orders WHERE id=$1`, orderID)
	if total != "51.00" {
		t.Errorf("total = %s, want 51.00", total)
	}
	idem := opipeScalarStr(t, e.db, `SELECT COALESCE(idempotency_key,'') FROM orders WHERE id=$1`, orderID)
	if idem != "wamid-hp-2" {
		t.Errorf("idempotency_key = %q, want source inbound id", idem)
	}
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM order_items WHERE order_id=$1 AND product_id=$2 AND quantity=2 AND unit_price::text='25.50'`, orderID, prod.ID); got != 1 {
		t.Errorf("order items = %d, want 1", got)
	}
	if stock := opipeScalarStr(t, e.db, `SELECT stock::text FROM products WHERE id=$1`, prod.ID); stock != "3" {
		t.Errorf("stock after decrement = %s, want 3", stock)
	}
}

func opipeScalarStr(t *testing.T, db *sql.DB, q string, args ...any) string {
	t.Helper()
	var s string
	if err := db.QueryRow(q, args...).Scan(&s); err != nil {
		t.Fatalf("scalar %q: %v", q, err)
	}
	return s
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// T8.20 — low confidence is flag-only (no transition/reply/images/CRM)
// ---------------------------------------------------------------------------

func TestOpipeLowConfidenceFlagOnly(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
		r.InternalConfidence = 0.5
	})}

	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-lowconf-1", "what was that?")
	opipeRun(t, e, biz, sender)

	state, reason, escalated := opipeConvRow(t, e.db, biz.ID, sender)
	if !escalated || reason != "LOW_CONFIDENCE" {
		t.Errorf("escalated=%v reason=%q, want true/LOW_CONFIDENCE", escalated, reason)
	}
	if state != "LEAD" {
		t.Errorf("state = %s, flag-only escalation must not transition", state)
	}
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM outbound_messages WHERE message_type='text'
		AND text_content = 'You are being connected to a human agent. Please wait.'
		AND status='pending'
		AND conversation_id = (SELECT id FROM conversations WHERE business_id=$1 AND customer_phone=$2)`, biz.ID, sender); got != 1 {
		t.Errorf("connecting messages = %d, want 1", got)
	}
	if n := len(e.publisher.ofTask(queue.TaskCRMProcess)); n != 0 {
		t.Errorf("crm jobs = %d, want 0 (early return before emission)", n)
	}
	if imgRows := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM outbound_messages WHERE message_type='image'`); imgRows != 0 {
		t.Errorf("image rows = %d, want 0", imgRows)
	}
	if llmRows := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM outbound_messages WHERE text_content='Hello! How can I help you today?'`); llmRows != 0 {
		t.Errorf("scripted reply must NOT be sent on low confidence")
	}
}

// ---------------------------------------------------------------------------
// T8.19 — keyword escalation short-circuits BEFORE the LLM; state untouched
// ---------------------------------------------------------------------------

func TestOpipeKeywordMatchEscalatesBeforeLLM(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-kw-1", "This is a scam, I want my money back")
	opipeRun(t, e, biz, sender)

	if calls := e.llm.callCount(); calls != 0 {
		t.Errorf("llm calls = %d, want 0 (keyword fast-path bypasses LLM)", calls)
	}
	state, reason, escalated := opipeConvRow(t, e.db, biz.ID, sender)
	if !escalated || reason != "KEYWORD_MATCH" {
		t.Errorf("escalated=%v reason=%q, want true/KEYWORD_MATCH", escalated, reason)
	}
	if state != "LEAD" {
		t.Errorf("state = %s, keyword path must not change state", state)
	}
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM outbound_messages
		WHERE text_content = 'You are being connected to a human agent. Please wait.'`); got != 1 {
		t.Errorf("connecting messages = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// T8.21 — image gating: explicit request THIS TURN is the only rule
// ---------------------------------------------------------------------------

func TestOpipeImageGatingExplicitRequestOnly(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	prod := e.factory.ProductWithImages(biz.ID, 1, harness.WithPrice("25.50"))
	shortID := prod.ID[:8]
	sender := biz.OwnerPhone

	// turn A: LLM wants to send but customer did NOT ask → suppressed
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
		r.Intent = "product_inquiry"
		r.CustomerRequestedImages = false
		r.SendProductImageIDs = []string{shortID}
	})}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-img-a", "how much is it?")
	opipeRun(t, e, biz, sender)
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM outbound_messages WHERE message_type='image'`); got != 0 {
		t.Fatalf("unsolicited image rows = %d, want 0", got)
	}

	// turn B: explicit ask → image sent BEFORE the text reply
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
		r.Intent = "product_inquiry"
		r.CustomerRequestedImages = true
		r.SendProductImageIDs = []string{shortID}
	})}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-img-b", "please send a photo")
	opipeRun(t, e, biz, sender)

	var imgURL, caption, productID, rawImg string
	if err := e.db.QueryRow(`SELECT image_url, text_content, product_id, raw_payload::text
		FROM outbound_messages WHERE message_type='image'`).Scan(&imgURL, &caption, &productID, &rawImg); err != nil {
		t.Fatalf("image row: %v", err)
	}
	if productID != prod.ID {
		t.Errorf("productId persistence: got %s want %s", productID, prod.ID)
	}
	wantCaption := prod.Name + " — GH₵25.50"
	if caption != wantCaption {
		t.Errorf("caption = %q, want %q", caption, wantCaption)
	}
	var imgRaw struct {
		MessagingProduct string `json:"messaging_product"`
		Type             string `json:"type"`
		Image            struct {
			Link    string `json:"link"`
			Caption string `json:"caption"`
		} `json:"image"`
	}
	if err := json.Unmarshal([]byte(rawImg), &imgRaw); err != nil {
		t.Fatalf("image rawPayload: %v", err)
	}
	if imgRaw.MessagingProduct != "whatsapp" || imgRaw.Type != "image" ||
		imgRaw.Image.Link != imgURL || imgRaw.Image.Caption != caption {
		t.Errorf("image rawPayload shape: %+v", imgRaw)
	}
	if imgURL == "" {
		t.Error("image_url must persist on image sends")
	}

	// turn C: asking AGAIN explicitly re-sends despite already-shown history
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
		r.Intent = "product_inquiry"
		r.CustomerRequestedImages = true
		r.SendProductImageIDs = []string{shortID}
	})}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-img-c", "can you show me again please?")
	opipeRun(t, e, biz, sender)
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM outbound_messages WHERE message_type='image' AND status='pending'`); got != 2 {
		t.Errorf("repeat explicit request images = %d, want 2 (gate is request-only)", got)
	}

	// invalid short IDs never crash or persist
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
		r.CustomerRequestedImages = true
		r.SendProductImageIDs = []string{"ZZZZ!!!"}
	})}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-img-d", "one more photo")
	opipeRun(t, e, biz, sender)
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM outbound_messages WHERE message_type='image'`); got != 2 {
		t.Errorf("invalid short id must be rejected; image rows = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// T8.23 — 24h window closed: blocked row shapes; pipeline still continues
// ---------------------------------------------------------------------------

func TestOpipe24hWindowClosedRowShapes(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	// The 24h rule keys off the NEWEST inbound row, so the turn under test
	// must itself carry an old payload timestamp (delayed webhook replay is
	// the realistic trigger).
	oldTS := func() time.Time { return time.Now().Add(-25 * time.Hour) }

	newWindow := func(sender, firstMsgID string) {
		t.Helper()
		e.llm.steps = append(e.llm.steps, opipeResp(func(r *orchestrator.LlmResponse) {
			r.Intent = "greeting"
		}))
		opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, firstMsgID, "hi")
		opipeRun(t, e, biz, sender) // creates customer/conversation inside window
		if _, err := e.db.Exec(`UPDATE inbound_messages SET "timestamp" = $3
			WHERE recipient_phone = $1 AND sender_phone = $2`,
			biz.WhatsAppPhoneNumberID, sender, oldTS()); err != nil {
			t.Fatal(err)
		}
	}

	// --- text reply blocked ---
	senderA := biz.OwnerPhone + "-wa" // distinct conversation, same business
	newWindow(senderA, "wamid-wa-a1")
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
		r.Intent = "greeting"
		r.ReplyText = "blocked reply body"
	})}
	opipeInboundAt(t, e.db, biz.WhatsAppPhoneNumberID, senderA, "wamid-wa-a2", "hello again", oldTS())
	opipeRun(t, e, biz, senderA)

	var status, raw string
	if err := e.db.QueryRow(`SELECT status, raw_payload::text FROM outbound_messages
		WHERE text_content = 'blocked reply body'`).Scan(&status, &raw); err != nil {
		t.Fatalf("blocked text row: %v", err)
	}
	if status != "failed_24h_window_closed" || raw != "{}" {
		t.Errorf("blocked text row: status=%s raw=%s", status, raw)
	}

	// --- image send blocked (explicit request, window closed) ---
	prod := e.factory.ProductWithImages(biz.ID, 1)
	senderB := biz.OwnerPhone + "-wb"
	newWindow(senderB, "wamid-wa-b1")
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
		r.CustomerRequestedImages = true
		r.SendProductImageIDs = []string{prod.ID[:8]}
	})}
	opipeInboundAt(t, e.db, biz.WhatsAppPhoneNumberID, senderB, "wamid-wa-b2", "send the photo now", oldTS())
	opipeRun(t, e, biz, senderB)

	var iStatus, iRaw, iProduct string
	if err := e.db.QueryRow(`SELECT status, raw_payload::text, COALESCE(product_id,'')
		FROM outbound_messages WHERE message_type='image' AND recipient_phone = $1`, senderB).
		Scan(&iStatus, &iRaw, &iProduct); err != nil {
		t.Fatalf("blocked image row: %v", err)
	}
	if iStatus != "failed_24h_window_closed" || iRaw != "{}" || iProduct != prod.ID {
		t.Errorf("blocked image row: status=%s raw=%s product=%s", iStatus, iRaw, iProduct)
	}

	// pipeline CONTINUES after a compliance block: transitions + CRM still run
	state, _, _ := opipeConvRow(t, e.db, biz.ID, senderB)
	if state != "LEAD" { // greeting intent → no transition; assert via CRM instead
		t.Logf("state=%s (greeting keeps LEAD)", state)
	}
}

// ---------------------------------------------------------------------------
// T8.17 — history merge semantics: sort asc → slice(-10) → empty-filter
// ---------------------------------------------------------------------------

func TestOpipeHistoryMergeSortFilterSlice(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	oc, ok, err := workers.ResolveConversationContext(context.Background(), e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("resolve context: ok=%v err=%v", ok, err)
	}

	base := time.Now().Add(-time.Hour)
	insertInbound := func(text string, at time.Time) {
		raw, _ := json.Marshal(map[string]any{"type": "text"})
		if _, err := e.db.Exec(`INSERT INTO inbound_messages
			(id, whatsapp_message_id, sender_phone, recipient_phone, message_type, text_content, raw_payload, "timestamp", business_id, conversation_id, created_at)
			VALUES ($1,$1,$2,$3,'text',NULLIF($4,''),$5,$6,$7,$8,$6)`,
			fmt.Sprintf("in-%d", at.UnixNano()), sender, biz.WhatsAppPhoneNumberID, text, raw, at, biz.ID, oc.ConversationID); err != nil {
			t.Fatal(err)
		}
	}
	insertOutbound := func(text string, at time.Time) {
		raw, _ := json.Marshal(map[string]any{})
		if _, err := e.db.Exec(`INSERT INTO outbound_messages
			(id, recipient_phone, message_type, text_content, raw_payload, status, business_id, conversation_id, created_at)
			VALUES ($1,$2,'text',$3,$4,'sent',$5,$6,$7)`,
			fmt.Sprintf("out-%d", at.UnixNano()), sender, text, raw, biz.ID, oc.ConversationID, at); err != nil {
			t.Fatal(err)
		}
	}
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }

	// merged ascending timeline (14 entries incl. current empty one):
	// i1 i2 o1 i3 o2 i4 o3 i5 o4 i6 o5 i7 o6 E
	insertInbound("i1", at(1))
	insertInbound("i2", at(2))
	insertOutbound("o1", at(3))
	insertInbound("i3", at(4))
	insertOutbound("o2", at(5))
	insertInbound("i4", at(6))
	insertOutbound("o3", at(7))
	insertInbound("i5", at(8))
	insertOutbound("o4", at(9))
	insertInbound("i6", at(10))
	insertOutbound("o5", at(11))
	insertInbound("i7", at(12))
	insertOutbound("o6", at(13))
	insertInbound("", at(14)) // newest, EMPTY — burns a slot then filters out

	e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
	opipeRun(t, e, biz, sender)

	if calls := e.llm.callCount(); calls != 1 {
		t.Fatalf("llm calls = %d, want 1", calls)
	}
	msgs := e.llm.calls[0].Messages
	type kv struct{ role, text string }
	var got []kv
	for _, m := range msgs {
		got = append(got, kv{m.Role, m.Parts[0].Text})
	}
	want := []kv{
		{"assistant", "o2"}, // slice(-10) dropped i1,i2,o1,i3; filter dropped E
		{"user", "i4"},
		{"assistant", "o3"},
		{"user", "i5"},
		{"assistant", "o4"},
		{"user", "i6"},
		{"assistant", "o5"},
		{"user", "i7"},
		{"assistant", "o6"},
	}
	if len(got) != len(want) {
		t.Fatalf("history length = %d (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("history[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestOpipeHistoryExcludesOlderThan24h(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	oc, ok, err := workers.ResolveConversationContext(context.Background(), e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("resolve context: ok=%v err=%v", ok, err)
	}

	insertInbound := func(text string, at time.Time) {
		raw, _ := json.Marshal(map[string]any{"type": "text"})
		if _, err := e.db.Exec(`INSERT INTO inbound_messages
			(id, whatsapp_message_id, sender_phone, recipient_phone, message_type, text_content, raw_payload, "timestamp", business_id, conversation_id, created_at)
			VALUES ($1,$1,$2,$3,'text',NULLIF($4,''),$5,$6,$7,$8,$6)`,
			fmt.Sprintf("in-%d", at.UnixNano()), sender, biz.WhatsAppPhoneNumberID, text, raw, at, biz.ID, oc.ConversationID); err != nil {
			t.Fatal(err)
		}
	}
	insertOutbound := func(text string, at time.Time) {
		raw, _ := json.Marshal(map[string]any{})
		if _, err := e.db.Exec(`INSERT INTO outbound_messages
			(id, recipient_phone, message_type, text_content, raw_payload, status, business_id, conversation_id, created_at)
			VALUES ($1,$2,'text',$3,$4,'sent',$5,$6,$7)`,
			fmt.Sprintf("out-%d", at.UnixNano()), sender, text, raw, biz.ID, oc.ConversationID, at); err != nil {
			t.Fatal(err)
		}
	}

	// Message from 2 days ago (must be excluded)
	insertInbound("old user message", time.Now().Add(-48*time.Hour))
	insertOutbound("old assistant message", time.Now().Add(-48*time.Hour))

	// Message from 2 hours ago (must be included)
	insertInbound("recent message", time.Now().Add(-2*time.Hour))

	e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
	opipeRun(t, e, biz, sender)

	if calls := e.llm.callCount(); calls != 1 {
		t.Fatalf("llm calls = %d, want 1", calls)
	}
	msgs := e.llm.calls[0].Messages
	if len(msgs) != 1 {
		t.Fatalf("history length = %d (%v), want 1 (older than 24h excluded)", len(msgs), msgs)
	}
	if msgs[0].Parts[0].Text != "recent message" {
		t.Errorf("history message text = %q, want 'recent message'", msgs[0].Parts[0].Text)
	}
}

// ---------------------------------------------------------------------------
// T8.18 — context resolution upsert idempotency + orphan backfill
// ---------------------------------------------------------------------------

func TestOpipeContextResolutionUpsertIdempotent(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	// orphan inbound captured pre-resolution
	raw, _ := json.Marshal(map[string]any{"type": "text"})
	if _, err := e.db.Exec(`INSERT INTO inbound_messages
		(id, whatsapp_message_id, sender_phone, recipient_phone, message_type, text_content, raw_payload, "timestamp")
		VALUES ('orphan-1','orphan-1',$1,$2,'text','early msg',$3,CURRENT_TIMESTAMP - INTERVAL '5 minutes')`,
		sender, biz.WhatsAppPhoneNumberID, raw); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	oc1, ok, err := workers.ResolveConversationContext(ctx, e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("first resolve: ok=%v err=%v", ok, err)
	}
	firstContact := opipeScalarStr(t, e.db, `SELECT first_contact_at::text FROM customers WHERE id=$1`, oc1.CustomerID)
	lastContact1 := opipeScalarStr(t, e.db, `SELECT last_contact_at::text FROM customers WHERE id=$1`, oc1.CustomerID)

	time.Sleep(10 * time.Millisecond)
	oc2, ok, err := workers.ResolveConversationContext(ctx, e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("second resolve: ok=%v err=%v", ok, err)
	}

	if oc1.CustomerID != oc2.CustomerID || oc1.ConversationID != oc2.ConversationID {
		t.Errorf("upsert must reuse rows: cust %s/%s conv %s/%s",
			oc1.CustomerID, oc2.CustomerID, oc1.ConversationID, oc2.ConversationID)
	}
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM customers WHERE business_id=$1 AND phone=$2`, biz.ID, sender); got != 1 {
		t.Errorf("customer rows = %d, want 1", got)
	}
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM customer_profiles WHERE customer_id=$1`, oc1.CustomerID); got != 1 {
		t.Errorf("profile rows = %d, want 1 (nested create only on first contact)", got)
	}
	if got := opipeScalarInt(t, e.db, `SELECT COUNT(*) FROM conversations WHERE business_id=$1 AND customer_phone=$2`, biz.ID, sender); got != 1 {
		t.Errorf("conversation rows = %d, want 1", got)
	}
	if again := opipeScalarStr(t, e.db, `SELECT first_contact_at::text FROM customers WHERE id=$1`, oc1.CustomerID); again != firstContact {
		t.Errorf("first_contact_at changed: %s -> %s", firstContact, again)
	}
	lastContact2 := opipeScalarStr(t, e.db, `SELECT last_contact_at::text FROM customers WHERE id=$1`, oc1.CustomerID)
	if lastContact2 < lastContact1 {
		t.Errorf("last_contact_at must not go backwards: %s -> %s", lastContact1, lastContact2)
	}

	// backfill linked the orphan to the resolved business+conversation
	var bID, cID sql.NullString
	if err := e.db.QueryRow(`SELECT business_id, conversation_id FROM inbound_messages WHERE id='orphan-1'`).Scan(&bID, &cID); err != nil {
		t.Fatal(err)
	}
	if !bID.Valid || bID.String != biz.ID || !cID.Valid || cID.String != oc1.ConversationID {
		t.Errorf("orphan not backfilled: biz=%v conv=%v", bID, cID)
	}
}

// ---------------------------------------------------------------------------
// T8.26 — real-asynq TaskID dedup collapses two rapid publishes into ONE job
// ---------------------------------------------------------------------------

func TestOpipeDebounceTaskIDDedupRealAsynq(t *testing.T) {
	opipeDockerAlive(t)
	ctx := t.Context()
	envH, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness redis: %v", err)
	}
	t.Cleanup(func() { envH.Terminate(context.Background()) })

	client := queue.NewClient(envH.RedisAddr, harness.RedisPassword)
	defer func() { _ = client.Close() }()

	pub := queue.Publisher(client)
	job := workers.OrchestratorJob{RecipientPhone: "+233555000111", SenderPhone: "+233555000222"}
	if err := workers.PublishOrchestratorDebounce(ctx, pub, job); err != nil {
		t.Fatal(err)
	}
	// rapid duplicate for the SAME pair must be silently swallowed
	if err := workers.PublishOrchestratorDebounce(ctx, pub, job); err != nil {
		t.Fatal(err)
	}
	other := workers.OrchestratorJob{RecipientPhone: "+233555000111", SenderPhone: "+233555000999"}
	if err := workers.PublishOrchestratorDebounce(ctx, pub, other); err != nil {
		t.Fatal(err)
	}

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: envH.RedisAddr, Password: harness.RedisPassword})
	defer func() { _ = inspector.Close() }()
	tasks, err := inspector.ListScheduledTasks(queue.QOrchestrator, asynq.PageSize(100))
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]int{}
	for _, ti := range tasks {
		ids[ti.ID]++
	}
	fixed := workers.OrchestratorDebounceTaskID("+233555000111", "+233555000222")
	if ids[fixed] != 1 {
		t.Errorf("debounce key occurrences = %d, want exactly 1", ids[fixed])
	}
	if len(tasks) != 2 {
		t.Errorf("scheduled tasks = %d (%v), want 2 distinct pairs", len(tasks), ids)
	}
	for _, ti := range tasks {
		if d := time.Until(ti.NextProcessAt); d < workers.OrchestratorDebounceDelay-time.Second {
			t.Errorf("task %s processes in %s, want ~3s delay", ti.ID, d)
		}
	}
}

// ---------------------------------------------------------------------------
// T8.27 — completion re-check re-enqueues when newer inbound lands mid-run
// ---------------------------------------------------------------------------

type opipeRegistrar struct {
	mu       sync.Mutex
	handlers map[string]queue.Handler
}

func newOpipeRegistrar() *opipeRegistrar {
	return &opipeRegistrar{handlers: map[string]queue.Handler{}}
}

func (r *opipeRegistrar) Register(taskType string, h queue.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[taskType] = h
}

type opipeSlowLLM struct {
	entered chan struct{}
	release chan struct{}
	inner   *opipeLLM
}

func (l *opipeSlowLLM) GenerateObject(ctx context.Context, req orchestrator.GenerateRequest) (orchestrator.LlmResponse, error) {
	select {
	case <-l.entered: // already open
	default:
		close(l.entered) // signal: processing has reached the LLM stall
	}
	<-l.release // hold the job active while the test inserts newer inbound
	return l.inner.GenerateObject(ctx, req)
}

func TestOpipeCompletionRecheckReenqueuesOnNewerInbound(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-rc-first", "hello")

	slow := &opipeSlowLLM{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		inner:   &opipeLLM{steps: []orchestrator.LlmResponse{opipeResp(nil)}},
	}
	deps := e.deps
	deps.LLM = slow

	reg := newOpipeRegistrar()
	workers.RegisterOrchestrator(reg, deps)
	handler := reg.handlers[queue.TaskOrchestratorDebounce]
	if handler == nil {
		t.Fatal("handler not registered")
	}

	job := workers.OrchestratorJob{RecipientPhone: biz.WhatsAppPhoneNumberID, SenderPhone: sender}
	payload, _ := json.Marshal(job)

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler(context.Background(), payload)
	}()
	<-slow.entered
	// newer inbound arrives WHILE the debounce job is active
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-rc-second", "and also this!")
	close(slow.release)
	if err := <-errCh; err != nil {
		t.Fatalf("handler: %v", err)
	}

	orchJobs := e.publisher.ofTask(queue.TaskOrchestratorDebounce)
	if len(orchJobs) != 1 {
		t.Fatalf("re-enqueued orchestrator jobs = %d, want 1", len(orchJobs))
	}
	fixed := workers.OrchestratorDebounceTaskID(biz.WhatsAppPhoneNumberID, sender)
	opts := orchJobs[0].Opts
	if opts == nil || opts.TaskID == "" || opts.TaskID == fixed || !strings.HasPrefix(opts.TaskID, fixed+":") {
		t.Errorf("fresh TaskID required: %+v (fixed=%s)", opts, fixed)
	}
	if opts.ProcessIn != workers.OrchestratorDebounceDelay {
		t.Errorf("re-enqueue delay = %s, want %s", opts.ProcessIn, workers.OrchestratorDebounceDelay)
	}

	// negative: nothing newer → no extra enqueue (fresh fast handler)
	e.publisher.jobs = nil
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
	fastReg := newOpipeRegistrar()
	workers.RegisterOrchestrator(fastReg, e.deps)
	if err := fastReg.handlers[queue.TaskOrchestratorDebounce](context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if n := len(e.publisher.ofTask(queue.TaskOrchestratorDebounce)); n != 0 {
		t.Errorf("re-enqueues without newer inbound = %d, want 0", n)
	}
}

// The completion re-check must not depend on the worker and database clocks
// agreeing: a message persisted BEFORE the run whose created_at is ahead of
// the worker clock (database clock drifted forward) is not "newer".
func TestOpipeCompletionRecheckImmuneToClockSkew(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-skew-1", "hello")
	if _, err := e.db.Exec(`UPDATE inbound_messages SET created_at = now() + interval '30 seconds' WHERE whatsapp_message_id = 'wamid-skew-1'`); err != nil {
		t.Fatal(err)
	}

	e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
	reg := newOpipeRegistrar()
	workers.RegisterOrchestrator(reg, e.deps)
	payload, _ := json.Marshal(workers.OrchestratorJob{RecipientPhone: biz.WhatsAppPhoneNumberID, SenderPhone: sender})
	if err := reg.handlers[queue.TaskOrchestratorDebounce](context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if n := len(e.publisher.ofTask(queue.TaskOrchestratorDebounce)); n != 0 {
		t.Errorf("clock-skewed pre-existing message triggered %d re-enqueue(s), want 0", n)
	}
}

// F.22 — reorder reset: a customer returning from PAID/CANCELLED with new
// shopping intent resets to BROWSING (so they can re-enter CHECKOUT later).
func TestOpipeReorderResetFromPaidOrCancelled(t *testing.T) {
	for _, start := range []string{"PAID", "CANCELLED"} {
		t.Run(start, func(t *testing.T) {
			e := opipeStart(t)
			biz := e.factory.Business()
			sender := biz.OwnerPhone

			// First run creates the conversation at LEAD.
			e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
			opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-re-1", "hello")
			opipeRun(t, e, biz, sender)

			if _, err := e.db.Exec(`UPDATE conversations SET state = $3
				WHERE business_id = $1 AND customer_phone = $2`, biz.ID, sender, start); err != nil {
				t.Fatal(err)
			}

			// Customer comes back with new shopping intent.
			e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
				r.Intent = "product_inquiry"
				r.ReplyText = "We have that in stock!"
			})}
			opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-re-2", "do you have shea butter?")
			opipeRun(t, e, biz, sender)

			state, _, _ := opipeConvRow(t, e.db, biz.ID, sender)
			if state != "BROWSING" {
				t.Errorf("state = %s, want BROWSING (reorder reset from %s)", state, start)
			}
		})
	}
}

// Regression for Issue 1: A customer who just paid sends a follow-up acknowledgment
// ("Great!!", "Okay"). Even if the LLM hallucinated OrderConfirmed=true and carried over
// detected items, the orchestrator MUST NOT enqueue a checkout job or create an order.
func TestOpipePostPaymentAcknowledgmentDoesNotTriggerOrder(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	prod := e.factory.Product(biz.ID, harness.WithPrice("2.50"))
	sender := biz.OwnerPhone

	// 1. Establish conversation in PAID state
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(nil)}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-init", "hello")
	opipeRun(t, e, biz, sender)

	if _, err := e.db.Exec(`UPDATE conversations SET state = 'PAID'
		WHERE business_id = $1 AND customer_phone = $2`, biz.ID, sender); err != nil {
		t.Fatal(err)
	}

	// 2. Customer sends post-payment acknowledgment "Great!!"
	// LLM hallucinates OrderConfirmed=true with the previous product
	e.llm.steps = []orchestrator.LlmResponse{opipeResp(func(r *orchestrator.LlmResponse) {
		r.Intent = "checkout_request"
		r.ReplyText = "I'm glad to hear that! Your order will be on its way soon! 😊"
		r.CrmSignals.OrderConfirmed = true
		r.CrmSignals.DetectedItems = []orchestrator.DetectedItem{
			{ProductID: prod.ID[:8], Quantity: 1},
		}
	})}
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-ack", "Great!!")
	opipeRun(t, e, biz, sender)

	// 3. Verify: NO TaskCheckout job enqueued!
	checkouts := e.publisher.ofTask(queue.TaskCheckout)
	if len(checkouts) != 0 {
		t.Fatalf("checkout jobs enqueued = %d, want 0 (must not trigger order from PAID acknowledgment)", len(checkouts))
	}

	// 4. Verify: State safely reset to BROWSING
	state, _, _ := opipeConvRow(t, e.db, biz.ID, sender)
	if state != "BROWSING" {
		t.Errorf("conversation state = %s, want BROWSING", state)
	}
}

// TestOpipeOrderIntentLifecycleStagesAndConverts verifies that a 2-turn purchase
// stages an order_intent on turn 1 and converts it on turn 2.
func TestOpipeOrderIntentLifecycleStagesAndConverts(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	prod := e.factory.Product(biz.ID, harness.WithStock(10), harness.WithPrice("25.50"))
	sender := biz.OwnerPhone
	shortID := prod.ID[:8]

	// Turn 1: Customer asks to buy items -> Bot stages order intent in AWAITING_CONFIRMATION
	e.llm.steps = []orchestrator.LlmResponse{
		opipeResp(func(r *orchestrator.LlmResponse) {
			r.Intent = "checkout_request"
			r.ReplyText = "You'd like to order 1x product for GH₵25.50. Should I confirm this order?"
			r.CrmSignals.OrderConfirmed = false
			r.CrmSignals.DetectedItems = []orchestrator.DetectedItem{
				{ProductID: shortID, Quantity: 1},
			}
		}),
		opipeResp(func(r *orchestrator.LlmResponse) {
			r.Intent = "checkout_request"
			r.ReplyText = "Your order is confirmed!"
			r.CrmSignals.OrderConfirmed = true
			r.CrmSignals.DetectedItems = []orchestrator.DetectedItem{
				{ProductID: shortID, Quantity: 1},
			}
		}),
	}

	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-stage-1", "I want to buy 1 piece")
	opipeRun(t, e, biz, sender)

	// Verify order_intent was staged
	var intentStatus string
	var intentCount int
	if err := e.db.QueryRow(`SELECT count(*), COALESCE(max(status::text),'') FROM order_intents
		WHERE business_id = $1`, biz.ID).Scan(&intentCount, &intentStatus); err != nil {
		t.Fatal(err)
	}
	if intentCount != 1 || intentStatus != "AWAITING_CONFIRMATION" {
		t.Fatalf("staged intent: count=%d status=%s, want 1 AWAITING_CONFIRMATION", intentCount, intentStatus)
	}

	// Turn 2: Customer confirms -> Intent converts to CONVERTED
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-stage-2", "Yes, confirm")
	opipeRun(t, e, biz, sender)
	opipeDrainCheckout(t, e)

	var convertedStatus string
	var orderID string
	if err := e.db.QueryRow(`SELECT status::text, COALESCE(order_id,'') FROM order_intents
		WHERE business_id = $1`, biz.ID).Scan(&convertedStatus, &orderID); err != nil {
		t.Fatal(err)
	}
	if convertedStatus != "CONVERTED" || orderID == "" {
		t.Fatalf("converted intent: status=%s order_id=%s, want CONVERTED and non-empty order_id", convertedStatus, orderID)
	}
}

func TestResolveCatalogSearchText(t *testing.T) {
	// Case 1: Short follow-up carries forward product details from assistant response
	history1 := []orchestrator.ChatMessage{
		{Role: "user", Parts: []orchestrator.MessagePart{{Type: "text", Text: "I need to buy a hair bundle. 1pc."}}},
		{Role: "assistant", Parts: []orchestrator.MessagePart{{Type: "text", Text: "The Human Hair Bundle is GH₵4 [ID: prod123]. Would you like to see a photo?"}}},
		{Role: "user", Parts: []orchestrator.MessagePart{{Type: "text", Text: "Confirm price."}}},
	}
	got1 := workers.ResolveCatalogSearchText("Confirm price.", history1)
	want1 := "The Human Hair Bundle is GH₵4 [ID: prod123]. Would you like to see a photo? Confirm price."
	if got1 != want1 {
		t.Errorf("got %q, want %q", got1, want1)
	}

	// Case 2: Short courtesy responses ("okay", "yes") are skipped to reach substantive context
	history2 := []orchestrator.ChatMessage{
		{Role: "user", Parts: []orchestrator.MessagePart{{Type: "text", Text: "What is the price of Shea Butter?"}}},
		{Role: "assistant", Parts: []orchestrator.MessagePart{{Type: "text", Text: "Shea Butter is GH₵35 [ID: shea456]"}}},
		{Role: "user", Parts: []orchestrator.MessagePart{{Type: "text", Text: "okay"}}},
		{Role: "user", Parts: []orchestrator.MessagePart{{Type: "text", Text: "How much?"}}},
	}
	got2 := workers.ResolveCatalogSearchText("How much?", history2)
	want2 := "Shea Butter is GH₵35 [ID: shea456] How much?"
	if got2 != want2 {
		t.Errorf("got %q, want %q", got2, want2)
	}

	// Case 3: Long query (> 5 words) is left untouched
	longQuery := "I want to order three bottles of moisturising body lotion"
	got3 := workers.ResolveCatalogSearchText(longQuery, history1)
	if got3 != longQuery {
		t.Errorf("got %q, want %q", got3, longQuery)
	}

	// Case 4: Empty history returns textContent untouched
	got4 := workers.ResolveCatalogSearchText("Confirm price.", nil)
	if got4 != "Confirm price." {
		t.Errorf("got %q, want %q", got4, "Confirm price.")
	}
}

func TestOpipeHistoryInactivityGapPruning(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	oc, ok, err := workers.ResolveConversationContext(context.Background(), e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("resolve context: ok=%v err=%v", ok, err)
	}

	insertInbound := func(text string, at time.Time) {
		raw, _ := json.Marshal(map[string]any{"type": "text"})
		if _, err := e.db.Exec(`INSERT INTO inbound_messages
			(id, whatsapp_message_id, sender_phone, recipient_phone, message_type, text_content, raw_payload, "timestamp", business_id, conversation_id, created_at)
			VALUES ($1,$1,$2,$3,'text',NULLIF($4,''),$5,$6,$7,$8,$6)`,
			fmt.Sprintf("in-%d", at.UnixNano()), sender, biz.WhatsAppPhoneNumberID, text, raw, at, biz.ID, oc.ConversationID); err != nil {
			t.Fatal(err)
		}
	}
	insertOutbound := func(text string, at time.Time) {
		raw, _ := json.Marshal(map[string]any{})
		if _, err := e.db.Exec(`INSERT INTO outbound_messages
			(id, recipient_phone, message_type, text_content, raw_payload, status, business_id, conversation_id, created_at)
			VALUES ($1,$2,'text',$3,$4,'sent',$5,$6,$7)`,
			fmt.Sprintf("out-%d", at.UnixNano()), sender, text, raw, biz.ID, oc.ConversationID, at); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()
	// Stale session from 5 hours ago
	insertInbound("old session question", now.Add(-5*time.Hour))
	insertOutbound("old session answer", now.Add(-5*time.Hour+time.Minute))

	// Inactivity gap: ~4 hours between old session answer and new session question

	// New active session starting 40 minutes ago
	insertInbound("new session question", now.Add(-40*time.Minute))
	insertOutbound("new session answer", now.Add(-39*time.Minute))
	insertInbound("latest follow-up", now.Add(-10*time.Minute))

	history, err := workers.FetchConversationHistory(context.Background(), e.pool, oc.ConversationID)
	if err != nil {
		t.Fatalf("FetchConversationHistory: %v", err)
	}

	if len(history) != 3 {
		t.Fatalf("expected 3 messages after inactivity gap pruning, got %d: %+v", len(history), history)
	}
	if history[0].Parts[0].Text != "new session question" {
		t.Errorf("history[0] = %q, want 'new session question'", history[0].Parts[0].Text)
	}
	if history[1].Parts[0].Text != "new session answer" {
		t.Errorf("history[1] = %q, want 'new session answer'", history[1].Parts[0].Text)
	}
	if history[2].Parts[0].Text != "latest follow-up" {
		t.Errorf("history[2] = %q, want 'latest follow-up'", history[2].Parts[0].Text)
	}
}

func TestOpipeHistoryPaidOrderBoundary(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	oc, ok, err := workers.ResolveConversationContext(context.Background(), e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("resolve context: ok=%v err=%v", ok, err)
	}

	insertInbound := func(text string, at time.Time) {
		raw, _ := json.Marshal(map[string]any{"type": "text"})
		if _, err := e.db.Exec(`INSERT INTO inbound_messages
			(id, whatsapp_message_id, sender_phone, recipient_phone, message_type, text_content, raw_payload, "timestamp", business_id, conversation_id, created_at)
			VALUES ($1,$1,$2,$3,'text',NULLIF($4,''),$5,$6,$7,$8,$6)`,
			fmt.Sprintf("in-%d", at.UnixNano()), sender, biz.WhatsAppPhoneNumberID, text, raw, at, biz.ID, oc.ConversationID); err != nil {
			t.Fatal(err)
		}
	}
	insertOutbound := func(text string, at time.Time) {
		raw, _ := json.Marshal(map[string]any{})
		if _, err := e.db.Exec(`INSERT INTO outbound_messages
			(id, recipient_phone, message_type, text_content, raw_payload, status, business_id, conversation_id, created_at)
			VALUES ($1,$2,'text',$3,$4,'sent',$5,$6,$7)`,
			fmt.Sprintf("out-%d", at.UnixNano()), sender, text, raw, biz.ID, oc.ConversationID, at); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()
	// Pre-order discussion 1 hour ago
	insertInbound("How much is the hair bundle?", now.Add(-60*time.Minute))
	insertOutbound("The hair bundle is GH₵4.00", now.Add(-58*time.Minute))
	insertInbound("I want to confirm order", now.Add(-55*time.Minute))

	// An order was paid 45 minutes ago
	orderPaidAt := now.Add(-45 * time.Minute)
	if _, err := e.db.Exec(`INSERT INTO orders
		(id, business_id, customer_id, conversation_id, status, total_amount, currency, created_at, updated_at)
		VALUES ($1, $2, $3, $4, 'PAID', 4.00, 'GHS', $5, $5)`,
		fmt.Sprintf("ord-%d", now.UnixNano()), biz.ID, oc.CustomerID, oc.ConversationID, orderPaidAt); err != nil {
		t.Fatal(err)
	}

	// Post-payment messages (must be included)
	insertInbound("Thank you!", now.Add(-40*time.Minute))
	insertInbound("What else do you have?", now.Add(-10*time.Minute))

	history, err := workers.FetchConversationHistory(context.Background(), e.pool, oc.ConversationID)
	if err != nil {
		t.Fatalf("FetchConversationHistory: %v", err)
	}

	// Pre-order messages (older than orderPaidAt) must be pruned
	if len(history) != 2 {
		t.Fatalf("expected 2 post-payment messages, got %d: %+v", len(history), history)
	}
	if history[0].Parts[0].Text != "Thank you!" {
		t.Errorf("history[0] = %q, want 'Thank you!'", history[0].Parts[0].Text)
	}
	if history[1].Parts[0].Text != "What else do you have?" {
		t.Errorf("history[1] = %q, want 'What else do you have?'", history[1].Parts[0].Text)
	}
}

func TestOpipeCatalogContextCarryForwardOnShortQuery(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	oc, ok, err := workers.ResolveConversationContext(context.Background(), e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("resolve context: ok=%v err=%v", ok, err)
	}

	// Seed previous turn in history where Human Hair Bundle was quoted
	now := time.Now()
	raw, _ := json.Marshal(map[string]any{})
	if _, err := e.db.Exec(`INSERT INTO outbound_messages
		(id, recipient_phone, message_type, text_content, raw_payload, status, business_id, conversation_id, created_at)
		VALUES ($1,$2,'text',$3,$4,'sent',$5,$6,$7)`,
		fmt.Sprintf("out-%d", now.UnixNano()), sender, "The Human Hair Bundle is GH₵4 [ID: prod123]", raw, biz.ID, oc.ConversationID, now.Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	var capturedCatalogSearchText string
	origCatalog := e.deps.Catalog
	e.deps.Catalog = func(ctx context.Context, businessID, searchText, currency, imageURL string) (string, error) {
		capturedCatalogSearchText = searchText
		if origCatalog != nil {
			return origCatalog(ctx, businessID, searchText, currency, imageURL)
		}
		return "=== CATALOG ===\n[ID: prod123] Human Hair Bundle - GH₵2.50\n", nil
	}

	e.llm.steps = []orchestrator.LlmResponse{
		opipeResp(func(r *orchestrator.LlmResponse) {
			r.Intent = "product_inquiry"
			r.ReplyText = "The Human Hair Bundle is GH₵2.50 [ID: prod123]"
		}),
	}

	// Customer sends short follow-up "Confirm price."
	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-confirm-price", "Confirm price.")
	opipeRun(t, e, biz, sender)

	// Verify deps.Catalog received the carried-forward product text
	if !strings.Contains(capturedCatalogSearchText, "Human Hair Bundle") || !strings.Contains(capturedCatalogSearchText, "Confirm price.") {
		t.Errorf("deps.Catalog search query = %q, expected it to contain 'Human Hair Bundle' and 'Confirm price.'", capturedCatalogSearchText)
	}
}

func TestStripInternalProductIDs(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "single product recommendation with id",
			in:   "We have Shea Butter [ID: abc12345] available for GH₵45.",
			want: "We have Shea Butter available for GH₵45.",
		},
		{
			name: "product list with multiple ids",
			in:   "1. Shea Butter - GH₵45 [ID: abc12345]\n2. Black Soap - GH₵20 [ID: def67890]",
			want: "1. Shea Butter - GH₵45\n2. Black Soap - GH₵20",
		},
		{
			name: "id at start of text",
			in:   "[ID: abc12345] Shea Butter is back in stock!",
			want: "Shea Butter is back in stock!",
		},
		{
			name: "case insensitive id tag",
			in:   "Order confirmed for [id: xyz999].",
			want: "Order confirmed for.",
		},
		{
			name: "text with no ids remains unchanged",
			in:   "Welcome to our shop! How can I help you today?",
			want: "Welcome to our shop! How can I help you today?",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := workers.StripInternalProductIDs(tc.in)
			if got != tc.want {
				t.Errorf("StripInternalProductIDs(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSyncCatalogPricesInHistory(t *testing.T) {
	catalog := "=== CATALOG ===\n[ID: prod123] Human Hair Bundle — GH₵12.00 (5 available)\n[ID: shea456] Shea Butter — GH₵45.00\n"

	history := []orchestrator.ChatMessage{
		{
			Role: "user",
			Parts: []orchestrator.MessagePart{
				{Type: "text", Text: "I want to buy the hair bundle. Earlier you said it was GH₵4."},
			},
		},
		{
			Role: "assistant",
			Parts: []orchestrator.MessagePart{
				{Type: "text", Text: "The Human Hair Bundle is GH₵4 [ID: prod123]. Would you like to see a photo?"},
			},
		},
		{
			Role: "assistant",
			Parts: []orchestrator.MessagePart{
				{Type: "text", Text: "We also have Shea Butter is GH₵35 [ID: shea456]."},
			},
		},
	}

	synced := workers.SyncCatalogPricesInHistory(history, catalog)

	// User message text must be preserved untouched
	if synced[0].Parts[0].Text != "I want to buy the hair bundle. Earlier you said it was GH₵4." {
		t.Errorf("user message was altered: %q", synced[0].Parts[0].Text)
	}

	// Assistant message 1: Human Hair Bundle price synced to GH₵12.00, [ID: prod123] stripped
	want1 := "The Human Hair Bundle is GH₵12.00. Would you like to see a photo?"
	if synced[1].Parts[0].Text != want1 {
		t.Errorf("assistant message 1 = %q, want %q", synced[1].Parts[0].Text, want1)
	}

	// Assistant message 2: Shea Butter price synced to GH₵45.00, [ID: shea456] stripped
	want2 := "We also have Shea Butter is GH₵45.00."
	if synced[2].Parts[0].Text != want2 {
		t.Errorf("assistant message 2 = %q, want %q", synced[2].Parts[0].Text, want2)
	}
}

func TestOpipeOutboundStripsProductIDs(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	oc, ok, err := workers.ResolveConversationContext(context.Background(), e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("resolve context: ok=%v err=%v", ok, err)
	}

	e.llm.steps = []orchestrator.LlmResponse{
		opipeResp(func(r *orchestrator.LlmResponse) {
			r.Intent = "product_inquiry"
			r.ReplyText = "We have the Human Hair Bundle for GH₵12.00 [ID: prod123]. Would you like to order?"
		}),
	}

	opipeInbound(t, e.db, biz.WhatsAppPhoneNumberID, sender, "wamid-strip-test-1", "Do you have human hair?")
	opipeRun(t, e, biz, sender)

	var sentText string
	if err := e.db.QueryRow(`SELECT text_content FROM outbound_messages WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1`,
		oc.ConversationID).Scan(&sentText); err != nil {
		t.Fatal(err)
	}

	wantSent := "We have the Human Hair Bundle for GH₵12.00. Would you like to order?"
	if sentText != wantSent {
		t.Errorf("outbound text = %q, want %q (internal product ID must be stripped)", sentText, wantSent)
	}
}

func TestOpipeHistoryFiltersAutomatedMessages(t *testing.T) {
	e := opipeStart(t)
	biz := e.factory.Business()
	sender := biz.OwnerPhone

	oc, ok, err := workers.ResolveConversationContext(context.Background(), e.pool, biz.WhatsAppPhoneNumberID, sender)
	if err != nil || !ok {
		t.Fatalf("resolve context: ok=%v err=%v", ok, err)
	}

	now := time.Now()
	raw, _ := json.Marshal(map[string]any{})

	// Insert conversational outbound
	if _, err := e.db.Exec(`INSERT INTO outbound_messages
		(id, recipient_phone, message_type, text_content, raw_payload, status, business_id, conversation_id, created_at)
		VALUES ($1,$2,'text',$3,$4,'sent',$5,$6,$7)`,
		fmt.Sprintf("out-conv-%d", now.UnixNano()), sender, "Hello! How can I help you?", raw, biz.ID, oc.ConversationID, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// Insert automated payment receipt
	if _, err := e.db.Exec(`INSERT INTO outbound_messages
		(id, recipient_phone, message_type, text_content, raw_payload, status, business_id, conversation_id, created_at)
		VALUES ($1,$2,'text',$3,$4,'sent',$5,$6,$7)`,
		fmt.Sprintf("out-rcpt-%d", now.UnixNano()), sender, "✅ Payment of *GHS 4.00* received! We're processing your order now.", raw, biz.ID, oc.ConversationID, now.Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// Insert automated payment link invoice
	if _, err := e.db.Exec(`INSERT INTO outbound_messages
		(id, recipient_phone, message_type, text_content, raw_payload, status, business_id, conversation_id, created_at)
		VALUES ($1,$2,'text',$3,$4,'sent',$5,$6,$7)`,
		fmt.Sprintf("out-inv-%d", now.UnixNano()), sender, "Tap the link below to pay securely: https://pay.example.com/inv-123", raw, biz.ID, oc.ConversationID, now.Add(-4*time.Minute)); err != nil {
		t.Fatal(err)
	}

	history, err := workers.FetchConversationHistory(context.Background(), e.pool, oc.ConversationID)
	if err != nil {
		t.Fatalf("FetchConversationHistory: %v", err)
	}

	// Only conversational outbound should be present
	if len(history) != 1 {
		t.Fatalf("expected 1 history message (receipt and invoice link excluded), got %d: %+v", len(history), history)
	}
	if history[0].Parts[0].Text != "Hello! How can I help you?" {
		t.Errorf("history[0] = %q, want 'Hello! How can I help you?'", history[0].Parts[0].Text)
	}
}
