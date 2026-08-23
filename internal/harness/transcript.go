// Stage 8b golden-transcript harness (T8.28a/T8.28b): drive a scripted
// conversation through BOTH stacks and compare DECISION VECTORS per turn —
// {intent, stateBefore->stateAfter, escalatedFlag(+reason), imageIDsSent,
// crmSignalsEmitted, replyLenBucket} — never free text ("decisions identical,
// wording may differ").
//
// Node side: real HTTP webhook posts into StartNodeStack with a scripted
// OpenAI stub. Go side: caller-supplied HTTP-less driver (see
// workers.TranscriptWebhookDriver) over the same Postgres, distinct business.
// With Node==nil the harness round-trips the Go driver against itself.
package harness

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// ---------------------------------------------------------------------------
// decision vectors
// ---------------------------------------------------------------------------

// TurnDecision is one turn's decision vector. Free-text reply content is
// deliberately reduced to ReplyLenBucket.
type TurnDecision struct {
	Intent           string   `json:"intent"`
	StateTransition  string   `json:"state_transition"`
	Escalated        bool     `json:"escalated"`
	EscalationReason string   `json:"escalation_reason,omitempty"`
	ImageIDsSent     []string `json:"image_ids_sent"`
	CRMSignals       string   `json:"crm_signals"`
	ReplyLenBucket   string   `json:"reply_len_bucket"`
}

// TurnSnapshot is the pre-turn baseline used to attribute new rows.
type TurnSnapshot struct {
	BusinessID    string
	CustomerPhone string
	OutboundCount int64
	OrderCount    int64
	State         string
}

// SnapshotTranscriptTurn captures the per-business baseline before a turn.
func SnapshotTranscriptTurn(db *sql.DB, businessID, customerPhone string) (TurnSnapshot, error) {
	snap := TurnSnapshot{BusinessID: businessID, CustomerPhone: customerPhone}
	err := db.QueryRow(`SELECT COALESCE(state::text,''), COALESCE(is_escalated_to_human,false)
		FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
		businessID, customerPhone).Scan(&snap.State, new(bool))
	if err != nil && err != sql.ErrNoRows {
		return snap, fmt.Errorf("transcript: snapshot state: %w", err)
	}
	if snap.State == "" {
		snap.State = "LEAD" // a brand-new conversation enters its first turn at LEAD
	}
	if err := db.QueryRow(trnOutboundCountSQL, businessID, customerPhone).Scan(&snap.OutboundCount); err != nil {
		return snap, fmt.Errorf("transcript: snapshot outbound count: %w", err)
	}
	if err := db.QueryRow(trnOrderCountSQL, businessID, customerPhone).Scan(&snap.OrderCount); err != nil {
		return snap, fmt.Errorf("transcript: snapshot order count: %w", err)
	}
	return snap, nil
}

const (
	trnConvBase         = `(SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2)`
	trnOutboundCountSQL = `SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = ` + trnConvBase
	trnOrderCountSQL    = `SELECT COUNT(*) FROM orders WHERE conversation_id = ` + trnConvBase
)

// CollectTurnDecision extracts the post-turn vector given the pre-turn
// snapshot. tokens maps full product IDs to stable scenario tokens ("p0")
// so cross-stack comparisons never depend on per-business UUIDs.
func CollectTurnDecision(db *sql.DB, before TurnSnapshot, intent string, tokens map[string]string) (TurnDecision, TurnSnapshot, error) {
	after, err := SnapshotTranscriptTurn(db, before.BusinessID, before.CustomerPhone)
	if err != nil {
		return TurnDecision{}, after, err
	}
	dec := TurnDecision{
		Intent:          intent,
		StateTransition: before.State + "->" + after.State,
		ImageIDsSent:    []string{},
		CRMSignals:      "-",
		ReplyLenBucket:  "none",
	}

	var escalated bool
	var reason *string
	if err := db.QueryRow(`SELECT is_escalated_to_human, escalation_reason FROM conversations
		WHERE business_id = $1 AND customer_phone = $2`,
		before.BusinessID, before.CustomerPhone).Scan(&escalated, &reason); err == nil {
		dec.Escalated = escalated
		if reason != nil {
			dec.EscalationReason = *reason
		}
	} else if err != sql.ErrNoRows {
		return dec, after, fmt.Errorf("transcript: read escalation flag: %w", err)
	}

	rows, err := db.Query(`SELECT message_type, COALESCE(text_content,''),
	       COALESCE(product_id,'')
		FROM outbound_messages WHERE conversation_id = `+trnConvBase+`
		ORDER BY created_at ASC OFFSET $3`, before.BusinessID, before.CustomerPhone, before.OutboundCount)
	if err != nil {
		return dec, after, fmt.Errorf("transcript: new outbound rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	maxLen := 0
	for rows.Next() {
		var kind, text, productID string
		if err := rows.Scan(&kind, &text, &productID); err != nil {
			return dec, after, fmt.Errorf("transcript: scan outbound rows: %w", err)
		}
		switch kind {
		case "text":
			if n := len(text); n > maxLen {
				maxLen = n
			}
		case "image":
			tok, ok := tokens[productID]
			if !ok {
				tok = "img:" + productID
			}
			dec.ImageIDsSent = append(dec.ImageIDsSent, tok)
		}
	}
	if err := rows.Err(); err != nil {
		return dec, after, fmt.Errorf("transcript: iterate outbound rows: %w", err)
	}
	dec.ReplyLenBucket = trnLenBucket(maxLen)

	newOrders := after.OrderCount - before.OrderCount
	crm, err := trnCRMVector(db, before.BusinessID, before.CustomerPhone, newOrders)
	if err != nil {
		return dec, after, err
	}
	dec.CRMSignals = crm
	return dec, after, nil
}

// trnCRMVector summarises durable CRM effects into one comparison token:
// orders=<n>,total=<sum>|sentiment=…|area=…|name=…|prefs=<n> ("-"" when all
// absent). Deterministic across stacks because both consume the same script.
func trnCRMVector(db *sql.DB, businessID, customerPhone string, newOrders int64) (string, error) {
	var parts []string
	if newOrders > 0 {
		rows, err := db.Query(`SELECT total_amount::text FROM orders
			WHERE conversation_id = `+trnConvBase+` ORDER BY created_at DESC LIMIT $3`,
			businessID, customerPhone, newOrders)
		if err != nil {
			return "", fmt.Errorf("transcript: order totals: %w", err)
		}
		defer func() { _ = rows.Close() }()
		sum := decimal.Zero
		count := int64(0)
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				return "", fmt.Errorf("transcript: scan order totals: %w", err)
			}
			d, err := decimal.NewFromString(raw)
			if err != nil {
				return "", fmt.Errorf("transcript: parse order total %s: %w", raw, err)
			}
			sum = sum.Add(d)
			count++
		}
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("transcript: iterate order totals: %w", err)
		}
		parts = append(parts, fmt.Sprintf("orders=%d,total=%s", count, sum.StringFixed(2)))
	}

	var name, sentiment, area sql.NullString
	var prefs sql.NullInt64
	err := db.QueryRow(`SELECT c.name, p.sentiment, p.delivery_area,
	       CASE WHEN p.customer_id IS NULL THEN 0 ELSE jsonb_array_length(p.preferences) END
		FROM customers c LEFT JOIN customer_profiles p ON p.customer_id = c.id
		WHERE c.business_id = $1 AND c.phone = $2`, businessID, customerPhone).
		Scan(&name, &sentiment, &area, &prefs)
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("transcript: crm profile read: %w", err)
	}
	if sentiment.Valid && sentiment.String != "" {
		parts = append(parts, "sentiment="+sentiment.String)
	}
	if area.Valid && area.String != "" {
		parts = append(parts, "area="+area.String)
	}
	if name.Valid && name.String != "" {
		parts = append(parts, "name="+name.String)
	}
	if prefs.Valid && prefs.Int64 > 0 {
		parts = append(parts, fmt.Sprintf("prefs=%d", prefs.Int64))
	}
	if len(parts) == 0 {
		return "-", nil
	}
	return strings.Join(parts, "|"), nil
}

func trnLenBucket(n int) string {
	switch {
	case n <= 0:
		return "none"
	case n < 80:
		return "s"
	case n < 160:
		return "m"
	case n < 320:
		return "l"
	default:
		return "xl"
	}
}

// DiffDecisions compares two recorded transcripts element-wise; an empty
// result means decisions are IDENTICAL for every turn.
func DiffDecisions(want, got []TurnDecision) []string {
	var out []string
	if len(want) != len(got) {
		out = append(out, fmt.Sprintf("turn count: want %d, got %d", len(want), len(got)))
	}
	n := len(want)
	if len(got) < n {
		n = len(got)
	}
	for i := 0; i < n; i++ {
		w, g := want[i], got[i]
		if w.Intent != g.Intent {
			out = append(out, fmt.Sprintf("turn %d.intent: want %s, got %s", i, w.Intent, g.Intent))
		}
		if w.StateTransition != g.StateTransition {
			out = append(out, fmt.Sprintf("turn %d.state_transition: want %s, got %s", i, w.StateTransition, g.StateTransition))
		}
		if w.Escalated != g.Escalated || w.EscalationReason != g.EscalationReason {
			out = append(out, fmt.Sprintf("turn %d.escalated: want %v/%s, got %v/%s",
				i, w.Escalated, w.EscalationReason, g.Escalated, g.EscalationReason))
		}
		if strings.Join(w.ImageIDsSent, ",") != strings.Join(g.ImageIDsSent, ",") {
			out = append(out, fmt.Sprintf("turn %d.image_ids_sent: want [%s], got [%s]",
				i, strings.Join(w.ImageIDsSent, ","), strings.Join(g.ImageIDsSent, ",")))
		}
		if w.CRMSignals != g.CRMSignals {
			out = append(out, fmt.Sprintf("turn %d.crm_signals: want %s, got %s", i, w.CRMSignals, g.CRMSignals))
		}
		if w.ReplyLenBucket != g.ReplyLenBucket {
			out = append(out, fmt.Sprintf("turn %d.reply_len_bucket: want %s, got %s", i, w.ReplyLenBucket, g.ReplyLenBucket))
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// scripted scenarios
// ---------------------------------------------------------------------------

// ProductSpec provisions one catalog product (with ImageCount photos) per
// stack; index position doubles as the stable cross-stack token ("p<i>").
type ProductSpec struct {
	Name       string
	Price      string
	ImageCount int
	Stock      int
}

// Item is one detected_items entry referencing a product by index.
type Item struct {
	ProductIndex int
	Quantity     float64
}

// LLMStep mirrors the LlmResponse decision surface; product references use
// scenario indices instead of raw IDs so both stacks receive equivalent
// scripts regardless of per-business UUIDs.
type LLMStep struct {
	ReplyText               string
	Confidence              float64
	Intent                  string
	CustomerRequestedImages bool
	SendImageIndexes        []int
	ReferencedIndexes       []int
	OrderConfirmed          bool
	Items                   []Item
	DeliveryArea            string
	Sentiment               string
}

// Turn couples the inbound message with its scripted LLM decision plus the
// extra settle predicates the Node side needs before collecting the vector.
type Turn struct {
	InboundText string
	LLM         LLMStep
	WantState   string // wait until conversations.state equals this ("" = skip)
	WantOrders  int64  // wait until orders >= this (-1 = skip)
}

// Scenario is a full golden transcript.
type Scenario struct {
	Name     string
	Products []ProductSpec
	Turns    []Turn
}

// ---------------------------------------------------------------------------
// runner
// ---------------------------------------------------------------------------

// GoStackInfo is handed to TranscriptRunnerOpts.GoDriveFactory once the Go
// side's business has been provisioned — the scripted fake LLM needs the real
// product shortIDs to emit equivalent decisions.
type GoStackInfo struct {
	BusinessID  string
	SenderPhone string
	ShortIDs    []string
	Tokens      map[string]string
}

// TranscriptRunnerOpts wires the two stacks.
type TranscriptRunnerOpts struct {
	// Node drives the real NodeStack via HTTP when non-nil; when nil the Go
	// driver runs twice and the diff proves self-consistency.
	Node *NodeStack
	// GoDriveFactory builds the HTTP-less Go-side driver (typically
	// workers.TranscriptWebhookDriver) after provisioning.
	GoDriveFactory func(GoStackInfo) (func(ctx context.Context, payload []byte) error, error)
}

// RunGoldenTranscript provisions two identical businesses (one per stack),
// replays the scenario against each, records per-turn decision vectors and
// returns (nodeVectors, goVectors, diffs). Exported for future CI.
func RunGoldenTranscript(t *testing.T, h *Harness, sc Scenario, opts TranscriptRunnerOpts) ([]TurnDecision, []TurnDecision, []string) {
	t.Helper()

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("transcript: open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	factory := NewFactory(t, db)
	var nodeVecs []TurnDecision
	if opts.Node != nil {
		nodeEnv := trnProvision(t, factory, sc, "node")
		nodeVecs = trnRunNodeSide(t, db, sc, opts.Node, nodeEnv)
	}

	goEnv := trnProvision(t, factory, sc, "go")
	goDrive := trnBuildDriver(t, opts.GoDriveFactory, goEnv)
	goVecs := trnRunGoSide(t, db, sc, goDrive, goEnv)

	want := nodeVecs
	if want == nil { // self round-trip: replay once more, diff vs first run
		go2Env := trnProvision(t, factory, sc, "go2")
		go2Drive := trnBuildDriver(t, opts.GoDriveFactory, go2Env)
		want = trnRunGoSide(t, db, sc, go2Drive, go2Env)
	}
	return want, goVecs, DiffDecisions(want, goVecs)
}

func trnBuildDriver(t *testing.T, make func(GoStackInfo) (func(ctx context.Context, payload []byte) error, error), env trnEnv) func(ctx context.Context, payload []byte) error {
	t.Helper()
	drive, err := make(GoStackInfo{
		BusinessID:  env.biz.ID,
		SenderPhone: env.senderPhone,
		ShortIDs:    env.shortIDs,
		Tokens:      env.tokens,
	})
	if err != nil {
		t.Fatalf("transcript: build go driver: %v", err)
	}
	return drive
}

type trnEnv struct {
	biz         Business
	senderPhone string
	shortIDs    []string // per product, first 8 hex chars
	tokens      map[string]string
}

func trnProvision(t *testing.T, factory *Factory, sc Scenario, tag string) trnEnv {
	t.Helper()
	biz := factory.Business()
	env := trnEnv{biz: biz, senderPhone: biz.OwnerPhone, tokens: map[string]string{}}
	for i, spec := range sc.Products {
		prod := factory.ProductWithImages(biz.ID, spec.ImageCount, WithPrice(spec.Price), WithStock(spec.Stock))
		_, _ = factory.DB.Exec(`UPDATE products SET name = $1 WHERE id = $2`,
			fmt.Sprintf("%s %s", spec.Name, tag), prod.ID)
		env.shortIDs = append(env.shortIDs, prod.ID[:8])
		env.tokens[prod.ID] = fmt.Sprintf("p%d", i)
	}
	return env
}

func trnRunNodeSide(t *testing.T, db *sql.DB, sc Scenario, stack *NodeStack, env trnEnv) []TurnDecision {
	t.Helper()
	call := 0
	stack.SetChatReply(func(map[string]any) map[string]any {
		step := sc.Turns[call].LLM
		call++
		return trnStepToStub(step, env.shortIDs)
	})

	vecs := make([]TurnDecision, 0, len(sc.Turns))
	for i, turn := range sc.Turns {
		before, err := SnapshotTranscriptTurn(db, env.biz.ID, env.senderPhone)
		if err != nil {
			t.Fatalf("transcript[node]: snapshot turn %d: %v", i, err)
		}
		msgID := fmt.Sprintf("wamid.%s-node-%d-%d", sc.Name, i, time.Now().UnixNano())
		payload := trnWhatsAppPayload("waba-"+sc.Name+"-node", env.biz.WhatsAppPhoneNumberID, msgID, env.senderPhone, turn.InboundText)
		if code := trnPostWebhook(stack, payload); code != http.StatusOK {
			t.Fatalf("transcript[node]: webhook turn %d status %d\n--- logs ---\n%s", i, code, stack.DumpLogs())
		}
		trnSettle(t, db, env, before, turn, fmt.Sprintf("transcript[node] turn %d", i), stack)
		vec, _, err := CollectTurnDecision(db, before, turn.LLM.Intent, env.tokens)
		if err != nil {
			t.Fatalf("transcript[node]: collect turn %d: %v", i, err)
		}
		vecs = append(vecs, vec)
	}
	return vecs
}

func trnRunGoSide(t *testing.T, db *sql.DB, sc Scenario, drive func(ctx context.Context, payload []byte) error, env trnEnv) []TurnDecision {
	t.Helper()
	vecs := make([]TurnDecision, 0, len(sc.Turns))
	for i, turn := range sc.Turns {
		before, err := SnapshotTranscriptTurn(db, env.biz.ID, env.senderPhone)
		if err != nil {
			t.Fatalf("transcript[go]: snapshot turn %d: %v", i, err)
		}
		msgID := fmt.Sprintf("wamid.%s-go-%d-%d", sc.Name, i, time.Now().UnixNano())
		payload := trnWhatsAppPayload("waba-"+sc.Name+"-go", env.biz.WhatsAppPhoneNumberID, msgID, env.senderPhone, turn.InboundText)
		if err := drive(context.Background(), payload); err != nil {
			t.Fatalf("transcript[go]: drive turn %d: %v", i, err)
		}
		vec, _, err := CollectTurnDecision(db, before, turn.LLM.Intent, env.tokens)
		if err != nil {
			t.Fatalf("transcript[go]: collect turn %d: %v", i, err)
		}
		vecs = append(vecs, vec)
	}
	return vecs
}

// trnSettle polls until the turn's durable effects landed on the Node side
// (its pipeline is async through BullMQ).
func trnSettle(t *testing.T, db *sql.DB, env trnEnv, before TurnSnapshot, turn Turn, label string, stack *NodeStack) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		var outCount, orderCount int64
		var state string
		if err := db.QueryRow(trnOutboundCountSQL, env.biz.ID, env.senderPhone).Scan(&outCount); err != nil {
			t.Fatalf("%s: outbound count: %v", label, err)
		}
		if err := db.QueryRow(trnOrderCountSQL, env.biz.ID, env.senderPhone).Scan(&orderCount); err != nil {
			t.Fatalf("%s: order count: %v", label, err)
		}
		if err := db.QueryRow(`SELECT COALESCE(state::text,'') FROM conversations
			WHERE business_id = $1 AND customer_phone = $2`, env.biz.ID, env.senderPhone).Scan(&state); err != nil && err != sql.ErrNoRows {
			t.Fatalf("%s: state: %v", label, err)
		}
		settled := outCount > before.OutboundCount &&
			(turn.WantState == "" || state == turn.WantState) &&
			(turn.WantOrders < 0 || orderCount >= turn.WantOrders)
		if settled && trnProfileSentimentSettled(t, db, env, turn) {
			time.Sleep(300 * time.Millisecond) // let CRM materialiser tail settle
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s: did not settle within 45s\n--- node logs ---\n%s", label, stack.DumpLogs())
}

// trnProfileSentimentSettled waits until the materialiser's profile_builder
// has written this turn's scripted sentiment (every LlmResponse carries a
// schema-required sentiment), so CRM-effect vectors are collected only after
// the signal job drained on the async Node side too.
func trnProfileSentimentSettled(t *testing.T, db *sql.DB, env trnEnv, turn Turn) bool {
	t.Helper()
	if turn.LLM.Sentiment == "" {
		return true
	}
	var sentiment sql.NullString
	err := db.QueryRow(`SELECT p.sentiment
		FROM customers c LEFT JOIN customer_profiles p ON p.customer_id = c.id
		WHERE c.business_id = $1 AND c.phone = $2`, env.biz.ID, env.senderPhone).Scan(&sentiment)
	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		t.Fatalf("transcript: profile sentiment poll: %v", err)
	}
	return sentiment.Valid && sentiment.String == turn.LLM.Sentiment
}

// trnStepToStub converts a scenario step into the OpenAI-stub chat reply,
// mapping product indices onto this stack's short IDs.
func trnStepToStub(step LLMStep, shortIDs []string) map[string]any {
	items := make([]any, 0, len(step.Items))
	for _, it := range step.Items {
		items = append(items, map[string]any{
			"product_id": shortIDs[it.ProductIndex],
			"quantity":   it.Quantity,
		})
	}
	imageIDs := make([]any, 0, len(step.SendImageIndexes))
	for _, idx := range step.SendImageIndexes {
		imageIDs = append(imageIDs, shortIDs[idx])
	}
	refIDs := make([]any, 0, len(step.ReferencedIndexes))
	for _, idx := range step.ReferencedIndexes {
		refIDs = append(refIDs, shortIDs[idx])
	}
	deliveryArea := any(nil)
	if step.DeliveryArea != "" {
		deliveryArea = step.DeliveryArea
	}
	sentiment := step.Sentiment
	if sentiment == "" {
		sentiment = "neutral" // schema-required enum; scenario may omit it
	}
	return map[string]any{
		"reply_text":                step.ReplyText,
		"internal_confidence":       step.Confidence,
		"intent":                    step.Intent,
		"customer_requested_images": step.CustomerRequestedImages,
		"send_product_image_ids":    imageIDs,
		"referenced_product_ids":    refIDs,
		"crm_signals": map[string]any{
			"order_confirmed":      step.OrderConfirmed,
			"detected_items":       items,
			"delivery_area":        deliveryArea,
			"customer_name":        nil,
			"detected_preferences": []any{},
			"sentiment":            sentiment,
		},
	}
}

// ---------------------------------------------------------------------------
// minimal WhatsApp webhook plumbing (non-test twins of the characterization
// helpers so transcript.go stays importable from other packages)
// ---------------------------------------------------------------------------

func trnWhatsAppPayload(wabaID, phoneNumberID, messageID, fromPhone, body string) []byte {
	p := map[string]any{
		"object": "whatsapp_business_account",
		"entry": []any{map[string]any{
			"id": wabaID,
			"changes": []any{map[string]any{
				"field": "messages",
				"value": map[string]any{
					"messaging_product": "whatsapp",
					"metadata": map[string]any{
						"display_phone_number": "233200000000",
						"phone_number_id":      phoneNumberID,
					},
					"messages": []any{map[string]any{
						"id":        messageID,
						"from":      fromPhone,
						"timestamp": fmt.Sprint(time.Now().Unix()),
						"type":      "text",
						"text":      map[string]string{"body": body},
					}},
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

func trnPostWebhook(stack *NodeStack, payload []byte) int {
	mac := hmac.New(sha256.New, []byte(testWhatsAppAppSecret))
	mac.Write(payload)
	req, err := http.NewRequest(http.MethodPost, stack.BaseURL+"/webhooks/whatsapp", bytes.NewReader(payload))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}
