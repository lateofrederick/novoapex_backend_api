package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// ---------------------------------------------------------------------------
// outbound-queue consumer
// ---------------------------------------------------------------------------

type fakeWA struct {
	mu    sync.Mutex
	calls []string
	fail  error
}

func (f *fakeWA) record(kind, pnid, to, a, b string) (map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("%s|%s|%s|%s|%s", kind, pnid, to, a, b))
	if f.fail != nil {
		return nil, f.fail
	}
	return map[string]any{"messages": []any{map[string]any{"id": "wamid.test-" + kind}}}, nil
}

func (f *fakeWA) SendTextMessageData(_ context.Context, pnid, to, text string) (map[string]any, error) {
	return f.record("text", pnid, to, text, "")
}

func (f *fakeWA) SendImageMessage(_ context.Context, pnid, to, url, caption string) (map[string]any, error) {
	return f.record("image", pnid, to, url, caption)
}

func (f *fakeWA) SendTemplateMessage(_ context.Context, pnid, to, name, lang string) (map[string]any, error) {
	return f.record("template", pnid, to, name, lang)
}

func (f *fakeWA) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func seedOutboundRow(t *testing.T, db *sql.DB, bizID, convID, phone, msgType, status string, text, imageURL, template *string) string {
	t.Helper()
	id := s7a_seedOutbound(t, db, bizID, convID, phone, status, time.Now())
	if _, err := db.Exec(`UPDATE outbound_messages SET message_type = $2, text_content = $3, image_url = $4, template_name = $5 WHERE id = $1`,
		id, msgType, text, imageURL, template); err != nil {
		t.Fatalf("shape outbound row: %v", err)
	}
	return id
}

func TestOutbound_SendsByTypeAndRecordsOutcome(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	conv := factory.Conversation(biz.ID, cust.Phone)

	t.Run("text is sent and marked sent with the Meta message id", func(t *testing.T) {
		wa := &fakeWA{}
		id := seedOutboundRow(t, s.db, biz.ID, conv.ID, cust.Phone, "text", "pending", ptr("hello there"), nil, nil)
		if err := HandleOutboundSend(t.Context(), OutboundDeps{Pool: s.pool, WA: wa}, id, true); err != nil {
			t.Fatalf("HandleOutboundSend: %v", err)
		}
		want := fmt.Sprintf("text|%s|%s|hello there|", biz.WhatsAppPhoneNumberID, cust.Phone)
		if got := wa.snapshot(); len(got) != 1 || got[0] != want {
			t.Fatalf("wa calls = %v, want [%s]", got, want)
		}
		if st := s7a_scalar(t, s.db, `SELECT status FROM outbound_messages WHERE id = $1`, id); st != "sent" {
			t.Errorf("status = %q, want sent", st)
		}
		if wamid := s7a_scalar(t, s.db, `SELECT whatsapp_message_id FROM outbound_messages WHERE id = $1`, id); wamid != "wamid.test-text" {
			t.Errorf("whatsapp_message_id = %q", wamid)
		}
		if meta := s7a_scalar(t, s.db, `SELECT meta_response->'messages'->0->>'id' FROM outbound_messages WHERE id = $1`, id); meta != "wamid.test-text" {
			t.Errorf("meta_response not persisted: %q", meta)
		}

		// A redelivered task for an already-sent row must not double-send.
		if err := HandleOutboundSend(t.Context(), OutboundDeps{Pool: s.pool, WA: wa}, id, true); err != nil {
			t.Fatalf("second HandleOutboundSend: %v", err)
		}
		if got := wa.snapshot(); len(got) != 1 {
			t.Errorf("sent row was re-sent: %v", got)
		}
	})

	t.Run("image and template dispatch to their Meta calls", func(t *testing.T) {
		wa := &fakeWA{}
		img := seedOutboundRow(t, s.db, biz.ID, conv.ID, cust.Phone, "image", "pending", ptr("Shea — GH₵45.00"), ptr("https://img.example/shea.jpg"), nil)
		tpl := seedOutboundRow(t, s.db, biz.ID, conv.ID, cust.Phone, "template", "pending", nil, nil, ptr("new_arrivals_v1"))
		for _, id := range []string{img, tpl} {
			if err := HandleOutboundSend(t.Context(), OutboundDeps{Pool: s.pool, WA: wa}, id, true); err != nil {
				t.Fatalf("HandleOutboundSend(%s): %v", id, err)
			}
		}
		got := wa.snapshot()
		wantImg := fmt.Sprintf("image|%s|%s|https://img.example/shea.jpg|Shea — GH₵45.00", biz.WhatsAppPhoneNumberID, cust.Phone)
		wantTpl := fmt.Sprintf("template|%s|%s|new_arrivals_v1|en_US", biz.WhatsAppPhoneNumberID, cust.Phone)
		if len(got) != 2 || got[0] != wantImg || got[1] != wantTpl {
			t.Fatalf("wa calls = %v", got)
		}
	})

	t.Run("compliance-blocked rows are never sent", func(t *testing.T) {
		wa := &fakeWA{}
		for _, status := range []string{"failed_24h_window_closed", "failed_stale"} {
			id := seedOutboundRow(t, s.db, biz.ID, conv.ID, cust.Phone, "text", status, ptr("blocked"), nil, nil)
			if err := HandleOutboundSend(t.Context(), OutboundDeps{Pool: s.pool, WA: wa}, id, true); err != nil {
				t.Fatalf("HandleOutboundSend(%s): %v", status, err)
			}
		}
		if got := wa.snapshot(); len(got) != 0 {
			t.Errorf("blocked rows were sent: %v", got)
		}
	})

	t.Run("delivery failure stays pending until the final attempt marks it failed", func(t *testing.T) {
		wa := &fakeWA{fail: errors.New("Meta API error: rate limited")}
		id := seedOutboundRow(t, s.db, biz.ID, conv.ID, cust.Phone, "text", "pending", ptr("retry me"), nil, nil)

		if err := HandleOutboundSend(t.Context(), OutboundDeps{Pool: s.pool, WA: wa}, id, false); err == nil {
			t.Fatal("non-final failure must return an error so asynq retries")
		}
		if st := s7a_scalar(t, s.db, `SELECT status FROM outbound_messages WHERE id = $1`, id); st != "pending" {
			t.Errorf("status after retryable failure = %q, want pending", st)
		}

		if err := HandleOutboundSend(t.Context(), OutboundDeps{Pool: s.pool, WA: wa}, id, true); err == nil {
			t.Fatal("final failure must still return an error (reported + archived)")
		}
		if st := s7a_scalar(t, s.db, `SELECT status FROM outbound_messages WHERE id = $1`, id); st != "failed" {
			t.Errorf("status after exhausted retries = %q, want failed", st)
		}
		if msg := s7a_scalar(t, s.db, `SELECT meta_response->>'error' FROM outbound_messages WHERE id = $1`, id); msg == "" {
			t.Error("meta_response.error not recorded")
		}
	})

	t.Run("concurrent out-of-order tasks deliver a conversation in write order exactly once", func(t *testing.T) {
		wa := &fakeWA{}
		orderedConv := factory.Conversation(biz.ID, "233209999000")
		var ids []string
		for i := 0; i < 12; i++ {
			ids = append(ids, seedOutboundRow(t, s.db, biz.ID, orderedConv.ID, "233209999000", "text", "pending", ptr(fmt.Sprintf("msg-%02d", i)), nil, nil))
		}
		var wg sync.WaitGroup
		errs := make(chan error, len(ids))
		for i := len(ids) - 1; i >= 0; i-- { // newest tasks start first
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				errs <- HandleOutboundSend(context.Background(), OutboundDeps{Pool: s.pool, WA: wa}, id, true)
			}(ids[i])
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("HandleOutboundSend: %v", err)
			}
		}
		got := wa.snapshot()
		if len(got) != len(ids) {
			t.Fatalf("sends = %d, want %d (no duplicates, none lost): %v", len(got), len(ids), got)
		}
		for i, call := range got {
			if want := fmt.Sprintf("|msg-%02d|", i); !strings.Contains(call, want) {
				t.Fatalf("send %d = %s, want %s — delivery order must follow write order", i, call, want)
			}
		}
	})

	t.Run("missing row is dropped without retry", func(t *testing.T) {
		if err := HandleOutboundSend(t.Context(), OutboundDeps{Pool: s.pool, WA: &fakeWA{}}, "does-not-exist", true); err != nil {
			t.Fatalf("missing row: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// webhook-processing consumer
// ---------------------------------------------------------------------------

func webhookJob(t *testing.T, key string, message map[string]any) WebhookJob {
	t.Helper()
	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return WebhookJob{IdempotencyKey: key, Payload: raw, ReceivedAt: time.Now().UTC().Format(time.RFC3339)}
}

func TestWebhookProcessing_PersistsInboundAndDebouncesOrchestrator(t *testing.T) {
	s := s7a_startStack(t)
	const recipient, sender = "1122334455", "233201234567"

	t.Run("text message", func(t *testing.T) {
		pub := &s7a_fakePublisher{}
		job := webhookJob(t, "waba:"+sender+":wamid.T1", map[string]any{
			"id": "wamid.T1", "from": sender, "type": "text", "timestamp": "1767225600",
			"text": map[string]any{"body": "do you have shea butter?"}, "recipientPhone": recipient,
		})
		if err := HandleWebhookJob(t.Context(), WebhookDeps{Pool: s.pool, Publisher: pub}, job, false); err != nil {
			t.Fatalf("HandleWebhookJob: %v", err)
		}

		if got := s7a_scalar(t, s.db, `SELECT text_content FROM inbound_messages WHERE whatsapp_message_id = 'wamid.T1'`); got != "do you have shea butter?" {
			t.Errorf("text_content = %q", got)
		}
		if got := s7a_scalar(t, s.db, `SELECT to_char("timestamp", 'YYYY-MM-DD"T"HH24:MI:SS') FROM inbound_messages WHERE whatsapp_message_id = 'wamid.T1'`); got != "2026-01-01T00:00:00" {
			t.Errorf("timestamp = %q, want UTC 2026-01-01T00:00:00", got)
		}
		if got := s7a_scalar(t, s.db, `SELECT raw_payload->>'recipientPhone' FROM inbound_messages WHERE whatsapp_message_id = 'wamid.T1'`); got != recipient {
			t.Errorf("raw_payload not preserved: recipientPhone = %q", got)
		}
		if n := s7a_count(t, s.db, `SELECT count(*) FROM webhook_events WHERE idempotency_key = $1`, job.IdempotencyKey); n != 1 {
			t.Errorf("webhook_events rows = %d, want 1", n)
		}

		calls := pub.recorded()
		if len(calls) != 1 {
			t.Fatalf("enqueues = %d, want 1", len(calls))
		}
		c := calls[0]
		if c.Queue != queue.QOrchestrator || c.TaskType != queue.TaskOrchestratorDebounce {
			t.Errorf("target = %s/%s", c.Queue, c.TaskType)
		}
		if c.Opts == nil || c.Opts.TaskID != OrchestratorDebounceTaskID(recipient, sender) || c.Opts.ProcessIn != OrchestratorDebounceDelay {
			t.Errorf("debounce opts = %+v", c.Opts)
		}
		var oj OrchestratorJob
		_ = json.Unmarshal(c.Payload, &oj)
		if oj.RecipientPhone != recipient || oj.SenderPhone != sender {
			t.Errorf("orchestrator payload = %+v", oj)
		}
	})

	t.Run("image without caption and audio text extraction", func(t *testing.T) {
		pub := &s7a_fakePublisher{}
		deps := WebhookDeps{Pool: s.pool, Publisher: pub}
		img := webhookJob(t, "k-img", map[string]any{"id": "wamid.IMG", "from": sender, "type": "image",
			"image": map[string]any{"id": "media-1"}, "recipientPhone": recipient})
		aud := webhookJob(t, "k-aud", map[string]any{"id": "wamid.AUD", "from": sender, "type": "audio",
			"audio": map[string]any{"id": "media-2"}, "recipientPhone": recipient})
		for _, j := range []WebhookJob{img, aud} {
			if err := HandleWebhookJob(t.Context(), deps, j, false); err != nil {
				t.Fatalf("HandleWebhookJob(%s): %v", j.IdempotencyKey, err)
			}
		}
		if got := s7a_scalar(t, s.db, `SELECT text_content FROM inbound_messages WHERE whatsapp_message_id = 'wamid.IMG'`); got != imageWithoutCaptionText {
			t.Errorf("captionless image text = %q", got)
		}
		if n := s7a_count(t, s.db, `SELECT count(*) FROM inbound_messages WHERE whatsapp_message_id = 'wamid.AUD' AND text_content IS NULL`); n != 1 {
			t.Error("audio text_content must be NULL (transcribed later)")
		}
	})

	t.Run("duplicate skips; a retry after a failed enqueue re-publishes", func(t *testing.T) {
		deps := WebhookDeps{Pool: s.pool}
		job := webhookJob(t, "k-dup", map[string]any{"id": "wamid.DUP", "from": sender, "type": "text",
			"text": map[string]any{"body": "hi"}, "recipientPhone": recipient})

		failing := &s7a_fakePublisher{Fail: errors.New("redis down")}
		deps.Publisher = failing
		if err := HandleWebhookJob(t.Context(), deps, job, false); err == nil {
			t.Fatal("enqueue failure must fail the job so it is retried")
		}
		if n := s7a_count(t, s.db, `SELECT count(*) FROM inbound_messages WHERE whatsapp_message_id = 'wamid.DUP'`); n != 1 {
			t.Fatalf("inbound row must be committed before the enqueue, rows = %d", n)
		}

		pub := &s7a_fakePublisher{}
		deps.Publisher = pub
		if err := HandleWebhookJob(t.Context(), deps, job, false); err != nil {
			t.Fatalf("duplicate delivery: %v", err)
		}
		if len(pub.recorded()) != 0 {
			t.Error("a plain duplicate must not re-trigger the orchestrator")
		}
		if err := HandleWebhookJob(t.Context(), deps, job, true); err != nil {
			t.Fatalf("retry: %v", err)
		}
		if len(pub.recorded()) != 1 {
			t.Errorf("retry of a persisted message must enqueue the orchestrator, got %d", len(pub.recorded()))
		}
		if n := s7a_count(t, s.db, `SELECT count(*) FROM inbound_messages WHERE whatsapp_message_id = 'wamid.DUP'`); n != 1 {
			t.Errorf("retries must not duplicate the inbound row, rows = %d", n)
		}
	})
}

// ---------------------------------------------------------------------------
// acquisition channel (customer-capture updateAcquisitionChannel)
// ---------------------------------------------------------------------------

func TestAcquisitionChannel_FromFirstReferralWriteOnce(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	biz := factory.Business()
	deps := WebhookDeps{Pool: s.pool, Publisher: &s7a_fakePublisher{}}

	ingest := func(id, sender string, referral map[string]any) {
		t.Helper()
		msg := map[string]any{"id": id, "from": sender, "type": "text",
			"text": map[string]any{"body": "hi"}, "recipientPhone": biz.WhatsAppPhoneNumberID}
		if referral != nil {
			msg["referral"] = referral
		}
		if err := HandleWebhookJob(t.Context(), deps, webhookJob(t, "k-"+id, msg), false); err != nil {
			t.Fatal(err)
		}
	}
	channelOf := func(sender string) string {
		t.Helper()
		rc, ok, err := ResolveConversationContext(t.Context(), s.pool, biz.WhatsAppPhoneNumberID, sender)
		if err != nil || !ok {
			t.Fatalf("resolve: ok=%v err=%v", ok, err)
		}
		return s7a_scalar(t, s.db, `SELECT acquisition_channel FROM customers WHERE id = $1`, rc.CustomerID)
	}

	t.Run("click-to-WhatsApp ad referral sets the channel", func(t *testing.T) {
		ingest("wamid.ad1", "233201110001", map[string]any{"source_type": "ad", "source_id": "120212345678", "source_url": "https://fb.me/x", "headline": "Shea sale"})
		if got := channelOf("233201110001"); got != "ad:120212345678" {
			t.Fatalf("channel = %q, want ad:120212345678", got)
		}
	})

	t.Run("the first attribution sticks when a later message carries another referral", func(t *testing.T) {
		ingest("wamid.ad2", "233201110001", map[string]any{"source_type": "post", "source_id": "999"})
		if got := channelOf("233201110001"); got != "ad:120212345678" {
			t.Errorf("channel = %q, want the original ad attribution", got)
		}
	})

	t.Run("no referral stays organic", func(t *testing.T) {
		ingest("wamid.org1", "233201110002", nil)
		if got := channelOf("233201110002"); got != "organic" {
			t.Errorf("channel = %q, want organic", got)
		}
	})

	t.Run("referral format", func(t *testing.T) {
		for want, ref := range map[string]map[string]any{
			"ad:1":       {"source_type": "ad", "source_id": "1"},
			"post":       {"source_type": "post"},
			"referral:7": {"source_id": "7"},
			"referral":   {},
		} {
			if got := AcquisitionChannelFromReferral(ref); got != want {
				t.Errorf("AcquisitionChannelFromReferral(%v) = %q, want %q", ref, got, want)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// publish-after-commit ordering
// ---------------------------------------------------------------------------

// visibilityPublisher checks, at enqueue time and from a separate connection,
// that the outbound row the job references is already committed.
type visibilityPublisher struct {
	db        *sql.DB
	mu        sync.Mutex
	invisible []string
	count     int
}

func (v *visibilityPublisher) Enqueue(_ context.Context, _, _ string, payload any, _ *queue.EnqueueOpts) error {
	raw, _ := json.Marshal(payload)
	var job queue.OutboundJob
	_ = json.Unmarshal(raw, &job)
	var n int
	_ = v.db.QueryRow(`SELECT count(*) FROM outbound_messages WHERE id = $1`, job.OutboundMessageID).Scan(&n)
	v.mu.Lock()
	defer v.mu.Unlock()
	v.count++
	if n == 0 {
		v.invisible = append(v.invisible, job.OutboundMessageID)
	}
	return nil
}

func TestNewArrivals_PublishesOnlyCommittedRows(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	biz := factory.Business()
	if _, err := s.db.Exec(`UPDATE businesses SET new_arrivals_template_name = 'tmpl_v1' WHERE id = $1`, biz.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		cust := factory.Customer(biz.ID)
		s7a_optIn(t, s.db, cust.ID)
		factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))
	}
	s7a_seedProduct(t, s.db, biz.ID, "hair", time.Now().UTC())

	pub := &visibilityPublisher{db: s.db}
	if err := SweepNewArrivals(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepNewArrivals: %v", err)
	}
	if pub.count != 3 {
		t.Fatalf("digests published = %d, want 3", pub.count)
	}
	if len(pub.invisible) != 0 {
		t.Errorf("published %d outbound ids before their rows were committed: %v", len(pub.invisible), pub.invisible)
	}
}

// ---------------------------------------------------------------------------
// terminal orchestrator failures free the debounce id
// ---------------------------------------------------------------------------

func TestDiscardMarksRevokeAndKeepsCause(t *testing.T) {
	cause := errors.New("llm unavailable")
	err := queue.Discard(fmt.Errorf("orchestrator: %w", cause))
	if !errors.Is(err, asynq.RevokeTask) {
		t.Error("Discard must carry asynq.RevokeTask so the task is deleted, not archived")
	}
	if !errors.Is(err, cause) {
		t.Error("Discard must preserve the original cause")
	}
	if err.Error() != "orchestrator: llm unavailable" {
		t.Errorf("message = %q, want the original text", err.Error())
	}
	if queue.Discard(nil) != nil {
		t.Error("Discard(nil) must be nil")
	}
}
