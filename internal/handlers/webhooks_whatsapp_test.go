package handlers

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// ---- fixtures ---------------------------------------------------------------

const (
	s5_appSecret   = "s5-app-secret-0123456789abcdef" // mirrors WHATSAPP_APP_SECRET
	s5_verifyToken = "s5-verify-token"
)

// s5_fakePublisher records Enqueue calls; err (when non-nil) is returned.
type s5_fakePublisher struct {
	mu    sync.Mutex
	calls []s5Enqueue
	err   error
}

type s5Enqueue struct {
	Queue    string
	TaskType string
	Payload  []byte
	Opts     *queue.EnqueueOpts
}

func (p *s5_fakePublisher) Enqueue(_ context.Context, q, taskType string, payload any, opts *queue.EnqueueOpts) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	call := s5Enqueue{Queue: q, TaskType: taskType, Payload: raw}
	if opts != nil {
		copied := *opts
		call.Opts = &copied
	}
	p.calls = append(p.calls, call)
	return p.err
}

func (p *s5_fakePublisher) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// s5_hubSign computes X-Hub-Signature-256 over the raw body.
func s5_hubSign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// s5_waBody builds a Meta webhook envelope around one message.
func s5_waBody(object, wabaID, phoneID, msgID, from string, extra map[string]any) []byte {
	message := map[string]any{
		"id":        msgID,
		"from":      from,
		"timestamp": 1755854400,
		"type":      "text",
		"text":      map[string]any{"body": "hello"},
	}
	for k, v := range extra {
		message[k] = v
	}
	body := map[string]any{
		"object": object,
		"entry": []any{map[string]any{
			"id": wabaID,
			"changes": []any{map[string]any{
				"value": map[string]any{
					"messaging_product": "whatsapp",
					"metadata":          map[string]any{"display_phone_number": "233201234567", "phone_number_id": phoneID},
					"contacts":          []any{map[string]any{"profile": map[string]any{"name": "Kofi"}, "wa_id": from}},
					"messages":          []any{message},
				},
			}},
		}},
	}
	raw, _ := json.Marshal(body)
	return raw
}

func s5_mountWebhooks(t *testing.T, e *epEnv, pub queue.Publisher, appSecret, verifyToken string) http.Handler {
	t.Helper()
	r := chi.NewRouter()
	MountWebhooks(r, WebhookDeps{
		Pool:           e.Pool,
		Publisher:      pub,
		AppSecret:      appSecret,
		VerifyToken:    verifyToken,
		PaystackSecret: "sk_test_s5",
	})
	return r
}

func s5_postWhatsApp(t *testing.T, h http.Handler, body []byte, signature string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/whatsapp", bytes.NewReader(body))
	if signature != "" {
		req.Header.Set("X-Hub-Signature-256", signature)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func s5_webhookEventCount(t *testing.T, e *epEnv) int {
	t.Helper()
	var n int
	if err := e.DB.QueryRow(`SELECT COUNT(*) FROM webhook_events`).Scan(&n); err != nil {
		t.Fatalf("count webhook_events: %v", err)
	}
	return n
}

// ---- T5.1 GET hub challenge --------------------------------------------------

func TestS5_T5_1_VerifyChallenge(t *testing.T) {
	e := ep_startTest(t)
	h := s5_mountWebhooks(t, e, &s5_fakePublisher{}, s5_appSecret, s5_verifyToken)

	t.Run("valid handshake echoes the RAW challenge as text/html", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			"/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token="+s5_verifyToken+"&hub.challenge=1158201444", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); got != "1158201444" { // no quotes, no newline, no envelope
			t.Fatalf("challenge body = %q, want raw string", got)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("content-type = %q (Nest res.send semantics)", ct)
		}
	})

	t.Run("wrong token -> ForbiddenException envelope", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			"/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token=WRONG&hub.challenge=x", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status=%d", rec.Code)
		}
		v := ep_decode(t, rec)
		if v["statusCode"] != json_Number("403") || v["message"] != "Verification failed" {
			t.Fatalf("envelope = %v", v)
		}
	})

	t.Run("wrong mode -> forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			"/webhooks/whatsapp?hub.mode=denied&hub.verify_token="+s5_verifyToken+"&hub.challenge=x", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status=%d", rec.Code)
		}
	})
}

// ---- T5.10a-e signature rejection matrix ------------------------------------

func TestS5_T5_10_SignatureRejectionMatrix(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountWebhooks(t, e, pub, s5_appSecret, s5_verifyToken)

	good := s5_waBody("whatsapp_business_account", "WABA1", "PNID1", "wamid.m1", "+233500000001", nil)
	validSig := s5_hubSign(s5_appSecret, good)
	differentBody := s5_waBody("whatsapp_business_account", "WABA1", "PNID1", "wamid.OTHER", "+233500000001", nil)

	cases := []struct {
		name      string
		body      []byte
		signature string
	}{
		{"T5.10a tampered body", good, s5_hubSign(s5_appSecret, differentBody)},
		{"T5.10b missing header", good, ""},
		{"T5.10c wrong secret", good, s5_hubSign("not-the-secret", good)},
		{"T5.10d truncated signature", good, validSig[:len(validSig)-4]},
		{"T5.10e valid signature over different body", differentBody, validSig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := s5_postWhatsApp(t, h, tc.body, tc.signature)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			v := ep_decode(t, rec)
			if v["statusCode"] != json_Number("403") || v["message"] != "Invalid signature" {
				t.Fatalf("envelope = %v", v)
			}
			if n := pub.len(); n != 0 {
				t.Errorf("rejected request enqueued %d job(s)", n)
			}
			if n := s5_webhookEventCount(t, e); n != 0 {
				t.Errorf("rejected request wrote %d webhook_events row(s)", n)
			}
		})
	}
}

// T5.4 unset WHATSAPP_APP_SECRET -> warn-and-skip, request still processed.
func TestS5_T5_4_UnsetSecretSkipsVerification(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountWebhooks(t, e, pub, "", s5_verifyToken)

	body := s5_waBody("whatsapp_business_account", "WABA1", "PNID1", "wamid.skip1", "+233500000002", nil)
	rec := s5_postWhatsApp(t, h, body, "") // NO signature header at all
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if pub.len() != 1 {
		t.Fatalf("skip-mode must still enqueue, calls=%d", pub.len())
	}
}

// T5.6 object !== whatsapp_business_account early return.
func TestS5_T5_6_EarlyReturnOnForeignObject(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountWebhooks(t, e, pub, s5_appSecret, s5_verifyToken)

	body := s5_waBody("page", "WABA1", "PNID1", "wamid.x", "+233500000003", nil)
	rec := s5_postWhatsApp(t, h, body, s5_hubSign(s5_appSecret, body))
	if rec.Code != http.StatusOK || !bytes.Equal(bytes.TrimSpace(rec.Body.Bytes()), []byte(`{"status":"ok"}`)) {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
	if pub.len() != 0 || s5_webhookEventCount(t, e) != 0 {
		t.Fatal("early return must not enqueue or persist")
	}
}

// T5.5 + T5.7 happy path: walk, enrich, persist idempotency row, publish.
func TestS5_T5_5_T5_7_HappyPathIngestAndEnqueue(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountWebhooks(t, e, pub, s5_appSecret, s5_verifyToken)

	body := s5_waBody("whatsapp_business_account", "WABA-9", "PNID-7", "wamid.happy1", "+233500000004", nil)
	rec := s5_postWhatsApp(t, h, body, s5_hubSign(s5_appSecret, body))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type=%q", ct)
	}

	if pub.len() != 1 {
		t.Fatalf("publisher calls=%d, want 1", pub.len())
	}
	call := pub.calls[0]
	if call.Queue != queue.QWebhookProcessing || call.TaskType != queue.TaskWebhookProcess {
		t.Fatalf("queue/task = %s/%s", call.Queue, call.TaskType)
	}
	wantKey := "WABA-9:+233500000004:wamid.happy1"
	if call.Opts == nil || call.Opts.TaskID != wantKey || call.Opts.MaxRetry != 2 {
		t.Fatalf("opts=%+v, want TaskID %s MaxRetry 2", call.Opts, wantKey)
	}

	var job struct {
		IdempotencyKey string         `json:"idempotencyKey"`
		Payload        map[string]any `json:"payload"`
		ReceivedAt     string         `json:"receivedAt"`
	}
	if err := json.Unmarshal(call.Payload, &job); err != nil {
		t.Fatalf("decode job payload: %v (%s)", err, call.Payload)
	}
	if job.IdempotencyKey != wantKey {
		t.Fatalf("job.idempotencyKey=%q want %q", job.IdempotencyKey, wantKey)
	}
	if _, err := time.Parse(time.RFC3339, strings.Replace(job.ReceivedAt, " ", "T", 1)); err == nil && !strings.HasSuffix(job.ReceivedAt, "Z") {
		t.Logf("note: receivedAt format %q", job.ReceivedAt)
	} else if err != nil {
		t.Fatalf("receivedAt %q not ISO-like", job.ReceivedAt)
	}
	// Enriched message payload: original fields + recipientPhone.
	if job.Payload["recipientPhone"] != "PNID-7" {
		t.Errorf("payload.recipientPhone=%v want PNID-7", job.Payload["recipientPhone"])
	}
	if job.Payload["from"] != "+233500000004" || job.Payload["id"] != "wamid.happy1" {
		t.Errorf("payload message passthrough broken: %v", job.Payload)
	}
	text, _ := job.Payload["text"].(map[string]any)
	if text == nil || text["body"] != "hello" {
		t.Errorf("payload.text lost: %v", job.Payload["text"])
	}

	if n := s5_webhookEventCount(t, e); n != 1 {
		t.Fatalf("webhook_events rows=%d want 1", n)
	}
	var storedKey string
	if err := e.DB.QueryRow(`SELECT idempotency_key FROM webhook_events`).Scan(&storedKey); err != nil || storedKey != wantKey {
		t.Fatalf("row key=%q err=%v want %q", storedKey, err, wantKey)
	}
}

// T5.7 idempotency: SAME payload twice -> ONE row, publisher called once,
// both responses ok.
func TestS5_T5_7_IdempotentReplaySingleRowSingleJob(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountWebhooks(t, e, pub, s5_appSecret, s5_verifyToken)

	body := s5_waBody("whatsapp_business_account", "WABA-R", "PNID-R", "wamid.replay", "+233500000005", nil)
	sig := s5_hubSign(s5_appSecret, body)

	for i := 1; i <= 2; i++ {
		rec := s5_postWhatsApp(t, h, body, sig)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
			t.Fatalf("delivery %d: status=%d body=%s", i, rec.Code, rec.Body.String())
		}
	}
	if n := s5_webhookEventCount(t, e); n != 1 {
		t.Fatalf("webhook_events rows=%d want exactly 1", n)
	}
	if n := pub.len(); n != 1 {
		t.Fatalf("publisher calls=%d want exactly 1 (duplicate skipped)", n)
	}
}

// T5.9 referral capture logging.
func TestS5_T5_9_ReferralCaptureLog(t *testing.T) {
	e := ep_startTest(t)
	var logBuf lockedBytes
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	defer slog.SetDefault(prev)

	pub := &s5_fakePublisher{}
	h := s5_mountWebhooks(t, e, pub, s5_appSecret, s5_verifyToken)

	body := s5_waBody("whatsapp_business_account", "WABA-C", "PNID-C", "wamid.ad1", "+233500000006",
		map[string]any{"referral": map[string]any{
			"source_type": "ad",
			"source_id":   "ad-123",
			"source_url":  "https://fb.me/abc",
			"headline":    "Buy Milo cheap",
		}})
	rec := s5_postWhatsApp(t, h, body, s5_hubSign(s5_appSecret, body))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}

	logs := logBuf.String()
	for _, want := range []string{`"event":"referral_captured"`, `"senderPhone":"+233500000006"`,
		`"sourceType":"ad"`, `"sourceId":"ad-123"`, `"sourceUrl":"https://fb.me/abc"`, `"headline":"Buy Milo cheap"`} {
		if !strings.Contains(logs, want) {
			t.Errorf("referral log missing %s\ngot: %s", want, logs)
		}
	}
}

// Multi-entry / multi-message walks fan out to one job per message.
func TestS5_MultiMessageFanOut(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountWebhooks(t, e, pub, s5_appSecret, s5_verifyToken)

	raw := map[string]any{
		"object": "whatsapp_business_account",
		"entry": []any{
			map[string]any{
				"id": "WABA-A",
				"changes": []any{map[string]any{"value": map[string]any{
					"metadata": map[string]any{"phone_number_id": "PNID-A"},
					"messages": []any{
						map[string]any{"id": "m1", "from": "+233600000001", "type": "text", "text": map[string]any{"body": "one"}},
						map[string]any{"id": "m2", "from": "+233600000002", "type": "text", "text": map[string]any{"body": "two"}},
					},
				}}},
			},
			map[string]any{
				"id": "WABA-B",
				"changes": []any{map[string]any{"value": map[string]any{
					"metadata": map[string]any{"phone_number_id": "PNID-B"},
					"messages": []any{
						map[string]any{"id": "m3", "from": "+233600000003", "type": "text", "text": map[string]any{"body": "three"}},
					},
				}}},
			},
		},
	}
	body, _ := json.Marshal(raw)
	rec := s5_postWhatsApp(t, h, body, s5_hubSign(s5_appSecret, body))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}

	wantKeys := map[string]bool{
		"WABA-A:+233600000001:m1": false,
		"WABA-A:+233600000002:m2": false,
		"WABA-B:+233600000003:m3": false,
	}
	if pub.len() != len(wantKeys) {
		t.Fatalf("publisher calls=%d want %d", pub.len(), len(wantKeys))
	}
	for _, c := range pub.calls {
		key := c.Opts.TaskID
		seen, ok := wantKeys[key]
		if !ok {
			t.Fatalf("unexpected TaskID %q", key)
		}
		if seen {
			t.Fatalf("duplicate TaskID %q", key)
		}
		wantKeys[key] = true

		var job struct {
			Payload struct {
				RecipientPhone string `json:"recipientPhone"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(c.Payload, &job); err != nil {
			t.Fatal(err)
		}
		wantPhone := "PNID-A"
		if strings.HasPrefix(key, "WABA-B:") {
			wantPhone = "PNID-B"
		}
		if job.Payload.RecipientPhone != wantPhone {
			t.Errorf("%s recipientPhone=%s want %s", key, job.Payload.RecipientPhone, wantPhone)
		}
	}
	if n := s5_webhookEventCount(t, e); n != len(wantKeys) {
		t.Errorf("rows=%d want %d", n, len(wantKeys))
	}
}

// Enqueue failure rolls the idempotency row back and STILL answers ok.
func TestS5_EnqueueFailureRollbackStillOk(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{err: errors.New("redis connection refused")}
	h := s5_mountWebhooks(t, e, pub, s5_appSecret, s5_verifyToken)

	body := s5_waBody("whatsapp_business_account", "WABA-F", "PNID-F", "wamid.fail", "+233500000007", nil)
	rec := s5_postWhatsApp(t, h, body, s5_hubSign(s5_appSecret, body))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
		t.Fatalf("enqueue failure must stay transparent: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if n := s5_webhookEventCount(t, e); n != 0 {
		t.Fatalf("failed publish must roll the idempotency row back, found %d", n)
	}
}

// ---- small local helpers ------------------------------------------------------

type lockedBytes struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBytes) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBytes) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
