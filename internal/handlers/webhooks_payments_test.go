package handlers

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

const s5_paystackSecret = "sk_test_s5_ingest"

// s5_paystackSign computes the x-paystack-signature header for a body.
func s5_paystackSign(secret string, body []byte) string {
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func s5_chargeSuccessBody() []byte {
	return []byte(`{"event":"charge.success","data":{"reference":"ref-s5-pay-1","amount":5100,"currency":"GHS","status":"success","paid_at":"2026-08-22T09:30:00.000Z","customer":{"email":"buyer@example.com"},"authorization":{"bank":"MTN"},"metadata":{"businessId":"biz-s5-pay"}}}`)
}

func s5_postPayments(t *testing.T, h http.Handler, provider string, body []byte, paystackSig, hubtelSig string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/payments/"+provider, bytes.NewReader(body))
	if paystackSig != "" {
		req.Header.Set("X-Paystack-Signature", paystackSig)
	}
	if hubtelSig != "" {
		req.Header.Set("X-Hubtel-Signature", hubtelSig)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func s5_mountPayments(t *testing.T, e *epEnv, pub queue.Publisher) http.Handler {
	t.Helper()
	r := chi.NewRouter()
	MountPaymentsIngest(r, PaymentsIngestDeps{Publisher: pub, PaystackSecret: s5_paystackSecret})
	return r
}

// T5.11+T5.12+T5.13+T5.17 happy path: valid signed charge.success enqueues
// the payment-events job whose rawPayload decodes to a fully-populated
// NormalisedPaymentEvent.
func TestS5_T5_11_PaystackChargeSuccessEnqueuesParseableEvent(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountPayments(t, e, pub)

	body := s5_chargeSuccessBody()
	rec := s5_postPayments(t, h, "paystack", body, s5_paystackSign(s5_paystackSecret, body), "")
	if rec.Code != http.StatusOK || !bytes.Equal(bytes.TrimSpace(rec.Body.Bytes()), []byte(`{"status":"ok"}`)) {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
	if pub.len() != 1 {
		t.Fatalf("publisher calls=%d want 1", pub.len())
	}

	call := pub.calls[0]
	if call.Queue != queue.QPaymentEvents || call.TaskType != queue.TaskPaymentProcess {
		t.Fatalf("queue/task = %s/%s", call.Queue, call.TaskType)
	}
	wantTaskID := "payment:paystack:ref-s5-pay-1" // jobId `payment:${providerName}:${externalReference}`
	if call.Opts == nil || call.Opts.TaskID != wantTaskID || call.Opts.MaxRetry != 4 {
		t.Fatalf("opts=%+v want TaskID %s MaxRetry 4", call.Opts, wantTaskID)
	}
	if call.Opts.UniqueTTL != 300*1e9 { // deduplication ttl 300_000 ms
		t.Errorf("UniqueTTL=%v want 5m", call.Opts.UniqueTTL)
	}

	var job struct {
		ProviderName string          `json:"providerName"`
		RawPayload   json.RawMessage `json:"rawPayload"`
		ReceivedAt   string          `json:"receivedAt"`
	}
	if err := json.Unmarshal(call.Payload, &job); err != nil {
		t.Fatalf("decode job: %v (%s)", err, call.Payload)
	}
	if job.ProviderName != "paystack" {
		t.Errorf("providerName=%q", job.ProviderName)
	}
	if job.ReceivedAt == "" || !strings.HasSuffix(job.ReceivedAt, "Z") {
		t.Errorf("receivedAt=%q", job.ReceivedAt)
	}
	if string(job.RawPayload) != string(body) {
		t.Error("rawPayload must carry the request bytes verbatim")
	}

	// The worker-side parse of the captured payload is fully populated.
	evt, err := paystack.ParseWebhookEvent(job.RawPayload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if evt.Event != "charge.success" || evt.Reference != "ref-s5-pay-1" ||
		evt.AmountMinor != 5100 || evt.Currency != "GHS" || evt.Status != "success" ||
		evt.BusinessID != "biz-s5-pay" || evt.CustomerEmail != "buyer@example.com" ||
		evt.AuthorizationBank != "MTN" || evt.PaidAt.IsZero() {
		t.Fatalf("normalised event incomplete: %+v", evt)
	}
}

// T5.11 rejection: bad signature -> exact source body.
func TestS5_T5_11_InvalidSignatureRejected(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountPayments(t, e, pub)

	body := s5_chargeSuccessBody()
	rec := s5_postPayments(t, h, "paystack", body, s5_paystackSign("wrong-secret", body), "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
	v := ep_decode(t, rec)
	if v["error"] != "Invalid signature" {
		t.Fatalf("body=%v — res.status(400).json({error:'Invalid signature'})", v)
	}
	if len(v) != 1 {
		t.Errorf("plain body expected, got envelope %v", v)
	}
	if pub.len() != 0 {
		t.Errorf("rejected event enqueued %d jobs", pub.len())
	}
}

// Unknown provider -> 400 {"error":"Unknown provider"} (SOURCE; the brief's
// "404 envelope" was wrong).
func TestS5_T5_11_UnknownProvider(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountPayments(t, e, pub)

	body := s5_chargeSuccessBody()
	rec := s5_postPayments(t, h, "momo_express", body, "", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
	if v := ep_decode(t, rec); v["error"] != "Unknown provider" || len(v) != 1 {
		t.Fatalf("body=%v", v)
	}
	if pub.len() != 0 {
		t.Errorf("unknown provider enqueued %d jobs", pub.len())
	}
}

// Signature header fallback chain: x-hubtel-signature used when the paystack
// header is absent.
func TestS5_HubtelHeaderFallback(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountPayments(t, e, pub)

	body := s5_chargeSuccessBody()
	rec := s5_postPayments(t, h, "paystack", body, "", s5_paystackSign(s5_paystackSecret, body))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if pub.len() != 1 {
		t.Fatalf("publisher calls=%d", pub.len())
	}
}

// Replay of the SAME captured payload: both deliveries answer ok and the
// handler always attempts the enqueue with the SAME fixed task id (the queue
// layer owns deduplication, exactly like Node's add() + deduplication.id).
func TestS5_PaymentReplaySameTaskID(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountPayments(t, e, pub)

	body := s5_chargeSuccessBody()
	sig := s5_paystackSign(s5_paystackSecret, body)
	for i := 1; i <= 2; i++ {
		rec := s5_postPayments(t, h, "paystack", body, sig, "")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
			t.Fatalf("delivery %d: status=%d body=%s", i, rec.Code, rec.Body.String())
		}
	}
	if pub.len() != 2 {
		t.Fatalf("handler must attempt both deliveries like Node add(), got %d", pub.len())
	}
	if pub.calls[0].Opts.TaskID != pub.calls[1].Opts.TaskID {
		t.Fatal("replays must target the same fixed id")
	}
}

// Missing-data payloads still enqueue (Node parses in the WORKER, not the
// controller) — the retry/permanent-fail machinery owns bad payloads.
func TestS5_MissingDataStillEnqueued(t *testing.T) {
	e := ep_startTest(t)
	pub := &s5_fakePublisher{}
	h := s5_mountPayments(t, e, pub)

	body := []byte(`{"event":"charge.success"}`)
	rec := s5_postPayments(t, h, "paystack", body, s5_paystackSign(s5_paystackSecret, body), "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if pub.len() != 1 {
		t.Fatalf("publisher calls=%d", pub.len())
	}
	// Fallback reference when data.reference absent.
	if id := pub.calls[0].Opts.TaskID; !strings.HasPrefix(id, "payment:paystack:paystack:") {
		t.Errorf("fallback TaskID=%q want payment:paystack:<providerName>:<ts>", id)
	}
}
