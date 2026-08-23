package paystack

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

const s5_secret = "sk_test_s5_secret"

// s5_paystackSign computes the x-paystack-signature value for body.
func s5_paystackSign(secret string, body []byte) string {
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// ---- VerifyWebhookSignature (T5.12) ----------------------------------------

func TestS5_VerifyWebhookSignature(t *testing.T) {
	body := []byte(`{"event":"charge.success"}`)
	valid := s5_paystackSign(s5_secret, body)

	if !VerifyWebhookSignature(s5_secret, body, valid) {
		t.Fatal("valid signature rejected")
	}

	cases := []struct {
		name      string
		secret    string
		body      []byte
		signature string
	}{
		{"tampered body", s5_secret, []byte(`{"event":"charge.failed"}`), valid},
		{"wrong secret", "sk_other", body, valid},
		{"truncated signature", s5_secret, body, valid[:len(valid)-2]},
		{"empty signature", s5_secret, body, ""},
		{"empty secret", "", body, valid},
		{"uppercase hex rejected", s5_secret, body, strings.ToUpper(valid)},
	}
	for _, tc := range cases {
		if VerifyWebhookSignature(tc.secret, tc.body, tc.signature) {
			t.Errorf("%s: must be rejected", tc.name)
		}
	}
}

// ---- InitiatePayment (T5.15) ------------------------------------------------

func TestS5_InitiatePayment_RequestShapeAndResult(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		_, _ = w.Write([]byte(`{"status":true,"message":"Authorization URL created","data":{"authorization_url":"https://checkout.paystack.test/pay/abc","access_code":"ACCESS_x","reference":"ps-ref-7"}}`))
	}))
	defer srv.Close()

	c := New(Config{SecretKey: s5_secret, BaseURL: srv.URL})
	res, err := c.InitiatePayment(context.Background(), InitiatePaymentRequest{
		AmountMajor:   decimal.RequireFromString("51"),
		Currency:      "GHS",
		CustomerPhone: "+233201234567",
		Reference:     "order-1",
		BusinessID:    "biz-1",
		CallbackURL:   "https://app.test/callback",
	})
	if err != nil {
		t.Fatalf("InitiatePayment: %v", err)
	}

	if gotPath != "/transaction/initialize" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer "+s5_secret {
		t.Fatalf("auth = %q", gotAuth)
	}

	// Math.round(major*100): 51.00 GHS -> 5100 pesewas.
	if n, ok := gotBody["amount"].(float64); !ok || n != 5100 {
		t.Fatalf("amount = %v (%T), want numeric 5100 minor units", gotBody["amount"], gotBody["amount"])
	}
	if gotBody["currency"] != "GHS" || gotBody["reference"] != "order-1" ||
		gotBody["callback_url"] != "https://app.test/callback" {
		t.Fatalf("scalar fields mismatch: %v", gotBody)
	}
	// Email fallback: digits of the phone + non-delivering subdomain.
	if gotBody["email"] != "233201234567@customers.novoapex.com" {
		t.Fatalf("email = %v", gotBody["email"])
	}
	meta, _ := gotBody["metadata"].(map[string]any)
	if meta == nil || meta["businessId"] != "biz-1" || meta["customer_phone"] != "+233201234567" {
		t.Fatalf("metadata = %v", meta)
	}

	if res.Status != "initiated" {
		t.Fatalf("status = %q, want initiated", res.Status)
	}
	if res.PaymentURL != "https://checkout.paystack.test/pay/abc" {
		t.Fatalf("paymentURL = %q", res.PaymentURL)
	}
	if res.ProviderReference != "ps-ref-7" {
		t.Fatalf("providerReference = %q", res.ProviderReference)
	}
}

func TestS5_InitiatePayment_FailureIsFailedStatusNotError(t *testing.T) {
	// !response.ok || !data.status -> {providerReference:'', status:'failed'}
	// returned as a RESULT, never an error (paystack.provider.ts:182-185).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"status":false,"message":"Amount exceeds balance"}`))
	}))
	defer srv.Close()

	c := New(Config{SecretKey: s5_secret, BaseURL: srv.URL})
	res, err := c.InitiatePayment(context.Background(), InitiatePaymentRequest{
		AmountMajor: decimal.RequireFromString("999999"),
		Currency:    "GHS",
	})
	if err != nil {
		t.Fatalf("failure surfaced as error: %v", err)
	}
	if res.Status != "failed" || res.PaymentURL != "" || res.ProviderReference != "" {
		t.Fatalf("result = %+v, want zeroed failed result", res)
	}
}

func TestS5_InitiatePayment_MissingSecretFailsClosedLikeSource(t *testing.T) {
	c := New(Config{})
	res, err := c.InitiatePayment(context.Background(), InitiatePaymentRequest{
		AmountMajor: decimal.RequireFromString("10"), Currency: "GHS",
	})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if res.Status != "failed" {
		t.Fatalf("status = %q", res.Status)
	}
}

func TestS5_InitiatePayment_ReferenceFallbackWhenDataOmitted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":true,"data":{"authorization_url":"https://checkout/pay/x"}}`))
	}))
	defer srv.Close()

	c := New(Config{SecretKey: s5_secret, BaseURL: srv.URL})
	res, err := c.InitiatePayment(context.Background(), InitiatePaymentRequest{
		AmountMajor: decimal.RequireFromString("10"), Reference: "my-order-ref",
	})
	if err != nil || res.ProviderReference != "my-order-ref" {
		t.Fatalf("res=%+v err=%v — data.reference ?? params.reference", res, err)
	}
}

// ---- ParseWebhookEvent (T5.13) ----------------------------------------------

const s5_chargeSuccess = `{"event":"charge.success","data":{"reference":"ref-s5-1","amount":5100,"currency":"ghs","status":"success","paid_at":"2026-08-22T09:30:00.000Z","customer":{"email":"buyer@example.com","phone":"+233201234567"},"authorization":{"bank":"MTN"},"metadata":{"businessId":"biz-s5-1"}}}`

func TestS5_ParseWebhookEvent_ChargeSuccessFullyPopulated(t *testing.T) {
	evt, err := ParseWebhookEvent([]byte(s5_chargeSuccess))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	wantPaidAt := time.Date(2026, 8, 22, 9, 30, 0, 0, time.UTC)
	if evt.Provider != "paystack" {
		t.Errorf("provider = %q", evt.Provider)
	}
	if evt.Event != "charge.success" {
		t.Errorf("event = %q", evt.Event)
	}
	if evt.Reference != "ref-s5-1" {
		t.Errorf("reference = %q", evt.Reference)
	}
	if evt.AmountMinor != 5100 {
		t.Errorf("amountMinor = %d, want provider-native 5100", evt.AmountMinor)
	}
	if evt.Currency != "GHS" {
		t.Errorf("currency = %q (upper-cased)", evt.Currency)
	}
	if evt.Status != "success" {
		t.Errorf("status = %q", evt.Status)
	}
	if !evt.PaidAt.Equal(wantPaidAt) {
		t.Errorf("paidAt = %v, want %v", evt.PaidAt, wantPaidAt)
	}
	if evt.BusinessID != "biz-s5-1" {
		t.Errorf("businessId = %q", evt.BusinessID)
	}
	if evt.CustomerEmail != "buyer@example.com" {
		t.Errorf("customerEmail = %q", evt.CustomerEmail)
	}
	if evt.AuthorizationBank != "MTN" {
		t.Errorf("authorizationBank = %q", evt.AuthorizationBank)
	}
	if string(evt.RawJSON) != s5_chargeSuccess {
		t.Error("RawJSON must be preserved verbatim")
	}
}

func TestS5_ParseWebhookEvent_StatusDerivedFromEventNameOnly(t *testing.T) {
	failed := `{"event":"charge.failed","data":{"reference":"r2","amount":100,"currency":"NGN","status":"abandoned"}}`
	evt, err := ParseWebhookEvent([]byte(failed))
	if err != nil {
		t.Fatal(err)
	}
	if evt.Status != "failed" {
		t.Errorf("charge.failed -> status %q, want failed", evt.Status)
	}

	other := `{"event":"transfer.success","data":{"reference":"r3","amount":1}}`
	if evt, err = ParseWebhookEvent([]byte(other)); err != nil || evt.Status != "pending" {
		t.Errorf("unknown event -> (%+v,%v), want pending status", evt.Status, err)
	}
}

func TestS5_ParseWebhookEvent_MissingDataThrowsSourceMessage(t *testing.T) {
	_, err := ParseWebhookEvent([]byte(`{"event":"charge.success"}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != `Paystack webhook payload missing "data" field` {
		t.Errorf("error = %q — must match paystack.provider.ts:79 verbatim", err)
	}
}

func TestS5_ParseWebhookEvent_DefaultsAndAbsents(t *testing.T) {
	evt, err := ParseWebhookEvent([]byte(`{"event":"charge.success","data":{"amount":51.99}}`))
	if err != nil {
		t.Fatal(err)
	}
	if evt.AmountMinor != 51 {
		t.Errorf("fractional amount -> %d minor (IntPart truncation)", evt.AmountMinor)
	}
	if evt.Currency != "GHS" {
		t.Errorf("defaulted currency = %q", evt.Currency)
	}
	if !evt.PaidAt.IsZero() {
		t.Errorf("absent paid_at must leave zero time, got %v", evt.PaidAt)
	}
	if evt.Reference != "" || evt.BusinessID != "" {
		t.Errorf("absent strings must be empty: %+v", evt)
	}

	// Unparseable paid_at behaves like JS Invalid Date -> zero time.
	bad := `{"event":"charge.success","data":{"paid_at":"not-a-date"}}`
	if evt, err = ParseWebhookEvent([]byte(bad)); err != nil || !evt.PaidAt.IsZero() {
		t.Errorf("bad paid_at -> (%v,%v)", evt.PaidAt, err)
	}

	// Malformed JSON surfaces a decode error, not a panic.
	if _, err = ParseWebhookEvent([]byte(`{nope`)); err == nil {
		t.Error("malformed JSON must error")
	}
}
