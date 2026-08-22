package paystack

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// stubServer records request hits and answers a canned envelope per path.
type stubServer struct {
	t *testing.T
	s *httptest.Server

	recipientHits atomic.Int64
	transferHits  atomic.Int64

	lastRecipientBody map[string]any
	lastTransferBody  map[string]any
}

func newStub(t *testing.T, transferStatus string) *stubServer {
	t.Helper()
	st := &stubServer{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /transferrecipient", func(w http.ResponseWriter, r *http.Request) {
		st.recipientHits.Add(1)
		st.lastRecipientBody = decodeBody(t, r)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  true,
			"message": "Transfer recipient created successfully",
			"data":    map[string]any{"recipient_code": "RCP_test_001", "type": "momo"},
		})
	})
	mux.HandleFunc("POST /transfer", func(w http.ResponseWriter, r *http.Request) {
		st.transferHits.Add(1)
		st.lastTransferBody = decodeBody(t, r)
		if transferStatus == "__error__" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": false, "message": "Insufficient balance", "data": nil,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": true, "message": "Transfer successful",
			"data": map[string]any{
				"status": transferStatus, "transfer_code": "TRF_test_001",
				"reference": st.lastTransferBody["reference"],
			},
		})
	})
	st.s = httptest.NewServer(mux)
	t.Cleanup(st.s.Close)
	return st
}

func decodeBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read stub body: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("stub body not JSON: %v (%s)", err, raw)
	}
	return m
}

func TestCreateTransferRecipient(t *testing.T) {
	st := newStub(t, "success")
	c := New(Config{SecretKey: "sk_test_x", BaseURL: st.s.URL})

	code, err := c.CreateTransferRecipient(context.Background(), CreateRecipientRequest{
		Name: "Jane Doe", AccountNumber: "0244000011", BankCode: "MTN", Currency: "GHS",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != "RCP_test_001" {
		t.Errorf("recipient code = %q", code)
	}

	body := st.lastRecipientBody
	if body["type"] != "momo" { // provider hardcodes momo (paystack.provider.ts:220)
		t.Errorf("type = %v, want momo", body["type"])
	}
	for k, want := range map[string]any{
		"name": "Jane Doe", "account_number": "0244000011", "bank_code": "MTN", "currency": "GHS",
	} {
		if body[k] != want {
			t.Errorf("%s = %v, want %v", k, body[k], want)
		}
	}
}

func TestCreateTransferRecipientAuthHeaderAndDefaultBaseURL(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"status":true,"message":"ok","data":{"recipient_code":"RCP_2"}}`)
	}))
	defer srv.Close()

	c := New(Config{SecretKey: "sk_live_secret", BaseURL: srv.URL})
	code, err := c.CreateTransferRecipient(context.Background(), CreateRecipientRequest{Name: "n"})
	if err != nil || code != "RCP_2" {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if gotAuth != "Bearer sk_live_secret" {
		t.Errorf("Authorization = %q", gotAuth)
	}
}

func TestInitiateTransferMinorUnitsAndEnvelope(t *testing.T) {
	st := newStub(t, "success")
	c := New(Config{SecretKey: "sk_test_x", BaseURL: st.s.URL})

	res, err := c.InitiateTransfer(context.Background(), TransferRequest{
		AmountMinor: 8000, // 80.00 GHS in pesewas — converted by the caller
		Recipient:   "RCP_test_001",
		Reason:      "Vendor Payout",
		Currency:    "GHS",
		Reference:   "payout_abc",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != "success" || res.TransferCode != "TRF_test_001" {
		t.Errorf("result = %+v", res)
	}

	body := st.lastTransferBody
	if body["source"] != "balance" {
		t.Errorf("source = %v, want balance", body["source"])
	}
	if n, ok := body["amount"].(float64); !ok || n != 8000 {
		t.Errorf("amount = %v, want numeric 8000 (minor units)", body["amount"])
	}
	if body["recipient"] != "RCP_test_001" || body["reason"] != "Vendor Payout" || body["reference"] != "payout_abc" {
		t.Errorf("body = %v", body)
	}
	if _, sent := body["currency"]; sent {
		t.Errorf("currency must NOT be serialised into /transfer (source never sends it): %v", body)
	}
}

func TestEnvelopeErrorPropagation(t *testing.T) {
	st := newStub(t, "__error__")
	c := New(Config{SecretKey: "sk_test_x", BaseURL: st.s.URL})

	if _, err := c.InitiateTransfer(context.Background(), TransferRequest{AmountMinor: 1}); err == nil {
		t.Fatal("expected error for status:false envelope")
	} else {
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("error type = %T, want *APIError", err)
		}
		if apiErr.HTTPStatus != http.StatusBadRequest || apiErr.Message != "Insufficient balance" || apiErr.Endpoint != "/transfer" {
			t.Errorf("APIError = %+v", apiErr)
		}
	}

	// HTTP 200 with envelope status false is ALSO a failure (provider gate).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"status":false,"message":"Bad credentials","data":null}`)
	}))
	defer srv.Close()
	c2 := New(Config{SecretKey: "sk", BaseURL: srv.URL})
	_, err := c2.CreateTransferRecipient(context.Background(), CreateRecipientRequest{Name: "n"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Message != "Bad credentials" {
		t.Errorf("err = %v, want APIError Bad credentials", err)
	}

	// Malformed JSON.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "<html>boom</html>")
	}))
	defer srv2.Close()
	c3 := New(Config{SecretKey: "sk", BaseURL: srv2.URL})
	_, err = c3.InitiateTransfer(context.Background(), TransferRequest{})
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusBadGateway {
		t.Errorf("err = %v, want APIError with http=502", err)
	}
}

func TestMissingSecretKeyFailsFast(t *testing.T) {
	c := New(Config{})
	if _, err := c.CreateTransferRecipient(context.Background(), CreateRecipientRequest{}); !errors.Is(err, ErrMissingSecretKey) {
		t.Errorf("err = %v, want ErrMissingSecretKey", err)
	}
}
