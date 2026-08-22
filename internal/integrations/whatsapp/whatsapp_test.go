package whatsapp_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
)

// SendText posts to ${base}/${version}/${phoneNumberID}/messages with a
// Bearer token and the exact payload shape whatsapp.service.ts builds.
func TestS3_SendTextMessage_RequestShapeAndSuccess(t *testing.T) {
	var gotPath, gotAuth, gotCT string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		_, _ = w.Write([]byte(`{"messaging_product":"whatsapp","contacts":[{"input":"+233200000000","wa_id":"233200000000"}],"messages":[{"id":"wamid.test"}]}`))
	}))
	defer srv.Close()

	c := &whatsapp.Client{BaseURL: srv.URL, Version: "v25.0", Token: "tok-1"}
	if err := c.SendTextMessage(t.Context(), "WNI123", "+233200000000", "Your NovoApex login code is: *123456*. It will expire in 5 minutes."); err != nil {
		t.Fatalf("SendTextMessage: %v", err)
	}

	if gotPath != "/v25.0/WNI123/messages" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer tok-1" {
		t.Fatalf("authorization = %q", gotAuth)
	}
	if !strings.HasPrefix(gotCT, "application/json") {
		t.Fatalf("content-type = %q", gotCT)
	}
	if gotBody["messaging_product"] != "whatsapp" || gotBody["to"] != "+233200000000" || gotBody["type"] != "text" {
		t.Fatalf("payload mismatch: %v", gotBody)
	}
	text, _ := gotBody["text"].(map[string]any)
	if text == nil || text["body"] != "Your NovoApex login code is: *123456*. It will expire in 5 minutes." {
		t.Fatalf("text payload mismatch: %v", gotBody["text"])
	}
}

// Non-2xx replies surface as "Meta API error: <body>" like whatsapp.service.ts.
func TestS3_SendTextMessage_MetaErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"(#131030) Recipient phone number not in allowed list","type":"OAuthException","code":131030,"error_data":{"messaging_product":"whatsapp","details":"..."}},"error_data":{"messaging_product":"whatsapp"},"error_user_title":"...","error_user_msg":"..."}`))
	}))
	defer srv.Close()

	c := &whatsapp.Client{BaseURL: srv.URL, Version: "v25.0", Token: "t"}
	err := c.SendTextMessage(t.Context(), "WNI", "+233200000001", "hi")
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "Meta API error: {") || !strings.Contains(msg, "OAuthException") {
		t.Fatalf("error should embed Meta envelope: %q", msg)
	}
}

func TestS3_New_DefaultBaseURLEnvOverride(t *testing.T) {
	if c := whatsapp.New("v25.0", "t"); c.BaseURL != "https://graph.facebook.com" {
		t.Fatalf("default base = %q", c.BaseURL)
	}
	t.Setenv("WHATSAPP_BASE_URL", "http://127.0.0.1:1")
	if c := whatsapp.New("v25.0", "t"); c.BaseURL != "http://127.0.0.1:1" {
		t.Fatalf("env override base = %q", c.BaseURL)
	}
}
