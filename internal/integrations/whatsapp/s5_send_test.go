package whatsapp_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
)

// s5_stubMeta records one POST /messages request and answers with a canned
// Meta success envelope.
type s5_stubMeta struct {
	t *testing.T
	s *httptest.Server

	mu       sync.Mutex
	path     string
	auth     string
	body     map[string]any
	rawBody  string
	status   int // canned HTTP status; 0 -> 200
	response string
}

func newS5StubMeta(t *testing.T) *s5_stubMeta {
	st := &s5_stubMeta{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/messages") {
			http.NotFound(w, r)
			return
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		st.path = r.URL.Path
		st.auth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		st.rawBody = string(raw)
		_ = json.Unmarshal(raw, &st.body)
		if st.status != 0 {
			w.WriteHeader(st.status)
		}
		resp := st.response
		if resp == "" {
			resp = `{"messaging_product":"whatsapp","contacts":[{"input":"+233200000000","wa_id":"233200000000"}],"messages":[{"id":"wamid.s5stub1"}]}`
		}
		_, _ = w.Write([]byte(resp))
	})
	st.s = httptest.NewServer(mux)
	t.Cleanup(st.s.Close)
	return st
}

// T5.19b sendTemplateMessage payload shape + en_US default parameter.
func TestS5_SendTemplateMessage_PayloadShape(t *testing.T) {
	for _, tc := range []struct {
		name, langIn, wantLang string
	}{
		{"explicit language", "en_GB", "en_GB"},
		{"default applied", "", "en_US"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := newS5StubMeta(t)
			c := &whatsapp.Client{BaseURL: stub.s.URL, Version: "v25.0", Token: "tok-s5"}

			if _, err := c.SendTemplateMessage(context.Background(), "WNI-S5", "+233201234567", "order_shipped", tc.langIn); err != nil {
				t.Fatalf("SendTemplateMessage: %v", err)
			}

			if stub.path != "/v25.0/WNI-S5/messages" {
				t.Fatalf("path = %q", stub.path)
			}
			if stub.auth != "Bearer tok-s5" {
				t.Fatalf("auth = %q", stub.auth)
			}
			want := map[string]any{
				"messaging_product": "whatsapp",
				"to":                "+233201234567",
				"type":              "template",
				"template": map[string]any{
					"name": "order_shipped",
					"language": map[string]any{
						"code": tc.wantLang,
					},
				},
			}
			got, _ := json.Marshal(stub.body)
			wantJSON, _ := json.Marshal(want)
			if string(got) != string(wantJSON) {
				t.Fatalf("payload:\n got %s\nwant %s", got, wantJSON)
			}
		})
	}
}

// T5.19c sendImageMessage(link[, caption]) payload shape; the caption key is
// OMITTED when empty (`if (caption)` guard).
func TestS5_SendImageMessage_PayloadShape(t *testing.T) {
	t.Run("with caption", func(t *testing.T) {
		stub := newS5StubMeta(t)
		c := &whatsapp.Client{BaseURL: stub.s.URL, Version: "v25.0", Token: "t"}
		if _, err := c.SendImageMessage(context.Background(), "WNI", "+233201234567",
			"https://img.test/product.jpg", "Milo - GHS 25.50"); err != nil {
			t.Fatal(err)
		}
		img, _ := stub.body["image"].(map[string]any)
		if img == nil || img["link"] != "https://img.test/product.jpg" || img["caption"] != "Milo - GHS 25.50" {
			t.Fatalf("image block = %v", stub.body["image"])
		}
	})

	t.Run("caption omitted when empty", func(t *testing.T) {
		stub := newS5StubMeta(t)
		c := &whatsapp.Client{BaseURL: stub.s.URL, Version: "v25.0", Token: "t"}
		if _, err := c.SendImageMessage(context.Background(), "WNI", "+233201234567",
			"https://img.test/x.jpg", ""); err != nil {
			t.Fatal(err)
		}
		img, _ := stub.body["image"].(map[string]any)
		if img == nil || img["link"] != "https://img.test/x.jpg" {
			t.Fatalf("image block = %v", img)
		}
		if _, present := img["caption"]; present {
			t.Fatalf("caption must be absent, got %q", img["caption"])
		}
	})
}

// T5.19d all sends share one HTTP path: template/image failures carry the
// same "Meta API error:" wrapping as text sends.
func TestS5_SharedSendMessage_ErrorParity(t *testing.T) {
	stub := newS5StubMeta(t)
	stub.mu.Lock()
	stub.status = http.StatusBadRequest
	stub.response = `{"error":{"message":"(#132000) Param to is invalid","type":"OAuthException","code":132000}}`
	stub.mu.Unlock()

	c := &whatsapp.Client{BaseURL: stub.s.URL, Version: "v25.0", Token: "t"}
	_, errTmpl := c.SendTemplateMessage(context.Background(), "WNI", "bad", "tpl", "")
	_, errImg := c.SendImageMessage(context.Background(), "WNI", "bad", "https://x", "")
	for name, err := range map[string]error{"template": errTmpl, "image": errImg} {
		if err == nil || !strings.Contains(err.Error(), "Meta API error: {") ||
			!strings.Contains(err.Error(), "OAuthException") {
			t.Errorf("%s error = %v", name, err)
		}
	}
}

// T5.19e extractMessageId: metaResponse?.messages?.[0]?.id.
func TestS5_ExtractMessageID(t *testing.T) {
	if id := whatsapp.ExtractMessageId([]byte(`{"messaging_product":"whatsapp","messages":[{"id":"wamid.abc123"}]}`)); id != "wamid.abc123" {
		t.Fatalf("id = %q", id)
	}
	if id := whatsapp.ExtractMessageId([]byte(`{"messaging_product":"whatsapp"}`)); id != "" {
		t.Fatalf("missing messages -> %q, want empty", id)
	}
	if id := whatsapp.ExtractMessageId([]byte(`not-json`)); id != "" {
		t.Fatalf("invalid json -> %q, want empty", id)
	}

	// The parsed Meta response from a real send round-trips through here.
	stub := newS5StubMeta(t)
	c := &whatsapp.Client{BaseURL: stub.s.URL, Version: "v25.0", Token: "t"}
	data, err := c.SendTextMessageData(context.Background(), "WNI", "+233200000000", "hi")
	if err != nil {
		t.Fatal(err)
	}
	blob, _ := json.Marshal(data)
	if id := whatsapp.ExtractMessageId(blob); id != "wamid.s5stub1" {
		t.Fatalf("round-trip id = %q", id)
	}
}
