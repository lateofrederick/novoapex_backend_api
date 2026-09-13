package handlers

import (
	"errors"
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// MessagesService.persistOutboundMessage: every manual send attempt leaves an
// audit row linked to the vendor's business and conversation.
func TestS5_MessagesPersistEveryAttempt(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	cust := e.F.Customer(biz.ID)
	conv := e.F.Conversation(biz.ID, cust.Phone)
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	to := "233209876543"
	if _, err := e.DB.Exec(`UPDATE conversations SET customer_phone = $2 WHERE id = $1`, conv.ID, to); err != nil {
		t.Fatal(err)
	}

	mount := func(wa MessageSender) http.Handler {
		r := chi.NewRouter()
		MountMessages(r, MessagesDeps{WA: wa, PhoneNumberID: "wni_default", Pool: e.Pool})
		return ep_mount(t, "/messages", r)
	}
	row := func(msgType string) (status, waID, businessID, conversationID, raw, meta string) {
		t.Helper()
		var w, b, c, m *string
		if err := e.DB.QueryRow(`
			SELECT status, whatsapp_message_id, business_id, conversation_id, raw_payload::text, meta_response::text
			  FROM outbound_messages WHERE recipient_phone = $1 AND message_type = $2
			 ORDER BY created_at DESC LIMIT 1`, to, msgType).Scan(&status, &w, &b, &c, &raw, &m); err != nil {
			t.Fatalf("audit row for %s: %v", msgType, err)
		}
		deref := func(p *string) string {
			if p == nil {
				return ""
			}
			return *p
		}
		return status, deref(w), deref(b), deref(c), raw, deref(m)
	}

	t.Run("successful text send is recorded as sent", func(t *testing.T) {
		wa := &s5_stubWA{resp: map[string]any{"messages": []any{map[string]any{"id": "wamid.audit.1"}}}}
		rec := s4o_do(t, mount(wa), http.MethodPost, "/messages/send/text", `{"to":"`+to+`","text":"hello from the owner"}`, &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		status, waID, businessID, conversationID, raw, meta := row("text")
		if status != "sent" || waID != "wamid.audit.1" || businessID != biz.ID || conversationID != conv.ID {
			t.Errorf("row = status:%s waId:%s business:%s conversation:%s", status, waID, businessID, conversationID)
		}
		if raw != `{"to": "`+to+`", "text": {"body": "hello from the owner"}, "type": "text", "messaging_product": "whatsapp"}` {
			t.Errorf("raw_payload = %s", raw)
		}
		if meta == "" {
			t.Error("meta_response not recorded")
		}
	})

	t.Run("failed template send is recorded as failed with the error", func(t *testing.T) {
		wa := &s5_stubWA{err: errors.New(`Meta API error: {"error":{"message":"template not approved"}}`)}
		rec := s4o_do(t, mount(wa), http.MethodPost, "/messages/send/template", `{"to":"`+to+`","templateName":"promo"}`, &claims)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		status, waID, _, _, _, meta := row("template")
		if status != "failed" || waID != "" || meta == "" {
			t.Errorf("row = status:%s waId:%q meta:%s", status, waID, meta)
		}
	})
}
