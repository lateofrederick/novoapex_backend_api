package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

// conversationsWriteTree builds the write subtree the way central integration
// does (MountConversationsWrite registered on the shared /conversations
// prefix router).
func conversationsWriteTree(e *epEnv, wa *s4o_waSpy) http.Handler {
	r := chi.NewRouter()
	deps := ConversationsWriteDeps{Pool: e.Pool}
	if wa != nil {
		deps.WA = wa
	}
	MountConversationsWrite(r, deps)
	return r
}

// s4o_waSpy records WhatsApp sends in order; fail makes every send fail the
// way the Meta client does.
type s4o_waSpy struct {
	mu    sync.Mutex
	sends []string // "<phoneNumberId>|<to>|<text>"
	fail  error
}

func (s *s4o_waSpy) SendTextMessageData(_ context.Context, pnid, to, text string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return nil, s.fail
	}
	s.sends = append(s.sends, pnid+"|"+to+"|"+text)
	return map[string]any{
		"messaging_product": "whatsapp",
		"messages":          []any{map[string]any{"id": fmt.Sprintf("wamid.s4o.%d", len(s.sends))}},
	}, nil
}

func (s *s4o_waSpy) SendImageMessage(context.Context, string, string, string, string) (map[string]any, error) {
	return nil, errors.New("unexpected image send")
}

func (s *s4o_waSpy) SendTemplateMessage(context.Context, string, string, string, string) (map[string]any, error) {
	return nil, errors.New("unexpected template send")
}

// T4.18/T4.19 POST /conversations/:id/takeover and /release — flag flips,
// state transitions, scoping 404s.
func TestS4O_T4_18_19_TakeoverRelease(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	other := e.F.Business()

	h := ep_mount(t, "/conversations", conversationsWriteTree(e, nil))
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	crossConv := e.F.Conversation(other.ID, "+233820000001", harness.WithState("ESCALATED"))

	type convState struct {
		Escalated bool
		State     string
	}
	fetch := func(id string) convState {
		t.Helper()
		var cs convState
		if err := e.DB.QueryRow(
			`SELECT is_escalated_to_human, state::text FROM conversations WHERE id = $1`, id).
			Scan(&cs.Escalated, &cs.State); err != nil {
			t.Fatalf("scan %s: %v", id, err)
		}
		return cs
	}

	t.Run("takeover escalates an idle LEAD conversation (201)", func(t *testing.T) {
		conv := e.F.Conversation(biz.ID, "+233821000001")
		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/takeover", "", &claims)
		if rec.Code != http.StatusCreated { // Nest @Post default 201
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if ks := fmt.Sprint(ep_keys(t, body)); ks != "[success]" || body["success"] != true {
			t.Errorf("body = %v (keys %s)", body, ks)
		}
		if got := fetch(conv.ID); !got.Escalated || got.State != "ESCALATED" {
			t.Errorf("after takeover = %+v", got)
		}
	})

	t.Run("release returns to LEAD with flag cleared (201)", func(t *testing.T) {
		conv := e.F.Conversation(biz.ID, "+233821000002")
		if _, err := e.DB.Exec(`UPDATE conversations SET is_escalated_to_human = true, state = 'SUPPORT' WHERE id = $1`, conv.ID); err != nil {
			t.Fatalf("pre-escalate: %v", err)
		}

		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/release", "", &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if got := fetch(conv.ID); got.Escalated || got.State != "LEAD" {
			t.Errorf("after release = %+v", got)
		}
	})

	t.Run("cross-tenant conversation -> NotFound 'Conversation not found'", func(t *testing.T) {
		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+crossConv.ID+"/takeover", "", &claims)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("takeover status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if body["statusCode"] != json_Number("404") || body["message"] != "Conversation not found" {
			t.Errorf("body = %v", body)
		}

		rec = s4o_do(t, h, http.MethodPost, "/conversations/"+crossConv.ID+"/release", "", &claims)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("release status=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("unknown conversation -> 404", func(t *testing.T) {
		rec := s4o_do(t, h, http.MethodPost, "/conversations/s4o-none/takeover", "", &claims)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

// T4.20 POST /conversations/:id/reply — ConversationsService.reply: send via
// WhatsApp synchronously, persist the sent OutboundMessage, 500 on Meta error.
func TestS4O_T4_20_ReplySendsSynchronously(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	now := time.Now().UTC()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}

	t.Run("sent via Meta; 201 with the persisted sent record", func(t *testing.T) {
		wa := &s4o_waSpy{}
		h := ep_mount(t, "/conversations", conversationsWriteTree(e, wa))
		conv := e.F.Conversation(biz.ID, "+233822000001")
		ep_seedInboundMessage(t, e, "s4o-in-1", conv.ID, biz.ID, "+233822000001", "hello", now.Add(-time.Hour))

		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/reply",
			`{"text":"We have it in stock!"}`, &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}

		if want := biz.WhatsAppPhoneNumberID + "|+233822000001|We have it in stock!"; len(wa.sends) != 1 || wa.sends[0] != want {
			t.Fatalf("sends = %v, want [%s]", wa.sends, want)
		}

		body := ep_decode(t, rec)
		wantKeys := "[businessId conversationId createdAt id imageUrl messageType metaResponse productId rawPayload recipientPhone status templateName textContent whatsappMessageId]"
		if ks := fmt.Sprint(ep_keys(t, body)); ks != wantKeys {
			t.Fatalf("key set =\n%s\nwant   \n%s", ks, wantKeys)
		}
		if body["status"] != "sent" || body["messageType"] != "text" || body["textContent"] != "We have it in stock!" ||
			body["recipientPhone"] != "+233822000001" || body["whatsappMessageId"] != "wamid.s4o.1" {
			t.Errorf("row fields = %v", body)
		}
		rawPayload, _ := body["rawPayload"].(map[string]any)
		if len(rawPayload) != 2 || rawPayload["type"] != "text" || rawPayload["text"] != "We have it in stock!" {
			t.Errorf("rawPayload = %v, want {type:'text', text}", rawPayload)
		}
		meta, _ := body["metaResponse"].(map[string]any)
		if meta["messaging_product"] != "whatsapp" {
			t.Errorf("metaResponse = %v, want the Meta API response", body["metaResponse"])
		}

		var status string
		if err := e.DB.QueryRow(`SELECT status FROM outbound_messages WHERE id = $1`, body["id"]).Scan(&status); err != nil || status != "sent" {
			t.Errorf("db row status = %q err=%v", status, err)
		}
	})

	t.Run("Meta rejection -> 500 envelope and no record left behind", func(t *testing.T) {
		wa := &s4o_waSpy{fail: errors.New(`Meta API error: {"error":{"code":131047}}`)}
		h := ep_mount(t, "/conversations", conversationsWriteTree(e, wa))
		conv := e.F.Conversation(biz.ID, "+233822000002")
		ep_seedInboundMessage(t, e, "s4o-in-2", conv.ID, biz.ID, "+233822000002", "old msg", now.Add(-25*time.Hour))

		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/reply", `{"text":"too late"}`, &claims)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if body := ep_decode(t, rec); body["message"] != "Internal server error" {
			t.Errorf("body = %v", body)
		}
		var n int
		_ = e.DB.QueryRow(`SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, conv.ID).Scan(&n)
		if n != 0 {
			t.Errorf("an undelivered reply must not be persisted, found %d rows", n)
		}
	})

	t.Run("a reply never overtakes an assistant message still queued", func(t *testing.T) {
		wa := &s4o_waSpy{}
		h := ep_mount(t, "/conversations", conversationsWriteTree(e, wa))
		conv := e.F.Conversation(biz.ID, "+233822000003")
		ep_seedInboundMessage(t, e, "s4o-in-3", conv.ID, biz.ID, "+233822000003", "price?", now.Add(-time.Minute))
		if _, err := e.DB.Exec(`
			INSERT INTO outbound_messages (id, business_id, conversation_id, recipient_phone, message_type, text_content, raw_payload, status)
			VALUES ('s4o-queued', $1, $2, '+233822000003', 'text', 'assistant answer', '{}', 'pending')`, biz.ID, conv.ID); err != nil {
			t.Fatal(err)
		}

		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/reply", `{"text":"owner follow-up"}`, &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		want := []string{
			biz.WhatsAppPhoneNumberID + "|+233822000003|assistant answer",
			biz.WhatsAppPhoneNumberID + "|+233822000003|owner follow-up",
		}
		if fmt.Sprint(wa.sends) != fmt.Sprint(want) {
			t.Errorf("send order = %v, want %v", wa.sends, want)
		}
	})

	t.Run("cross-tenant conversation -> 404, nothing sent or persisted", func(t *testing.T) {
		wa := &s4o_waSpy{}
		h := ep_mount(t, "/conversations", conversationsWriteTree(e, wa))
		other := e.F.Business()
		oConv := e.F.Conversation(other.ID, "+233822000009")
		ep_seedInboundMessage(t, e, "s4o-in-x", oConv.ID, other.ID, "+233822000009", "hi", now)

		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+oConv.ID+"/reply", `{"text":"steal?"}`, &claims)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var n int
		_ = e.DB.QueryRow(`SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, oConv.ID).Scan(&n)
		if n != 0 || len(wa.sends) != 0 {
			t.Errorf("cross-tenant reply: rows=%d sends=%v", n, wa.sends)
		}
	})

	t.Run("ReplyDto: missing text -> invalid_type Required; empty -> too_small", func(t *testing.T) {
		wa := &s4o_waSpy{}
		h := ep_mount(t, "/conversations", conversationsWriteTree(e, wa))
		conv := e.F.Conversation(biz.ID, "+233822000005")
		ep_seedInboundMessage(t, e, "s4o-in-5", conv.ID, biz.ID, "+233822000005", "hi", now)

		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/reply", `{}`, &claims)
		s4o_assertZodIssue(t, rec, "invalid_type", "text")

		rec = s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/reply", `{"text":""}`, &claims)
		s4o_assertZodIssue(t, rec, "too_small", "text")

		var n int
		_ = e.DB.QueryRow(`SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, conv.ID).Scan(&n)
		if n != 0 || len(wa.sends) != 0 {
			t.Errorf("validation failures must not send or persist: rows=%d sends=%v", n, wa.sends)
		}
	})
}
