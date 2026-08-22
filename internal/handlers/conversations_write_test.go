package handlers

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

// conversationsWriteTree builds the write subtree the way central integration
// will (MountConversationsWrite registered on the shared /conversations
// prefix router).
func conversationsWriteTree(e *epEnv, pub OutboundPublisher) http.Handler {
	r := chi.NewRouter()
	MountConversationsWrite(r, ConversationsWriteDeps{Pool: e.Pool, Publisher: pub})
	return r
}

// s4o_publisherSpy records PublishOutbound calls; every test that cares about
// enqueueing uses it instead of NullPublisher.
type s4o_publisherSpy struct {
	ids []string
	err error // when set, PublishOutbound fails
}

func (s *s4o_publisherSpy) PublishOutbound(_ context.Context, id string) error {
	if s.err != nil {
		return s.err
	}
	s.ids = append(s.ids, id)
	return nil
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

// T4.20 POST /conversations/:id/reply — the 24h window gate + persist-then-
// publish flow (conversation-orchestrator.service.ts:28-81 characterization).
func TestS4O_T4_20_ReplyWindowGate(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	now := time.Now().UTC()

	h := ep_mount(t, "/conversations", conversationsWriteTree(e, nil))
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}

	t.Run("INSIDE window -> pending row + publisher called", func(t *testing.T) {
		spy := &s4o_publisherSpy{}
		hh := ep_mount(t, "/conversations", conversationsWriteTree(e, spy))
		conv := e.F.Conversation(biz.ID, "+233822000001")
		ep_seedInboundMessage(t, e, "s4o-in-1", conv.ID, biz.ID, "+233822000001", "hello", now.Add(-time.Hour))

		rec := s4o_do(t, hh, http.MethodPost, "/conversations/"+conv.ID+"/reply",
			`{"text":"We have it in stock!"}`, &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}

		body := ep_decode(t, rec)
		wantKeys := "[businessId conversationId createdAt id imageUrl messageType metaResponse productId rawPayload recipientPhone status templateName textContent whatsappMessageId]"
		if ks := fmt.Sprint(ep_keys(t, body)); ks != wantKeys {
			t.Fatalf("key set =\n%s\nwant   \n%s", ks, wantKeys)
		}
		if body["status"] != "pending" || body["messageType"] != "text" || body["textContent"] != "We have it in stock!" {
			t.Errorf("row fields = %v", body)
		}
		if v := body["recipientPhone"]; v != "+233822000001" {
			t.Errorf("recipientPhone = %v (conversation.customerPhone)", v)
		}
		rawPayload, _ := body["rawPayload"].(map[string]any)
		if rawPayload["messaging_product"] != "whatsapp" || rawPayload["to"] != "+233822000001" ||
			rawPayload["type"] != "text" {
			t.Errorf("rawPayload envelope = %v", rawPayload)
		}
		textObj, _ := rawPayload["text"].(map[string]any)
		if textObj["body"] != "We have it in stock!" {
			t.Errorf("rawPayload.text = %v, want body %q", rawPayload["text"], "We have it in stock!")
		}
		if body["metaResponse"] != nil || body["whatsappMessageId"] != nil {
			t.Errorf("pending row must carry null metaResponse/whatsappMessageId: %v %v",
				body["metaResponse"], body["whatsappMessageId"])
		}

		if len(spy.ids) != 1 {
			t.Fatalf("publisher calls = %d, want 1", len(spy.ids))
		}
		if spy.ids[0] != body["id"].(string) {
			t.Errorf("published id = %s, want row id %v", spy.ids[0], body["id"])
		}

		var status string
		if err := e.DB.QueryRow(`SELECT status FROM outbound_messages WHERE id = $1`, body["id"]).Scan(&status); err != nil || status != "pending" {
			t.Errorf("db row status = %q err=%v", status, err)
		}
	})

	t.Run("OUTSIDE window -> failed_24h_window_closed row, publisher NOT called", func(t *testing.T) {
		spy := &s4o_publisherSpy{}
		hh := ep_mount(t, "/conversations", conversationsWriteTree(e, spy))
		conv := e.F.Conversation(biz.ID, "+233822000002")
		// Newest inbound backdated past the window via SQL.
		ep_seedInboundMessage(t, e, "s4o-in-2", conv.ID, biz.ID, "+233822000002", "old msg", now.Add(-25*time.Hour))

		rec := s4o_do(t, hh, http.MethodPost, "/conversations/"+conv.ID+"/reply",
			`{"text":"compliance blocked"}`, &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}

		body := ep_decode(t, rec)
		if body["status"] != "failed_24h_window_closed" {
			t.Errorf("status = %v", body["status"])
		}
		rawPayload, _ := body["rawPayload"].(map[string]any)
		if len(rawPayload) != 0 {
			t.Errorf("blocked rawPayload = %v, want empty object {}", rawPayload)
		}

		if len(spy.ids) != 0 {
			t.Errorf("publisher must NOT be called for window-blocked sends; got %v", spy.ids)
		}

		var status string
		var raw string
		if err := e.DB.QueryRow(`SELECT status, raw_payload::text FROM outbound_messages WHERE conversation_id = $1`,
			conv.ID).Scan(&status, &raw); err != nil || status != "failed_24h_window_closed" || raw != "{}" {
			t.Errorf("db row = status:%q raw:%q err=%v", status, raw, err)
		}
	})

	t.Run("no inbound at all -> window closed (latestInbound null)", func(t *testing.T) {
		spy := &s4o_publisherSpy{}
		hh := ep_mount(t, "/conversations", conversationsWriteTree(e, spy))
		conv := e.F.Conversation(biz.ID, "+233822000003")

		rec := s4o_do(t, hh, http.MethodPost, "/conversations/"+conv.ID+"/reply",
			`{"text":"anyone there?"}`, &claims)
		body := ep_decode(t, rec)
		if body["status"] != "failed_24h_window_closed" || len(spy.ids) != 0 {
			t.Errorf("no-inbound reply = status:%v published:%v", body["status"], spy.ids)
		}
	})

	t.Run("boundary: exactly 24h old inbound -> closed (< strict)", func(t *testing.T) {
		spy := &s4o_publisherSpy{}
		hh := ep_mount(t, "/conversations", conversationsWriteTree(e, spy))
		conv := e.F.Conversation(biz.ID, "+233822000004")
		ep_seedInboundMessage(t, e, "s4o-in-4", conv.ID, biz.ID, "+233822000004", "edge", now.Add(-wo_window-time.Minute))

		rec := s4o_do(t, hh, http.MethodPost, "/conversations/"+conv.ID+"/reply", `{"text":"x"}`, &claims)
		body := ep_decode(t, rec)
		if body["status"] != "failed_24h_window_closed" {
			t.Errorf("status = %v", body["status"])
		}
	})

	t.Run("cross-tenant conversation -> 404 Conversation not found", func(t *testing.T) {
		other := e.F.Business()
		oConv := e.F.Conversation(other.ID, "+233822000009")
		ep_seedInboundMessage(t, e, "s4o-in-x", oConv.ID, other.ID, "+233822000009", "hi", now)

		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+oConv.ID+"/reply",
			`{"text":"steal?"}`, &claims)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var n int
		_ = e.DB.QueryRow(`SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, oConv.ID).Scan(&n)
		if n != 0 {
			t.Errorf("cross-tenant reply must not persist rows, found %d", n)
		}
	})

	t.Run("ReplyDto: missing text -> invalid_type Required; empty -> too_small", func(t *testing.T) {
		conv := e.F.Conversation(biz.ID, "+233822000005")
		ep_seedInboundMessage(t, e, "s4o-in-5", conv.ID, biz.ID, "+233822000005", "hi", now)

		rec := s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/reply", `{}`, &claims)
		s4o_assertZodIssue(t, rec, "invalid_type", "text")

		rec = s4o_do(t, h, http.MethodPost, "/conversations/"+conv.ID+"/reply", `{"text":""}`, &claims)
		s4o_assertZodIssue(t, rec, "too_small", "text")

		var n int
		_ = e.DB.QueryRow(`SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, conv.ID).Scan(&n)
		if n != 0 {
			t.Errorf("validation failures must not persist rows, found %d", n)
		}
	})
}
