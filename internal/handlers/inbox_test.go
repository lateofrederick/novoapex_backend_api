package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// T2.19 GET /inbox/summary, T2.20 GET /inbox/handoffs,
// T2.21 GET /conversations/:id/messages
func TestInboxSummary(t *testing.T) {
	e := ep_startTest(t)
	bizA := e.F.Business()
	bizB := e.F.Business()

	base := time.Now().UTC()
	ep_seedConversation(t, e, "cv_a1", bizA.ID, "", "+233601000001", "ESCALATED", true, "price", base)
	ep_seedConversation(t, e, "cv_a2", bizA.ID, "", "+233601000002", "LEAD", false, "", base)
	ep_seedConversation(t, e, "cv_b1", bizB.ID, "", "+233601000003", "SUPPORT", true, "angry", base)

	rec := ep_do(t, NewInbox(e.Pool), http.MethodGet, "/summary",
		&auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := ep_decode(t, rec)
	if ks := fmt.Sprint(ep_keys(t, body)); ks != "[handoffs totalConversations]" {
		t.Fatalf("key set = %s", ks)
	}
	if body["totalConversations"] != json_Number("2") || body["handoffs"] != json_Number("1") {
		t.Errorf("summary = %v (scoped: B's handoff excluded)", body)
	}
}

func TestInboxHandoffs(t *testing.T) {
	e := ep_startTest(t)
	bizA := e.F.Business()
	bizB := e.F.Business()

	cust := e.F.Customer(bizA.ID)
	base := time.Now().UTC()

	ep_seedConversation(t, e, "cv_linked", bizA.ID, cust.ID, cust.Phone, "ESCALATED", true, "refund", base)
	ep_seedConversation(t, e, "cv_unlinked", bizA.ID, "", "+233601111111", "CHECKOUT", true, "", base.Add(-1*time.Hour))
	ep_seedConversation(t, e, "cv_normal", bizA.ID, "", "+233601222222", "LEAD", false, "", base)
	ep_seedConversation(t, e, "cv_other", bizB.ID, "", "+233603333333", "SUPPORT", true, "", base)

	claimsA := auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID}
	h := ep_mount(t, "/inbox", NewInbox(e.Pool))

	t.Run("escalated only with customer or null ordered updatedAt desc", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/inbox/handoffs?page=1&limit=10", &claimsA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		data := body["data"].([]any)
		if len(data) != 2 {
			t.Fatalf("rows = %d, want 2 escalated of A", len(data))
		}

		first := data[0].(map[string]any)
		wantKeys := "[businessId createdAt customer customerId customerPhone escalationReason id isEscalatedToHuman language state updatedAt]"
		if ks := fmt.Sprint(ep_keys(t, first)); ks != wantKeys {
			t.Errorf("conversation key set = %s\nwant                   %s", ks, wantKeys)
		}
		if first["id"] != "cv_linked" { // newest updated_at first
			t.Errorf("orderBy updatedAt desc broken: %v", first["id"])
		}

		linked := first["customer"].(map[string]any)
		if linked["id"] != cust.ID {
			t.Errorf("include customer = %v", linked)
		}

		second := data[1].(map[string]any)
		if v, exists := second["customer"]; !exists || v != nil {
			t.Errorf("unlinked conversation must serialise customer:null, got exists=%v v=%v", exists, v)
		}
		if second["state"] != "CHECKOUT" || second["isEscalatedToHuman"] != true {
			t.Errorf("second row = %v", second)
		}

		meta := body["meta"].(map[string]any)
		if meta["total"] != json_Number("2") {
			t.Errorf("meta = %v", meta)
		}
	})

	t.Run("pagination over handoffs", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/inbox/handoffs?page=2&limit=1", &claimsA)
		data := ep_decode(t, rec)["data"].([]any)
		if len(data) != 1 || data[0].(map[string]any)["id"] != "cv_unlinked" {
			t.Fatalf("page2 = %v", data)
		}
	})
}

func TestConversationMessages(t *testing.T) {
	e := ep_startTest(t)
	bizA := e.F.Business()
	bizB := e.F.Business()

	base := time.Now().UTC().Truncate(time.Second)
	conv := "cv_msgs"
	ep_seedConversation(t, e, conv, bizA.ID, "", "+233604444444", "SUPPORT", true, "", base)

	// Interleaved timeline: i1 < o1 < i2 < o2 across BOTH sides.
	ep_seedInboundMessage(t, e, "in_1", conv, bizA.ID, "+233604444444", "first inbound", base.Add(0))
	ep_seedOutboundMessage(t, e, "out_1", conv, bizA.ID, "+233604444444", "first reply", "sent", base.Add(1*time.Minute))
	ep_seedInboundMessage(t, e, "in_2", conv, bizA.ID, "+233604444444", "second inbound", base.Add(2*time.Minute))
	ep_seedOutboundMessage(t, e, "out_2", conv, bizA.ID, "+233604444444", "second reply", "sent", base.Add(3*time.Minute))

	otherConv := "cv_other_tenant"
	ep_seedConversation(t, e, otherConv, bizB.ID, "", "+233605555555", "LEAD", false, "", base)
	ep_seedInboundMessage(t, e, "in_x", otherConv, bizB.ID, "+233605555555", "secret", base)

	claimsA := auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID}
	h := ep_mount(t, "/conversations", NewConversations(e.Pool))
	path := "/conversations/" + conv + "/messages"

	t.Run("merge ascending with type and time tags", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, path, &claimsA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		msgs := ep_decodeArray(t, rec)
		if len(msgs) != 4 {
			t.Fatalf("merged len = %d, want 4", len(msgs))
		}

		type seq struct {
			id   string
			typ  string
			time string
		}
		var got []seq
		for _, raw := range msgs {
			m := raw.(map[string]any)
			got = append(got, seq{id: m["id"].(string), typ: m["type"].(string), time: m["time"].(string)})
			ep_assertISO(t, "time", m["time"])
		}
		want := []seq{
			{id: "in_1", typ: "inbound"}, {id: "out_1", typ: "outbound"},
			{id: "in_2", typ: "inbound"}, {id: "out_2", typ: "outbound"},
		}
		for i, s := range want {
			if got[i].id != s.id || got[i].typ != s.typ {
				t.Fatalf("merged[%d] = %+v, want %+v (ascending merge across sides)", i, got[i], s)
			}
		}

		in := msgs[0].(map[string]any)
		wantInKeys := "[businessId conversationId createdAt id messageType rawPayload recipientPhone senderPhone textContent time timestamp type whatsappMessageId]"
		if ks := fmt.Sprint(ep_keys(t, in)); ks != wantInKeys {
			t.Errorf("inbound key set = %s\nwant            %s", ks, wantInKeys)
		}
		if in["timestamp"] != in["time"] {
			t.Errorf("inbound time must mirror timestamp: %v vs %v", in["time"], in["timestamp"])
		}
		payload := in["rawPayload"].(map[string]any)
		if payload["text"] != "first inbound" {
			t.Errorf("rawPayload passthrough broken: %v", payload)
		}

		out := msgs[1].(map[string]any)
		wantOutKeys := "[businessId conversationId createdAt id imageUrl messageType metaResponse productId rawPayload recipientPhone status templateName textContent time type whatsappMessageId]"
		if ks := fmt.Sprint(ep_keys(t, out)); ks != wantOutKeys {
			t.Errorf("outbound key set = %s\nwant             %s", ks, wantOutKeys)
		}
		if v, exists := out["metaResponse"]; !exists || v != nil {
			t.Errorf("null jsonb metaResponse must be explicit null, got exists=%v v=%v", exists, v)
		}
		if out["createdAt"] != out["time"] {
			t.Errorf("outbound time must mirror createdAt: %v vs %v", out["time"], out["createdAt"])
		}
		if out["status"] != "sent" {
			t.Errorf("status = %v", out["status"])
		}
	})

	t.Run("limit keeps the most recent after merge", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, path+"?limit=2", &claimsA)
		msgs := ep_decodeArray(t, rec)
		if len(msgs) != 2 {
			t.Fatalf("len = %d", len(msgs))
		}
		if msgs[0].(map[string]any)["id"] != "in_2" || msgs[1].(map[string]any)["id"] != "out_2" {
			t.Fatalf("slice(-limit) wrong: %v", msgs)
		}

		one := ep_decodeArray(t, ep_do(t, h, http.MethodGet, path+"?limit=1", &claimsA))
		if one[0].(map[string]any)["id"] != "out_2" {
			t.Fatalf("limit=1 -> %v", one)
		}
	})

	t.Run("limit coercion NaN and clamps", func(t *testing.T) {
		all := ep_decodeArray(t, ep_do(t, h, http.MethodGet, path+"?limit=abc", &claimsA))
		if len(all) != 4 {
			t.Errorf("Number('abc')||50 must fall back to default 50, got %d rows", len(all))
		}
		zero := ep_decodeArray(t, ep_do(t, h, http.MethodGet, path+"?limit=0", &claimsA))
		if len(zero) != 1 {
			t.Errorf("limit=0 clamps to 1, got %d rows", len(zero))
		}
		huge := ep_decodeArray(t, ep_do(t, h, http.MethodGet, path+"?limit=500", &claimsA))
		if len(huge) != 4 {
			t.Errorf("limit>200 caps at 200 (all rows here), got %d", len(huge))
		}
	})

	t.Run("empty thread renders []", func(t *testing.T) {
		e2 := "cv_empty"
		ep_seedConversation(t, e, e2, bizA.ID, "", "+233606666666", "LEAD", false, "", base)
		rec := ep_do(t, h, http.MethodGet, "/conversations/"+e2+"/messages", &claimsA)
		if !strings.Contains(rec.Body.String(), "[]") {
			t.Fatalf("body = %s, want []", rec.Body.String())
		}
	})

	t.Run("404 unknown and cross-tenant conversations", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/conversations/nope/messages", &claimsA)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unknown status=%d", rec.Code)
		}
		if m := ep_decode(t, rec); m["message"] != "Conversation not found" {
			t.Errorf("message = %v", m["message"])
		}

		cross := ep_do(t, h, http.MethodGet, "/conversations/"+otherConv+"/messages", &claimsA)
		if cross.Code != http.StatusNotFound {
			t.Fatalf("cross-tenant messages status=%d want 404", cross.Code)
		}
	})
}
