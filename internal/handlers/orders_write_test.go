package handlers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// ---- Stage 4B shared test helpers (prefix s4o_) ---------------------------

// s4o_do performs an authenticated request with an optional JSON body.
func s4o_do(t *testing.T, h http.Handler, method, target, body string, claims *auth.Claims) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	if claims != nil {
		req.Header.Set("Authorization", "Bearer "+ep_mintToken(t, *claims))
	}
	rec := httptest.NewRecorder()
	ep_withClaims(h).ServeHTTP(rec, req)
	return rec
}

// s4o_assertZodIssue asserts the nestjs-zod 400 envelope carries an issue
// with the expected code and first path segment.
func s4o_assertZodIssue(t *testing.T, rec *httptest.ResponseRecorder, code, path string) {
	t.Helper()
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := ep_decode(t, rec)
	if body["statusCode"] != json_Number("400") || body["message"] != "Validation failed" {
		t.Fatalf("envelope = %v", body)
	}
	errs, _ := body["errors"].([]any)
	for _, raw := range errs {
		issue, _ := raw.(map[string]any)
		gotPath := ""
		if segs, ok := issue["path"].([]any); ok && len(segs) > 0 {
			gotPath, _ = segs[0].(string)
		}
		if issue["code"] == code && gotPath == path {
			return
		}
	}
	t.Fatalf("no %s issue on %s within %s", code, path, rec.Body.String())
}

// ordersWriteTree builds the write subtree the way central integration will
// (MountOrdersWrite registered on the shared /orders prefix router).
func ordersWriteTree(e *epEnv) http.Handler {
	r := chi.NewRouter()
	MountOrdersWrite(r, OrdersWriteDeps{Pool: e.Pool})
	return r
}

// T4.16 PATCH /orders/:id/fulfillment — happy path, every DTO enum value,
// invalid status zod 400, cross-tenant 404.
func TestS4O_T4_16_Fulfillment(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	other := e.F.Business()
	cust := e.F.Customer(biz.ID)
	now := time.Now().UTC()

	h := ep_mount(t, "/orders", ordersWriteTree(e))
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}

	orderID := "s4o-ord-1"
	ep_seedOrder(t, e, orderID, biz.ID, cust.ID, "", "PENDING", "120.00", now)
	crossID := "s4o-ord-cross"
	ep_seedOrder(t, e, crossID, other.ID, e.F.Customer(other.ID).ID, "", "PENDING", "5.00", now)

	t.Run("happy path updates row and answers {success,status}", func(t *testing.T) {
		rec := s4o_do(t, h, http.MethodPatch, "/orders/"+orderID+"/fulfillment",
			`{"status":"PAID"}`, &claims)
		if rec.Code != http.StatusOK { // PATCH -> 200 (Nest default for non-POST)
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if ks := fmt.Sprint(ep_keys(t, body)); ks != "[status success]" {
			t.Errorf("key set = %s", ks)
		}
		if body["success"] != true || body["status"] != "PAID" {
			t.Errorf("body = %v", body)
		}

		var got string
		if err := e.DB.QueryRow(`SELECT status::text FROM orders WHERE id = $1`, orderID).Scan(&got); err != nil || got != "PAID" {
			t.Errorf("db status = %q err=%v", got, err)
		}
	})

	t.Run("every UpdateOrderDto enum value is accepted", func(t *testing.T) {
		for _, s := range []string{"PENDING", "CONFIRMED", "PAYMENT_PENDING", "PAID"} {
			rec := s4o_do(t, h, http.MethodPatch, "/orders/"+orderID+"/fulfillment",
				fmt.Sprintf(`{"status":%q}`, s), &claims)
			if rec.Code != http.StatusOK {
				t.Errorf("%s -> status=%d body=%s", s, rec.Code, rec.Body.String())
			}
		}
	})

	t.Run("status outside DTO set -> zod invalid_enum_value 400", func(t *testing.T) {
		rec := s4o_do(t, h, http.MethodPatch, "/orders/"+orderID+"/fulfillment",
			`{"status":"SHIPPED"}`, &claims)
		s4o_assertZodIssue(t, rec, "invalid_enum_value", "status")
		if !strings.Contains(rec.Body.String(), "Invalid enum value") ||
			!strings.Contains(rec.Body.String(), "'PAYMENT_PENDING'") {
			t.Errorf("message shape mismatch: %s", rec.Body.String())
		}
		var dbStatus string
		if err := e.DB.QueryRow(`SELECT status::text FROM orders WHERE id = $1`, orderID).Scan(&dbStatus); err != nil || dbStatus != "PAID" {
			t.Errorf("rejected write must not persist; db=%q err=%v", dbStatus, err)
		}
	})

	t.Run("missing status -> zod invalid_type Required 400", func(t *testing.T) {
		rec := s4o_do(t, h, http.MethodPatch, "/orders/"+orderID+"/fulfillment", `{}`, &claims)
		s4o_assertZodIssue(t, rec, "invalid_type", "status")
	})

	t.Run("cross-tenant order -> NotFound 'Order not found'", func(t *testing.T) {
		rec := s4o_do(t, h, http.MethodPatch, "/orders/"+crossID+"/fulfillment",
			`{"status":"PAID"}`, &claims)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if body["statusCode"] != json_Number("404") || body["message"] != "Order not found" {
			t.Errorf("body = %v", body)
		}
	})
}

// T4.17 POST /orders/:id/escalate — flips ONLY the linked conversation;
// unlinked orders still answer success; 404 scoping holds; 201 like Nest @Post.
func TestS4O_T4_17_Escalate(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	other := e.F.Business()
	cust := e.F.Customer(biz.ID)
	now := time.Now().UTC()

	h := ep_mount(t, "/orders", ordersWriteTree(e))
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}

	t.Run("linked conversation -> isEscalatedToHuman + state ESCALATED", func(t *testing.T) {
		conv := e.F.Conversation(biz.ID, cust.Phone)
		ep_seedOrder(t, e, "s4o-esc-1", biz.ID, cust.ID, conv.ID, "PAID", "10.00", now)

		rec := s4o_do(t, h, http.MethodPost, "/orders/s4o-esc-1/escalate", "", &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if body["success"] != true || body["message"] != "Associated conversation escalated to human" {
			t.Errorf("body = %v", body)
		}

		var escalated bool
		var state, reason string
		if err := e.DB.QueryRow(
			`SELECT is_escalated_to_human, state::text, COALESCE(escalation_reason,'') FROM conversations WHERE id = $1`,
			conv.ID).Scan(&escalated, &state, &reason); err != nil {
			t.Fatalf("scan conversation: %v", err)
		}
		if !escalated || state != "ESCALATED" {
			t.Errorf("conversation flags = escalated:%v state:%s", escalated, state)
		}
		if reason != "" {
			t.Errorf("escalation_reason must stay untouched by operator escalate, got %q", reason)
		}
	})

	t.Run("unlinked order still answers success without side effects", func(t *testing.T) {
		var before int
		if err := e.DB.QueryRow(`SELECT COUNT(*) FROM conversations`).Scan(&before); err != nil {
			t.Fatalf("count before: %v", err)
		}
		ep_seedOrder(t, e, "s4o-esc-2", biz.ID, cust.ID, "", "PENDING", "3.00", now)
		rec := s4o_do(t, h, http.MethodPost, "/orders/s4o-esc-2/escalate", "", &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var after int
		if err := e.DB.QueryRow(`SELECT COUNT(*) FROM conversations`).Scan(&after); err != nil {
			t.Fatalf("count after: %v", err)
		}
		if after != before {
			t.Errorf("escalate must not create conversation rows: before=%d after=%d", before, after)
		}
	})

	t.Run("cross-tenant order -> 404 Order not found", func(t *testing.T) {
		oCust := e.F.Customer(other.ID)
		conv := e.F.Conversation(other.ID, oCust.Phone)
		ep_seedOrder(t, e, "s4o-esc-x", other.ID, oCust.ID, conv.ID, "PAID", "8.00", now)

		rec := s4o_do(t, h, http.MethodPost, "/orders/s4o-esc-x/escalate", "", &claims)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var escalated bool
		_ = e.DB.QueryRow(`SELECT is_escalated_to_human FROM conversations WHERE id = $1`, conv.ID).Scan(&escalated)
		if escalated {
			t.Errorf("other tenant's conversation must not be touched")
		}
	})

	t.Run("unknown order -> 404", func(t *testing.T) {
		rec := s4o_do(t, h, http.MethodPost, "/orders/s4o-none/escalate", "", &claims)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}
