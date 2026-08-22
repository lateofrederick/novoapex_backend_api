package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// T2.22 GET /dashboard/summary
func TestDashboardSummary(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	now := time.Now().UTC()
	ep_seedPayment(t, e, "pay_s1", biz.ID, "", "SUCCESS", "100.50", now, now)
	ep_seedPayment(t, e, "pay_s2", biz.ID, "", "SUCCESS", "49.50", now, now)
	ep_seedPayment(t, e, "pay_p1", biz.ID, "", "PENDING", "999.00", now, now)
	ep_seedPayment(t, e, "pay_f1", biz.ID, "", "FAILED", "75.00", now, now)
	otherBiz := e.F.Business()
	ep_seedPayment(t, e, "pay_x", otherBiz.ID, "", "SUCCESS", "5000.00", now, now)

	ep_seedConversation(t, e, "cv_e", biz.ID, "", "+233607000001", "SUPPORT", true, "", now)
	ep_seedConversation(t, e, "cv_n1", biz.ID, "", "+233607000002", "LEAD", false, "", now)
	ep_seedConversation(t, e, "cv_n2", biz.ID, "", "+233607000003", "LEAD", false, "", now)

	rec := ep_do(t, NewDashboard(e.Pool), http.MethodGet, "/summary",
		&auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := ep_decode(t, rec)
	if ks := fmt.Sprint(ep_keys(t, body)); ks != "[activeHandoffs pendingPayments totalPayouts]" {
		t.Fatalf("key set = %s", ks)
	}
	if body["pendingPayments"] != json_Number("1") {
		t.Errorf("pendingPayments = %v", body["pendingPayments"])
	}
	if body["activeHandoffs"] != json_Number("1") {
		t.Errorf("activeHandoffs = %v", body["activeHandoffs"])
	}
	// CONTRACT QUIRK: totalPayouts is the sum of SUCCESS PAYMENTS (not payouts).
	ep_assertMoney(t, body["totalPayouts"], "150")
}

// T2.23 GET /analytics/overview
func TestAnalyticsOverview(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	now := time.Now().UTC()
	cust := e.F.Customer(biz.ID)

	ep_seedConversation(t, e, "cv_esc", biz.ID, cust.ID, cust.Phone, "ESCALATED", true, "", now)
	ep_seedConversation(t, e, "cv_norm", biz.ID, "", "+233608000001", "LEAD", false, "", now)

	// rescuedLeads: PAID orders whose conversation was escalated — only r1.
	ep_seedOrder(t, e, "an_r1", biz.ID, cust.ID, "cv_esc", "PAID", "10.00", now)
	ep_seedOrder(t, e, "an_r2", biz.ID, cust.ID, "cv_norm", "PAID", "11.00", now)           // not escalated
	ep_seedOrder(t, e, "an_r3", biz.ID, cust.ID, "cv_esc", "PAYMENT_PENDING", "12.00", now) // not PAID
	ep_seedOrder(t, e, "an_r4", biz.ID, cust.ID, "", "PAID", "13.00", now)                  // no conversation

	rec := ep_do(t, NewAnalytics(e.Pool), http.MethodGet, "/overview",
		&auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := ep_decode(t, rec)
	if ks := fmt.Sprint(ep_keys(t, body)); ks != "[averageResponseTimeMins rescuedLeads totalConversations]" {
		t.Fatalf("key set = %s", ks)
	}
	if body["rescuedLeads"] != json_Number("1") {
		t.Errorf("rescuedLeads = %v, want 1", body["rescuedLeads"])
	}
	if body["totalConversations"] != json_Number("2") {
		t.Errorf("totalConversations = %v", body["totalConversations"])
	}
	if body["averageResponseTimeMins"] != json_Number("5") {
		t.Errorf("stub averageResponseTimeMins = %v, want literal 5", body["averageResponseTimeMins"])
	}
}

// T2.24 GET /analytics/handoff-reasons
func TestAnalyticsHandoffReasons(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	now := time.Now().UTC()
	for i, reason := range []string{"price", "price", "price", "support"} {
		ep_seedConversation(t, e, fmt.Sprintf("hr_%d", i), biz.ID, "",
			fmt.Sprintf("+233609000%03d", i), "ESCALATED", true, reason, now)
	}
	// NULL reasons are excluded by the groupBy filter.
	ep_seedConversation(t, e, "hr_null", biz.ID, "", "+233609000999", "LEAD", false, "", now)

	rec := ep_do(t, NewAnalytics(e.Pool), http.MethodGet, "/handoff-reasons",
		&auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	rows := ep_decodeArray(t, rec)
	if len(rows) != 2 {
		t.Fatalf("rows = %d (%v)", len(rows), rows)
	}
	first := rows[0].(map[string]any)
	if ks := fmt.Sprint(ep_keys(t, first)); ks != "[count reason]" {
		t.Fatalf("key set = %s", ks)
	}
	if first["reason"] != "price" || first["count"] != json_Number("3") {
		t.Errorf("first = %v, want price/3 (count desc)", first)
	}
	second := rows[1].(map[string]any)
	if second["reason"] != "support" || second["count"] != json_Number("1") {
		t.Errorf("second = %v", second)
	}
}

// T2.25 GET /analytics/demand
func TestAnalyticsDemand(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	requestCounts := map[string]int{"d_9": 9, "d_7a": 7, "d_7b": 7, "d_6": 6, "d_2": 2, "d_1": 1}
	for _, count := range requestCounts {
		p := e.F.Product(biz.ID)
		if _, err := e.DB.Exec(`UPDATE products SET request_count = $2 WHERE id = $1`, p.ID, count); err != nil {
			t.Fatalf("bump request_count: %v", err)
		}
	}
	noise := e.F.Product(e.F.Business().ID)
	if _, err := e.DB.Exec(`UPDATE products SET request_count = 1000 WHERE id = $1`, noise.ID); err != nil {
		t.Fatal(err)
	}

	rec := ep_do(t, NewAnalytics(e.Pool), http.MethodGet, "/demand",
		&auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	rows := ep_decodeArray(t, rec)
	if len(rows) != 5 { // take: 5
		t.Fatalf("rows = %d, want exactly top 5 of 6", len(rows))
	}
	first := rows[0].(map[string]any)
	if ks := fmt.Sprint(ep_keys(t, first)); ks != "[id name requestCount]" {
		t.Fatalf("key set = %s, want [id name requestCount] ONLY", ks)
	}
	if first["id"] == noise.ID || first["requestCount"] != json_Number("9") {
		t.Errorf("top row = %v (tenant noise must be excluded)", first)
	}

	// request_count strictly decreasing across the page.
	prev := int64(1 << 62)
	for _, raw := range rows {
		m := raw.(map[string]any)
		nStr, _ := m["requestCount"].(json.Number)
		n, err := nStr.Int64()
		if err != nil {
			t.Fatalf("requestCount not a number: %v", m["requestCount"])
		}
		if n > prev {
			t.Errorf("requestCount not descending at %v", m)
		}
		prev = n
	}
}
