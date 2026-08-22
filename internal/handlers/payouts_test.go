package handlers

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// T2.26 GET /payouts/balance — T0.16 arithmetic:
// balance = sum(SUCCESS payments) - sum(SUCCESS+PENDING payouts), floored 0.
func TestPayoutsBalance(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	now := time.Now().UTC()
	ep_seedPayment(t, e, "pb_s1", biz.ID, "", "SUCCESS", "100.50", now, now)
	ep_seedPayment(t, e, "pb_s2", biz.ID, "", "SUCCESS", "49.50", now, now)
	ep_seedPayment(t, e, "pb_p", biz.ID, "", "PENDING", "500.00", now, now) // ignored
	ep_seedPayment(t, e, "pb_f", biz.ID, "", "FAILED", "75.00", now, now)   // ignored

	ep_seedPayout(t, e, "po_s", biz.ID, "SUCCESS", "30.00", "ref-s", now)
	ep_seedPayout(t, e, "po_p", biz.ID, "PENDING", "20.25", "ref-p", now)
	ep_seedPayout(t, e, "po_f", biz.ID, "FAILED", "10.00", "ref-f", now) // releases reservation

	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	h := ep_mount(t, "/payouts", NewPayouts(e.Pool))

	t.Run("formula and currency", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/payouts/balance", &claims)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if ks := fmt.Sprint(ep_keys(t, body)); ks != "[availableBalance committedPayouts currency totalRevenue]" {
			t.Fatalf("key set = %s", ks)
		}
		ep_assertMoney(t, body["totalRevenue"], "150")
		ep_assertMoney(t, body["committedPayouts"], "50.25") // SUCCESS+PENDING only
		ep_assertMoney(t, body["availableBalance"], "99.75")
		if body["currency"] != "GHS" {
			t.Errorf("currency = %v (from businesses row)", body["currency"])
		}
	})

	t.Run("floored at zero like Math.max(0, ...)", func(t *testing.T) {
		biz2 := e.F.Business()
		ep_seedPayment(t, e, "pf_s", biz2.ID, "", "SUCCESS", "10.00", now, now)
		ep_seedPayout(t, e, "pf_o", biz2.ID, "SUCCESS", "25.00", "pf-ref", now)

		rec := ep_do(t, h, http.MethodGet, "/payouts/balance",
			&auth.Claims{Phone: biz2.OwnerPhone, BusinessID: biz2.ID})
		body := ep_decode(t, rec)
		ep_assertMoney(t, body["availableBalance"], "0")
	})

	t.Run("unknown business -> BadRequestException", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/payouts/balance",
			&auth.Claims{Phone: biz.OwnerPhone, BusinessID: "00000000-0000-0000-0000-000000000000"})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if body["statusCode"] != json_Number("400") || body["message"] != "Business not found" {
			t.Errorf("body = %v", body)
		}
	})
}

// T2.27 GET /payouts/history
func TestPayoutsHistory(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	base := time.Now().UTC()
	ep_seedPayout(t, e, "ph_1", biz.ID, "PENDING", "20.25", "ref-1", base.Add(-1*time.Hour))
	ep_seedPayout(t, e, "ph_2", biz.ID, "SUCCESS", "30.00", "ref-2", base)
	ep_seedPayout(t, e, "ph_3", biz.ID, "FAILED", "5.00", "", base.Add(-2*time.Hour)) // null reference
	ep_seedPayout(t, e, "ph_x", e.F.Business().ID, "SUCCESS", "777.00", "ref-x", base)

	h := ep_mount(t, "/payouts", NewPayouts(e.Pool))
	rec := ep_do(t, h, http.MethodGet, "/payouts/history?page=1&limit=3",
		&auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := ep_decode(t, rec)

	data := body["data"].([]any)
	if len(data) != 3 {
		t.Fatalf("rows = %d", len(data))
	}
	wantOrder := []string{"ph_2", "ph_1", "ph_3"} // createdAt desc
	for i, raw := range data {
		row := raw.(map[string]any)
		if row["id"] != wantOrder[i] {
			t.Fatalf("row[%d] = %v, want %s", i, row["id"], wantOrder[i])
		}
		wantKeys := "[amount businessId createdAt currency id paystackTransferId reference status updatedAt]"
		if ks := fmt.Sprint(ep_keys(t, row)); ks != wantKeys {
			t.Errorf("payout key set = %s\nwant        %s", ks, wantKeys)
		}
	}
	first := data[0].(map[string]any)
	ep_assertMoney(t, first["amount"], "30.00")
	if first["paystackTransferId"] != nil {
		t.Errorf("fixture paystackTransferId should be null, got %v", first["paystackTransferId"])
	}
	nullRef := data[2].(map[string]any)
	if v, exists := nullRef["reference"]; !exists || v != nil {
		t.Errorf("null reference must be an explicit null key, got exists=%v v=%v", exists, v)
	}

	meta := body["meta"].(map[string]any)
	if meta["total"] != json_Number("3") || meta["totalPages"] != json_Number("1") {
		t.Errorf("meta = %v (scoped total)", meta)
	}
}
