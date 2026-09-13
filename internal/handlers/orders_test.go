package handlers

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// T2.16 GET /orders (incl items), T2.17 summary, T2.18 :id
func TestOrdersList(t *testing.T) {
	e := ep_startTest(t)
	bizA := e.F.Business()
	bizB := e.F.Business()

	custA := e.F.Customer(bizA.ID)
	pA1 := e.F.Product(bizA.ID)
	pA2 := e.F.Product(bizA.ID)

	base := time.Now().UTC()
	ep_seedOrder(t, e, "ord_o1", bizA.ID, custA.ID, "", "PAID", "35.50", base.Add(-1*time.Hour))
	ep_seedOrderItem(t, e, "oi_1", "ord_o1", pA1.ID, pA1.Name, 2, "10.00")
	ep_seedOrderItem(t, e, "oi_2", "ord_o1", pA2.ID, pA2.Name, 1, "15.50")

	ep_seedOrder(t, e, "ord_o2", bizA.ID, custA.ID, "", "PENDING", "12.25", base.Add(-30*time.Minute))
	ep_seedOrder(t, e, "ord_o3", bizA.ID, custA.ID, "", "CONFIRMED", "20.00", base.Add(-20*time.Minute))
	ep_seedOrder(t, e, "ord_o4", bizA.ID, custA.ID, "", "CANCELLED", "999.99", base.Add(-10*time.Minute))
	ep_seedOrder(t, e, "ord_oB", bizB.ID, e.F.Customer(bizB.ID).ID, "", "PENDING", "77.00", base)

	claimsA := auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID}
	h := ep_mount(t, "/orders", NewOrders(e.Pool))

	t.Run("list includes customer newest first scoped", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/orders?page=1&limit=3", &claimsA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		data := body["data"].([]any)
		if len(data) != 3 {
			t.Fatalf("page len = %d", len(data))
		}
		wantOrder := []string{"ord_o4", "ord_o3", "ord_o2"} // createdAt desc
		for i, raw := range data {
			row := raw.(map[string]any)
			if row["id"] != wantOrder[i] {
				t.Fatalf("row[%d] = %v, want %s (orderBy createdAt desc)", i, row["id"], wantOrder[i])
			}

			wantKeys := "[businessId conversationId createdAt currency customer customerId fulfillmentType id idempotencyKey location locationId status totalAmount updatedAt]"
			if ks := fmt.Sprint(ep_keys(t, row)); ks != wantKeys {
				t.Errorf("order key set = %s\nwant        %s", ks, wantKeys)
			}
			if _, has := row["items"]; has {
				t.Error("findAll must NOT include items (Node include is customer only)")
			}
			if row["customer"] == nil {
				t.Fatal("include customer missing")
			}
			cust := row["customer"].(map[string]any)
			if cust["id"] != custA.ID || cust["phone"] != custA.Phone {
				t.Errorf("nested customer = %v/%v, want %s/%s", cust["id"], cust["phone"], custA.ID, custA.Phone)
			}
			wantCustKeys := "[acquisitionChannel businessId createdAt firstContactAt id lastContactAt marketingOptIn name phone updatedAt]"
			if ks := fmt.Sprint(ep_keys(t, cust)); ks != wantCustKeys {
				t.Errorf("nested customer key set = %s", ks)
			}
		}

		first := data[0].(map[string]any)
		if first["idempotencyKey"] != "wamid-ord_o4" {
			t.Errorf("idempotencyKey = %v", first["idempotencyKey"])
		}
		if v, exists := first["conversationId"]; !exists || v != nil {
			t.Errorf("conversationId must be explicit null, got exists=%v v=%v", exists, v)
		}
		meta := body["meta"].(map[string]any)
		if meta["total"] != json_Number("4") {
			t.Errorf("meta.total = %v, want 4 (scoped)", meta["total"])
		}
	})

	t.Run("T2.17 summary math", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/orders/summary", &claimsA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if ks := fmt.Sprint(ep_keys(t, body)); ks != "[paidOrders pendingOrders totalOrders totalRevenue]" {
			t.Fatalf("key set = %s", ks)
		}
		if body["totalOrders"] != json_Number("4") ||
			body["pendingOrders"] != json_Number("1") ||
			body["paidOrders"] != json_Number("1") {
			t.Errorf("counts = %v", body)
		}
		ep_assertMoney(t, body["totalRevenue"], "35.50") // sum(totalAmount) where PAID only
	})

	t.Run("T2.18 detail includes items with plain products", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/orders/ord_o1", &claimsA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)

		wantTop := "[businessId conversationId createdAt currency customer customerId fulfillmentType id idempotencyKey items location locationId status totalAmount updatedAt]"
		if ks := fmt.Sprint(ep_keys(t, body)); ks != wantTop {
			t.Errorf("detail key set = %s\nwant        %s", ks, wantTop)
		}
		ep_assertMoney(t, body["totalAmount"], "35.50")
		if body["status"] != "PAID" {
			t.Errorf("status = %v", body["status"])
		}

		items := body["items"].([]any)
		if len(items) != 2 {
			t.Fatalf("items len = %d", len(items))
		}
		item0 := items[0].(map[string]any)
		wantItemKeys := "[id orderId product productId productName quantity unitPrice]"
		if ks := fmt.Sprint(ep_keys(t, item0)); ks != wantItemKeys {
			t.Errorf("item key set = %s, want %s", ks, wantItemKeys)
		}
		if item0["productName"] != pA1.Name || item0["quantity"] != json_Number("2") {
			t.Errorf("item0 = %v", item0)
		}
		ep_assertMoney(t, item0["unitPrice"], "10.00")

		prod := item0["product"].(map[string]any)
		wantProdKeys := "[businessId category createdAt deliveryNote description id isAvailable name price requestCount sku stock stockNote updatedAt]"
		if ks := fmt.Sprint(ep_keys(t, prod)); ks != wantProdKeys {
			t.Errorf("nested product key set = %s\nwant                   %s", ks, wantProdKeys)
		}
		if _, has := prod["images"]; has {
			t.Error("items.product must NOT include images (Node include is bare)")
		}
		ep_assertMoney(t, prod["price"], "25.50")
	})

	t.Run("404 semantics", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/orders/missing", &claimsA)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unknown order status=%d", rec.Code)
		}
		if m := ep_decode(t, rec); m["message"] != "Order not found" {
			t.Errorf("message = %v", m["message"])
		}

		cross := ep_do(t, h, http.MethodGet, "/orders/ord_o1",
			&auth.Claims{Phone: bizB.OwnerPhone, BusinessID: bizB.ID})
		if cross.Code != http.StatusNotFound {
			t.Errorf("cross-tenant order fetch status=%d want 404", cross.Code)
		}
	})
}
