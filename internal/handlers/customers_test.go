package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// T2.9 GET / — MobileApiController.getHello returns the bare string, served
// as text/html (Nest res.send semantics), NOT JSON. Like every vendor route
// it sits behind the default-deny JwtAuthGuard (ALWAYS_PUBLIC_PATHS only has
// /metrics), so it needs a valid bearer token.
func TestRootHello(t *testing.T) {
	h := ep_withClaims(NewRoot())

	rec := ep_do(t, h, http.MethodGet, "/", &auth.Claims{Phone: "+233200000001"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("content-type = %q, want text/html", got)
	}
	if body := rec.Body.String(); body != "Hello World!" {
		t.Errorf("body = %q", body)
	}

	rec = ep_do(t, h, http.MethodGet, "/", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated GET / status = %d, want 401", rec.Code)
	}
}

// T2.10 GET /customers
func TestCustomersList(t *testing.T) {
	e := ep_startTest(t)
	bizA := e.F.Business()
	bizB := e.F.Business()

	old1 := e.F.Customer(bizA.ID) // created earlier via factory seq; force ordering below
	old2 := e.F.Customer(bizA.ID)
	fresh := e.F.Customer(bizA.ID)
	other := e.F.Customer(bizB.ID)

	base := time.Now().UTC()
	ep_setCustomerLastContact(t, e, old1.ID, base.Add(-2*time.Hour))
	ep_setCustomerLastContact(t, e, old2.ID, base.Add(-1*time.Hour))
	ep_setCustomerLastContact(t, e, fresh.ID, base)
	ep_setCustomerLastContact(t, e, other.ID, base)

	claimsA := auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID}

	t.Run("shape order scoping meta", func(t *testing.T) {
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers?page=1&limit=2", &claimsA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)

		if got, ok := body["data"].([]any); !ok || len(got) != 2 {
			t.Fatalf("data = %v (%T)", body["data"], body["data"])
		} else {
			first := got[0].(map[string]any)
			if first["id"] != fresh.ID {
				t.Errorf("orderBy lastContactAt desc broken: first=%v want %s", first["id"], fresh.ID)
			}
			second := got[1].(map[string]any)
			if second["id"] != old2.ID {
				t.Errorf("second=%v want %s", second["id"], old2.ID)
			}

			wantKeys := "[acquisitionChannel businessId createdAt firstContactAt id lastContactAt marketingOptIn name phone updatedAt]"
			if ks := fmt.Sprint(ep_keys(t, first)); ks != wantKeys {
				t.Errorf("customer key set = %s, want %s", ks, wantKeys)
			}
			for _, k := range []string{"firstContactAt", "lastContactAt", "createdAt", "updatedAt"} {
				ep_assertISO(t, k, first[k])
			}
			if first["businessId"] != bizA.ID {
				t.Errorf("businessId = %v", first["businessId"])
			}
			if _, isStr := first["name"].(string); !isStr {
				t.Errorf("name should be a string for fixture customers: %v", first["name"])
			}
		}

		meta := body["meta"].(map[string]any)
		if fmt.Sprint(ep_keys(t, meta)) != "[limit page total totalPages]" {
			t.Errorf("meta keys = %v", ep_keys(t, meta))
		}
		if meta["total"] != json_Number("3") || meta["page"] != json_Number("1") ||
			meta["limit"] != json_Number("2") || meta["totalPages"] != json_Number("2") {
			t.Errorf("meta = %v (total must count ALL scoped rows)", meta)
		}
	})

	t.Run("page two and ceil math", func(t *testing.T) {
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers?page=2&limit=2", &claimsA)
		body := ep_decode(t, rec)
		data := body["data"].([]any)
		if len(data) != 1 || data[0].(map[string]any)["id"] != old1.ID {
			t.Fatalf("page2 data = %v", data)
		}
		if body["meta"].(map[string]any)["totalPages"] != json_Number("2") {
			t.Fatalf("totalPages = %v", body["meta"])
		}
	})

	t.Run("cross-tenant rows absent", func(t *testing.T) {
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers?limit=100", &claimsA)
		for _, row := range ep_decode(t, rec)["data"].([]any) {
			if row.(map[string]any)["id"] == other.ID {
				t.Fatal("business B customer leaked into business A list")
			}
		}
	})

	t.Run("empty list is [] not null", func(t *testing.T) {
		bizC := e.F.Business()
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers",
			&auth.Claims{Phone: bizC.OwnerPhone, BusinessID: bizC.ID})
		raw := rec.Body.String()
		if !strings.Contains(raw, `"data":[]`) {
			t.Fatalf("empty list must render [], got %s", raw)
		}
		if m := ep_decode(t, rec); m["data"] == nil {
			t.Fatal("empty data decoded as null")
		}
	})

	t.Run("invalid limit -> zod validation envelope", func(t *testing.T) {
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers?limit=abc", &claimsA)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if body["statusCode"] != json_Number("400") || body["message"] != "Validation failed" {
			t.Fatalf("envelope = %v", body)
		}
		errs := body["errors"].([]any)
		issue := errs[0].(map[string]any)
		path := issue["path"].([]any)
		if len(path) != 1 || path[0] != "limit" {
			t.Errorf("issue path = %v, want [limit]", path)
		}
	})

	t.Run("no businessId claim -> 403 like @BusinessId()", func(t *testing.T) {
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers",
			&auth.Claims{Phone: "+233999000001"})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

// T2.11 GET /customers/summary
func TestCustomersSummary(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	e.F.Customer(biz.ID) // recent
	stale1 := e.F.Customer(biz.ID)
	stale2 := e.F.Customer(biz.ID)
	e.F.Customer(e.F.Business().ID) // other tenant noise

	if _, err := e.DB.Exec(`UPDATE customers SET created_at = NOW() - INTERVAL '40 days' WHERE id IN ($1, $2)`,
		stale1.ID, stale2.ID); err != nil {
		t.Fatalf("backdate customers: %v", err)
	}

	rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers/summary",
		&auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := ep_decode(t, rec)
	if ks := fmt.Sprint(ep_keys(t, body)); ks != "[newCustomersLast30Days totalCustomers]" {
		t.Fatalf("key set = %s", ks)
	}
	if body["totalCustomers"] != json_Number("3") {
		t.Errorf("totalCustomers = %v, want 3 (scoped to business)", body["totalCustomers"])
	}
	if body["newCustomersLast30Days"] != json_Number("1") {
		t.Errorf("newCustomersLast30Days = %v, want 1 (createdAt >= now-30d)", body["newCustomersLast30Days"])
	}
}

// T2.12 GET /customers/:id
func TestCustomersFindOne(t *testing.T) {
	e := ep_startTest(t)
	bizA := e.F.Business()
	bizB := e.F.Business()

	cust := e.F.Customer(bizA.ID)
	other := e.F.Customer(bizB.ID)

	// Give the profile non-default values incl a nested JSON array preference.
	if _, err := e.DB.Exec(`UPDATE customer_profiles SET
		delivery_area = 'Accra', average_order_value = 51.01, order_frequency_days = 2.5,
		total_orders = 4, total_spent = 204.04, sentiment = 'positive', preferences = $2::jsonb
		WHERE id = $1`, cust.ProfileID, `[["milo",2],{"sweet":true}]`); err != nil {
		t.Fatalf("update profile: %v", err)
	}
	// A nullable column left null on the customer row itself.
	if _, err := e.DB.Exec(`UPDATE customers SET acquisition_channel = NULL WHERE id = $1`, cust.ID); err != nil {
		t.Fatalf("null channel: %v", err)
	}

	claimsA := auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID}

	t.Run("includes profile with exact shapes", func(t *testing.T) {
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers/"+cust.ID, &claimsA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)

		wantTop := "[acquisitionChannel businessId createdAt firstContactAt id lastContactAt marketingOptIn name phone profile updatedAt]"
		if ks := fmt.Sprint(ep_keys(t, body)); ks != wantTop {
			t.Errorf("customer key set = %s, want %s", ks, wantTop)
		}
		if v, exists := body["acquisitionChannel"]; !exists || v != nil {
			t.Errorf("acquisitionChannel must be an explicit null key, got exists=%v v=%v", exists, v)
		}

		profile := body["profile"].(map[string]any)
		wantProfile := "[averageOrderValue customerId deliveryArea id lastOrderAt lastReengagementAt latePaymentCount orderFrequencyDays preferences preferredPaymentNetwork sentiment totalOrders totalSpent updatedAt]"
		if ks := fmt.Sprint(ep_keys(t, profile)); ks != wantProfile {
			t.Errorf("profile key set = %s, want %s", ks, wantProfile)
		}
		if profile["deliveryArea"] != "Accra" {
			t.Errorf("deliveryArea = %v", profile["deliveryArea"])
		}
		if profile["customerId"] != cust.ID {
			t.Errorf("profile.customerId = %v", profile["customerId"])
		}
		prefs, ok := profile["preferences"].([]any)
		if !ok || len(prefs) != 2 {
			t.Fatalf("preferences must decode as the stored JSON array, got %v (%T)", profile["preferences"], profile["preferences"])
		}
		ep_assertMoney(t, profile["averageOrderValue"], "51.01")
		ep_assertMoney(t, profile["totalSpent"], "204.04")
		if profile["orderFrequencyDays"] != json_Number("2.5") {
			t.Errorf("orderFrequencyDays = %v (%T)", profile["orderFrequencyDays"], profile["orderFrequencyDays"])
		}
		if profile["totalOrders"] != json_Number("4") {
			t.Errorf("totalOrders = %v", profile["totalOrders"])
		}
		if v, exists := profile["sentiment"]; !exists || v != "positive" {
			t.Errorf("sentiment = %v exists=%v", v, exists)
		}
		if v, exists := profile["lastOrderAt"]; !exists || v != nil {
			t.Errorf("lastOrderAt must be explicit null, got %v exists=%v", v, exists)
		}
	})

	t.Run("404 unknown id with filter envelope", func(t *testing.T) {
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers/nope", &claimsA)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		if body["statusCode"] != json_Number("404") || body["message"] != "Customer not found" {
			t.Fatalf("body = %v", body)
		}
		if body["path"] != "/customers/nope" {
			t.Errorf("path = %v", body["path"])
		}
		ep_assertISO(t, "timestamp", body["timestamp"])
	})

	t.Run("cross-tenant id behaves as 404", func(t *testing.T) {
		rec := ep_do(t, ep_mount(t, "/customers", NewCustomers(e.Pool)), http.MethodGet, "/customers/"+other.ID, &claimsA)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("cross-tenant fetch status=%d want 404 body=%s", rec.Code, rec.Body.String())
		}
	})
}
