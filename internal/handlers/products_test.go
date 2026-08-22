package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

// T2.13 GET /products (incl images)
func TestProductsList(t *testing.T) {
	e := ep_startTest(t)
	bizA := e.F.Business()
	bizB := e.F.Business()

	pOld := e.F.ProductWithImages(bizA.ID, 3)
	pNew := e.F.Product(bizA.ID)
	pOther := e.F.Product(bizB.ID)

	base := time.Now().UTC()
	ep_setProductCreated(t, e, pOld.ID, base.Add(-2*time.Hour))
	ep_setProductCreated(t, e, pNew.ID, base.Add(-1*time.Hour))

	claimsA := auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID}
	h := ep_mount(t, "/products", NewProducts(e.Pool))

	t.Run("shape images ordering legacy imageUrl", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/products?page=1&limit=1", &claimsA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)

		data := body["data"].([]any)
		if len(data) != 1 {
			t.Fatalf("page size = %d", len(data))
		}
		first := data[0].(map[string]any)
		if first["id"] != pNew.ID {
			t.Fatalf("orderBy createdAt desc broken: %v want %s", first["id"], pNew.ID)
		}

		wantKeys := "[businessId category createdAt deliveryNote description id imageUrl images isAvailable name price requestCount sku stock stockNote updatedAt]"
		if ks := fmt.Sprint(ep_keys(t, first)); ks != wantKeys {
			t.Errorf("product key set = %s\nwant        %s", ks, wantKeys)
		}
		if _, has := first["embedding"]; has {
			t.Error("product must NOT carry an embedding key (Prisma Unsupported field)")
		}

		// No-images product: empty array + null legacy url.
		if imgs, ok := first["images"].([]any); !ok || len(imgs) != 0 {
			t.Fatalf("images = %v (%T), want []", first["images"], first["images"])
		}
		if v, exists := first["imageUrl"]; !exists || v != nil {
			t.Fatalf("imageUrl must be an explicit null key, got exists=%v v=%v", exists, v)
		}

		meta := body["meta"].(map[string]any)
		if meta["total"] != json_Number("2") || meta["totalPages"] != json_Number("2") {
			t.Errorf("meta = %v", meta)
		}
	})

	t.Run("images ordered by position asc with money price", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/products?page=2&limit=1", &claimsA)
		body := ep_decode(t, rec)
		row := body["data"].([]any)[0].(map[string]any)
		if row["id"] != pOld.ID {
			t.Fatalf("page2 id=%v want %s", row["id"], pOld.ID)
		}

		imgs := row["images"].([]any)
		if len(imgs) != 3 {
			t.Fatalf("images len = %d", len(imgs))
		}
		for i, raw := range imgs {
			img := raw.(map[string]any)
			wantImgKeys := "[contentHash createdAt id position productId storageKey url]"
			if ks := fmt.Sprint(ep_keys(t, img)); ks != wantImgKeys {
				t.Errorf("image key set = %s, want %s", ks, wantImgKeys)
			}
			if _, has := img["embedding"]; has {
				t.Error("image must NOT carry an embedding key")
			}
			if img["position"] != json_Number(fmt.Sprint(i)) {
				t.Errorf("image[%d] position = %v, want %d (orderBy position asc)", i, img["position"], i)
			}
			if img["productId"] != pOld.ID {
				t.Errorf("image productId = %v", img["productId"])
			}
			if _, isStr := img["url"].(string); !isStr {
				t.Errorf("image[%d] url not a string: %v", i, img["url"])
			}
			if v, exists := img["storageKey"]; !exists || v != nil {
				t.Errorf("fixture storageKey should be explicit null, got exists=%v v=%v", exists, v)
			}
		}
		firstURL := imgs[0].(map[string]any)["url"].(string)
		if row["imageUrl"] != firstURL {
			t.Errorf("legacy imageUrl = %v, want primary image url %s", row["imageUrl"], firstURL)
		}
		ep_assertMoney(t, row["price"], "25.50") // fixture default; bare JSON number
	})

	t.Run("scoping and empty list rendering", func(t *testing.T) {
		rec := ep_do(t, h, http.MethodGet, "/products?limit=100", &claimsA)
		for _, raw := range ep_decode(t, rec)["data"].([]any) {
			if raw.(map[string]any)["id"] == pOther.ID {
				t.Fatal("business B product leaked into business A list")
			}
		}

		bizC := e.F.Business()
		empty := ep_do(t, h, http.MethodGet, "/products",
			&auth.Claims{Phone: bizC.OwnerPhone, BusinessID: bizC.ID})
		if !strings.Contains(empty.Body.String(), `"data":[]`) {
			t.Fatalf("empty list must render [], got %s", empty.Body.String())
		}
	})
}

// T2.14 GET /products/metrics
func TestProductsMetrics(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	m1 := e.F.Product(biz.ID, harness.WithStock(3), harness.WithPrice("10.50"))
	e.F.Product(biz.ID, harness.WithStock(0), harness.WithPrice("7.25")) // out of stock
	m3 := e.F.Product(biz.ID, harness.WithStock(2), harness.WithPrice("100.00"))
	e.F.Product(e.F.Business().ID, harness.WithStock(50), harness.WithPrice("999.00")) // other tenant

	rec := ep_do(t, NewProducts(e.Pool), http.MethodGet, "/metrics",
		&auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
	_ = m1
	_ = m3
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := ep_decode(t, rec)
	if ks := fmt.Sprint(ep_keys(t, body)); ks != "[outOfStock totalInventoryItems totalInventoryValue totalProducts]" {
		t.Fatalf("key set = %s", ks)
	}
	if body["totalProducts"] != json_Number("3") {
		t.Errorf("totalProducts = %v", body["totalProducts"])
	}
	if body["outOfStock"] != json_Number("1") { // stock <= 0
		t.Errorf("outOfStock = %v", body["outOfStock"])
	}
	if body["totalInventoryItems"] != json_Number("5") { // sum(stock)
		t.Errorf("totalInventoryItems = %v", body["totalInventoryItems"])
	}
	ep_assertMoney(t, body["totalInventoryValue"], "231.5") // sum(price*stock): 31.5+0+200
}

// T2.15 GET /products/:id
func TestProductsFindOne(t *testing.T) {
	e := ep_startTest(t)
	bizA := e.F.Business()
	bizB := e.F.Business()
	p := e.F.ProductWithImages(bizA.ID, 2)
	e.F.Product(bizB.ID)

	claimsA := auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID}
	h := ep_mount(t, "/products", NewProducts(e.Pool))

	rec := ep_do(t, h, http.MethodGet, "/products/"+p.ID, &claimsA)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := ep_decode(t, rec)
	if body["id"] != p.ID {
		t.Fatalf("id = %v", body["id"])
	}
	if len(body["images"].([]any)) != 2 {
		t.Fatalf("images = %v", body["images"])
	}

	notFound := ep_do(t, h, http.MethodGet, "/products/does-not-exist", &claimsA)
	if notFound.Code != http.StatusNotFound {
		t.Fatalf("unknown id status=%d", notFound.Code)
	}
	nf := ep_decode(t, notFound)
	if nf["message"] != "Product not found" || nf["statusCode"] != json_Number("404") {
		t.Fatalf("404 body = %v", nf)
	}

	cross := ep_do(t, h, http.MethodGet, "/products/"+p.ID,
		&auth.Claims{Phone: bizB.OwnerPhone, BusinessID: bizB.ID})
	if cross.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant fetch status=%d want 404", cross.Code)
	}
}
