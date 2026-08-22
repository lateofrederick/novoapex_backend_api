package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

// Live parity: the SAME seeded Postgres is served by the Node stack and by
// these handlers; stable fields must match exactly. Requires the Node dist
// build (skipped when absent — CI builds it).
func TestLiveParity_VendorReadsAgainstNodeStack(t *testing.T) {
	repoDir, err := harness.NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	for _, bin := range []string{
		filepath.Join(repoDir, "dist/apps/api/apps/api/src/main.js"),
	} {
		if _, err := os.Stat(bin); err != nil {
			t.Skipf("node dist missing (%s) — run npm run build:all first", bin)
		}
	}

	e := ep_startTest(t)

	bizA := e.F.Business()
	bizB := e.F.Business()
	custA := e.F.Customer(bizA.ID)
	custB := e.F.Customer(bizA.ID)
	e.F.Customer(bizB.ID)

	pImg := e.F.ProductWithImages(bizA.ID, 2, harness.WithStock(3), harness.WithPrice("10.50"))
	pPlain := e.F.Product(bizA.ID, harness.WithStock(0), harness.WithPrice("7.25"))
	e.F.Product(bizB.ID, harness.WithStock(50), harness.WithPrice("999.00"))

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	ep_setProductCreated(t, e, pImg.ID, base.Add(-2*time.Minute))
	ep_setProductCreated(t, e, pPlain.ID, base.Add(-1*time.Minute))
	ep_setCustomerLastContact(t, e, custA.ID, base)
	ep_setCustomerLastContact(t, e, custB.ID, base.Add(time.Minute))

	if _, err := e.DB.Exec(`UPDATE customer_profiles SET delivery_area = 'Accra',
		average_order_value = 51.01, total_orders = 4, total_spent = 204.04,
		preferences = '[["milo",2]]'::jsonb WHERE customer_id = $1`, custA.ID); err != nil {
		t.Fatalf("seed profile: %v", err)
	}

	ep_seedConversation(t, e, "lp_conv", bizA.ID, custA.ID, custA.Phone, "ESCALATED", true, "price_too_high", base)
	ep_seedConversation(t, e, "lp_conv2", bizA.ID, "", "+233611111111", "LEAD", false, "", base)
	ep_seedConversation(t, e, "lp_conv3", bizA.ID, "", "+233622222222", "SUPPORT", true, "price_too_high", base.Add(2*time.Minute))
	ep_seedConversation(t, e, "lp_convB", bizB.ID, "", "+233633333333", "LEAD", false, "", base)

	ep_seedOrder(t, e, "lp_o1", bizA.ID, custA.ID, "lp_conv", "PAID", "35.50", base)
	ep_seedOrderItem(t, e, "lp_oi1", "lp_o1", pImg.ID, pImg.Name, 2, "10.00")
	ep_seedOrderItem(t, e, "lp_oi2", "lp_o1", pPlain.ID, pPlain.Name, 1, "15.00")
	ep_seedOrder(t, e, "lp_o2", bizA.ID, custA.ID, "", "PAYMENT_PENDING", "12.25", base.Add(time.Minute))

	ep_seedPayment(t, e, "lp_pay1", bizA.ID, "lp_o1", "SUCCESS", "35.50",
		base.Add(5*time.Minute), base)
	ep_seedPayment(t, e, "lp_pay2", bizA.ID, "", "PENDING", "12.25", base, base)
	ep_seedPayout(t, e, "lp_po1", bizA.ID, "PENDING", "10.00", "lp-ref-1", base)

	ep_seedInboundMessage(t, e, "lp_in1", "lp_conv", bizA.ID, custA.Phone, "hello", base)
	ep_seedOutboundMessage(t, e, "lp_out1", "lp_conv", bizA.ID, custA.Phone, "hi!", "sent", base.Add(30*time.Second))
	ep_seedInboundMessage(t, e, "lp_in2", "lp_conv", bizA.ID, custA.Phone, "how much", base.Add(time.Minute))

	if _, err := e.DB.Exec(`UPDATE products SET request_count = 9 WHERE id = $1`, pImg.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.DB.Exec(`UPDATE products SET request_count = 7 WHERE id = $1`, pPlain.ID); err != nil {
		t.Fatal(err)
	}

	stack := harness.StartNodeStack(t, e.H)

	token := ep_mintToken(t, auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID})
	goRoot := func(pattern string, h http.Handler) http.Handler {
		return ep_mount(t, pattern, h)
	}
	mounted := map[string]http.Handler{
		"/customers":     goRoot("/customers", NewCustomers(e.Pool)),
		"/products":      goRoot("/products", NewProducts(e.Pool)),
		"/orders":        goRoot("/orders", NewOrders(e.Pool)),
		"/inbox":         goRoot("/inbox", NewInbox(e.Pool)),
		"/conversations": goRoot("/conversations", NewConversations(e.Pool)),
		"/dashboard":     goRoot("/dashboard", NewDashboard(e.Pool)),
		"/analytics":     goRoot("/analytics", NewAnalytics(e.Pool)),
		"/payouts":       goRoot("/payouts", NewPayouts(e.Pool)),
	}

	cases := []struct {
		path    string
		handler http.Handler
	}{
		{path: "/customers?page=1&limit=2", handler: mounted["/customers"]},
		{path: "/customers/summary", handler: mounted["/customers"]},
		{path: "/customers/" + custA.ID, handler: mounted["/customers"]},
		{path: "/products?limit=2", handler: mounted["/products"]},
		{path: "/products/metrics", handler: mounted["/products"]},
		{path: "/products/" + pImg.ID, handler: mounted["/products"]},
		{path: "/orders?limit=2", handler: mounted["/orders"]},
		{path: "/orders/summary", handler: mounted["/orders"]},
		{path: "/orders/lp_o1", handler: mounted["/orders"]},
		{path: "/inbox/summary", handler: mounted["/inbox"]},
		{path: "/inbox/handoffs?limit=5", handler: mounted["/inbox"]},
		{path: "/conversations/lp_conv/messages", handler: mounted["/conversations"]},
		{path: "/conversations/lp_conv/messages?limit=2", handler: mounted["/conversations"]},
		{path: "/dashboard/summary", handler: mounted["/dashboard"]},
		{path: "/analytics/overview", handler: mounted["/analytics"]},
		{path: "/analytics/handoff-reasons", handler: mounted["/analytics"]},
		{path: "/analytics/demand", handler: mounted["/analytics"]},
		{path: "/payouts/balance", handler: mounted["/payouts"]},
		{path: "/payouts/history?limit=5", handler: mounted["/payouts"]},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			nodeRec := ep_nodeGet(t, stack.BaseURL, tc.path, token)
			goRec := ep_do(t, tc.handler, http.MethodGet, tc.path,
				&auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID})

			if nodeRec.Status != goRec.Code {
				t.Fatalf("status: node=%d go=%d\nnode body: %s\ngo body: %s",
					nodeRec.Status, goRec.Code, nodeRec.Body, goRec.Body.String())
			}

			var nodeVal, goVal any
			decN := json.NewDecoder(bytes.NewReader(nodeRec.Body))
			decN.UseNumber()
			decG := json.NewDecoder(goRec.Body)
			decG.UseNumber()
			if err := decN.Decode(&nodeVal); err != nil {
				t.Fatalf("decode node: %v\n%s", err, nodeRec.Body)
			}
			if err := decG.Decode(&goVal); err != nil {
				t.Fatalf("decode go: %v\n%s", err, goRec.Body.String())
			}

			if !reflect.DeepEqual(goVal, nodeVal) {
				t.Errorf("body mismatch for %s\n--- go ---\n%s\n--- node ---\n%s",
					tc.path, ep_pretty(goVal), ep_pretty(nodeVal))
			}
		})
	}

	t.Run("GET / hello text parity", func(t *testing.T) {
		nodeRec := ep_nodeGet(t, stack.BaseURL, "/", token)
		raw := nodeRec.Body
		nodeStatus := nodeRec.Status
		goRec := ep_do(t, ep_withClaims(NewRoot()), http.MethodGet, "/", &auth.Claims{Phone: bizA.OwnerPhone})
		if nodeStatus != goRec.Code || string(raw) != goRec.Body.String() {
			t.Errorf("GET /: node=(%d,%q) go=(%d,%q)",
				nodeStatus, raw, goRec.Code, goRec.Body.String())
		}
	})
}

type epNodeResponse struct {
	Status int
	Body   []byte
}

func ep_nodeGet(t *testing.T, baseURL, path, token string) epNodeResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("node %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read node response: %v", err)
	}
	return epNodeResponse{Status: resp.StatusCode, Body: raw}
}

func ep_pretty(v any) string {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(out)
}
