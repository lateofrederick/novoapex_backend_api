package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"

	"github.com/novoapex/novoapex-backend-api/internal/apidocs"
	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
)

func testRouter(t *testing.T, mutate func(*config.Config), mobileOnly bool) http.Handler {
	t.Helper()
	cfg := &config.Config{
		NodeEnv:           "development",
		JWTSecret:         "0123456789abcdef0123456789abcdef",
		BullBoardUser:     "admin",
		BullBoardPassword: "s3cret",
	}
	if mutate != nil {
		mutate(cfg)
	}
	kernel, closer, err := buildRouter(routerDeps{
		cfg:        cfg,
		queueRedis: asynq.RedisClientOpt{Addr: "127.0.0.1:1"},
		wa:         whatsapp.New("v25.0", "token"),
		mobileOnly: mobileOnly,
	})
	if err != nil {
		t.Fatalf("buildRouter: %v", err)
	}
	t.Cleanup(func() { _ = closer() })
	return kernel.Router
}

// routes lists "METHOD /path" for every registered route, excluding the
// generated UIs (API docs, queue dashboard) that are not part of the API.
func routes(t *testing.T, h http.Handler) []string {
	t.Helper()
	seen := map[string]bool{}
	err := chi.Walk(h.(chi.Router), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		route = strings.ReplaceAll(route, "/*/", "/")
		for strings.Contains(route, "//") {
			route = strings.ReplaceAll(route, "//", "/")
		}
		if len(route) > 1 {
			route = strings.TrimSuffix(route, "/")
		}
		if strings.HasPrefix(route, apidocs.UIPath) || strings.HasPrefix(route, httpx.QueueDashboardPath) {
			return nil
		}
		seen[method+" "+route] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	out := make([]string, 0, len(seen))
	for r := range seen {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

func TestOpenAPIDocumentsExactlyTheLiveRoutes(t *testing.T) {
	live := routes(t, testRouter(t, nil, false))
	documented := apidocs.Routes()
	sort.Strings(documented)

	liveSet, docSet := map[string]bool{}, map[string]bool{}
	for _, r := range live {
		liveSet[r] = true
	}
	for _, r := range documented {
		if docSet[r] {
			t.Errorf("route documented twice: %s", r)
		}
		docSet[r] = true
	}
	for _, r := range live {
		if !docSet[r] {
			t.Errorf("route served but missing from the OpenAPI document: %s", r)
		}
	}
	for _, r := range documented {
		if !liveSet[r] {
			t.Errorf("route documented but not served: %s", r)
		}
	}
}

func TestOpenAPIDocumentReferencesResolve(t *testing.T) {
	raw, err := json.Marshal(apidocs.Spec(apidocs.Options{PaymentProviders: []string{"paystack"}}))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Components struct {
			Schemas map[string]json.RawMessage `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, m := range strings.Split(string(raw), `"$ref":"#/components/schemas/`)[1:] {
		name := m[:strings.IndexByte(m, '"')]
		if _, ok := doc.Components.Schemas[name]; !ok {
			t.Errorf("unresolved $ref %s", name)
		}
	}
}

func TestAPIDocsServedOutsideProductionOnly(t *testing.T) {
	get := func(h http.Handler, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}

	dev := testRouter(t, nil, false)
	if rec := get(dev, "/api-docs"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "swagger-ui") {
		t.Fatalf("GET /api-docs = %d", rec.Code)
	}
	for _, asset := range []string{"/api-docs/swagger-ui.css", "/api-docs/swagger-ui-bundle.js", "/api-docs/swagger-ui-standalone-preset.js"} {
		if rec := get(dev, asset); rec.Code != 200 || rec.Body.Len() < 1000 {
			t.Errorf("GET %s = %d (%d bytes)", asset, rec.Code, rec.Body.Len())
		}
	}
	rec := get(dev, "/api-docs-json")
	var doc map[string]any
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &doc) != nil || doc["openapi"] != "3.0.0" {
		t.Fatalf("GET /api-docs-json = %d %.200s", rec.Code, rec.Body.String())
	}
	if rec := get(dev, "/api-docs-yaml"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "openapi: 3.0.0") {
		t.Errorf("GET /api-docs-yaml = %d", rec.Code)
	}

	prod := testRouter(t, func(c *config.Config) { c.NodeEnv = "production" }, false)
	if rec := get(prod, "/api-docs-json"); rec.Code != 404 {
		t.Errorf("production without ENABLE_SWAGGER: GET /api-docs-json = %d, want 404", rec.Code)
	}
	staging := testRouter(t, func(c *config.Config) { c.NodeEnv = "production"; c.EnableSwagger = true }, false)
	if rec := get(staging, "/api-docs-json"); rec.Code != 200 {
		t.Errorf("production with ENABLE_SWAGGER: GET /api-docs-json = %d, want 200", rec.Code)
	}
}

func TestQueueDashboardBasicAuthFailsClosed(t *testing.T) {
	do := func(h http.Handler, user, pass string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/admin/queues", nil)
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	h := testRouter(t, nil, false)
	if rec := do(h, "", ""); rec.Code != 401 || rec.Header().Get("WWW-Authenticate") != `Basic realm="NovoApex Queue Dashboard"` {
		t.Errorf("no credentials: %d %q", rec.Code, rec.Header().Get("WWW-Authenticate"))
	}
	if rec := do(h, "admin", "wrong"); rec.Code != 401 {
		t.Errorf("wrong password: %d, want 401", rec.Code)
	}
	page := do(h, "admin", "s3cret")
	if page.Code != 200 || !strings.Contains(strings.ToLower(page.Body.String()), "<html") {
		t.Fatalf("valid credentials: %d, want the dashboard page", page.Code)
	}
	// The page's script and stylesheet bundles must resolve under the mount
	// point, or the dashboard renders blank.
	assets := regexp.MustCompile(`(?:src|href)="(/admin/queues/static/[^"]+\.(?:js|css))"`).FindAllStringSubmatch(page.Body.String(), -1)
	if len(assets) == 0 {
		t.Fatalf("dashboard page references no bundles under /admin/queues/static:\n%.600s", page.Body.String())
	}
	for _, m := range assets {
		req := httptest.NewRequest(http.MethodGet, m[1], nil)
		req.SetBasicAuth("admin", "s3cret")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 200 || rec.Body.Len() == 0 {
			t.Errorf("dashboard asset %s = %d (%d bytes)", m[1], rec.Code, rec.Body.Len())
		}
	}

	unset := testRouter(t, func(c *config.Config) { c.BullBoardUser, c.BullBoardPassword = "", "" }, false)
	if rec := do(unset, "admin", "s3cret"); rec.Code != 503 {
		t.Errorf("credentials unconfigured: %d, want 503 (fail closed)", rec.Code)
	}
}

func TestMobileOnlyServesJustTheMobileAPI(t *testing.T) {
	got := routes(t, testRouter(t, nil, true))
	for _, r := range got {
		for _, excluded := range []string{"/webhooks/", "/messages/"} {
			if strings.Contains(r, excluded) {
				t.Errorf("mobile-only router serves %s", r)
			}
		}
	}
	has := func(want string) bool {
		for _, r := range got {
			if r == want {
				return true
			}
		}
		return false
	}
	for _, want := range []string{"POST /auth/verify-otp", "GET /products", "PATCH /orders/{id}/fulfillment", "GET /payouts/balance"} {
		if !has(want) {
			t.Errorf("mobile-only router is missing %s", want)
		}
	}
	h := testRouter(t, nil, true)
	for _, path := range []string{"/api-docs-json", "/admin/queues"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != 404 {
			t.Errorf("mobile-only GET %s = %d, want 404", path, rec.Code)
		}
	}
}
