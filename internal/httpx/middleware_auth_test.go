package httpx_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

const maTestSecret = "test-secret"

func maToken(t *testing.T) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"phone": "+15550001111",
		"exp":   time.Now().Add(time.Hour).Unix(),
	})
	s, err := tok.SignedString([]byte(maTestSecret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func TestRequireAuth(t *testing.T) {
	h := httpx.RequireAuth(maTestSecret)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("deny all", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("/metrics without token under RequireAuth = %d, want 401 (deny-all)", rec.Code)
		}
	})

	t.Run("valid token passes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/products", nil)
		req.Header.Set("Authorization", "Bearer "+maToken(t))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
	})
}

// Exact-match parity with ALWAYS_PUBLIC_PATHS.includes(request.path).
func TestPublicPaths(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*http.Request)
		want  bool
	}{
		{"default /metrics exact match", func(r *http.Request) { r.URL.Path = "/metrics" }, true},
		{"prefix must NOT match", func(r *http.Request) { r.URL.Path = "/metrics/foo" }, false},
		{"other path denied", func(r *http.Request) { r.URL.Path = "/products" }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isPublic := httpx.PublicPaths() // no args -> ['/metrics'] default
			req := httptest.NewRequest(http.MethodGet, "/anything", nil)
			tc.setup(req)
			if got := isPublic(req); got != tc.want {
				t.Fatalf("PublicPaths()(%s) = %v, want %v", req.URL.Path, got, tc.want)
			}
		})
	}

	t.Run("explicit paths override default", func(t *testing.T) {
		isPublic := httpx.PublicPaths("/webhooks/whatsapp")
		req := httptest.NewRequest(http.MethodPost, "/webhooks/whatsapp", nil)
		if !isPublic(req) {
			t.Fatal("explicit path should be public")
		}
		req2 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		if isPublic(req2) {
			t.Fatal("default /metrics must not leak into explicit list")
		}
	})

	t.Run("query string ignored for path match", func(t *testing.T) {
		isPublic := httpx.PublicPaths("/health")
		req := httptest.NewRequest(http.MethodGet, "/health?verbose=1", nil)
		if !isPublic(req) {
			t.Fatal("r.URL.Path excludes query string")
		}
	})
}
