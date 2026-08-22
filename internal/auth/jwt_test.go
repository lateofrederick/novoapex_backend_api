package auth_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

const jwtTestSecret = "test-secret"

func jwtSign(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func jwtValidClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"phone":      "+15550001111",
		"businessId": "biz_123",
		"exp":        time.Now().Add(time.Hour).Unix(),
	}
}

func TestParseToken_RoundTrip(t *testing.T) {
	raw := jwtSign(t, jwtTestSecret, jwtValidClaims())

	got, err := auth.ParseToken(jwtTestSecret, raw)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if got.Phone != "+15550001111" || got.BusinessID != "biz_123" {
		t.Fatalf("claims mismatch: %+v", got)
	}
}

func TestParseToken_OptionalBusinessID(t *testing.T) {
	raw := jwtSign(t, jwtTestSecret, jwt.MapClaims{
		"phone": "+15550002222",
		"exp":   time.Now().Add(time.Hour).Unix(),
	})

	got, err := auth.ParseToken(jwtTestSecret, raw)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if got.Phone != "+15550002222" || got.BusinessID != "" {
		t.Fatalf("claims mismatch: %+v", got)
	}
}

func TestParseToken_Rejections(t *testing.T) {
	expired := jwtSign(t, jwtTestSecret, func() jwt.MapClaims {
		c := jwtValidClaims()
		c["exp"] = time.Now().Add(-time.Minute).Unix()
		return c
	}())
	wrongSig := jwtSign(t, "other-secret", jwtValidClaims())
	wrongAlg := jwt.NewWithClaims(jwt.SigningMethodNone, jwtValidClaims())
	noneTok, err := wrongAlg.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none: %v", err)
	}

	cases := []struct {
		name string
		raw  string
		want error
	}{
		{"expired", expired, jwt.ErrTokenExpired},
		{"garbage", "not-a-jwt", jwt.ErrTokenMalformed},
		{"wrong signature", wrongSig, jwt.ErrTokenSignatureInvalid},
		{"alg none rejected", noneTok, jwt.ErrTokenSignatureInvalid}, // WithValidMethods -> not HS256
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := auth.ParseToken(jwtTestSecret, tc.raw)
			if got != nil || !errors.Is(err, tc.want) {
				t.Fatalf("ParseToken()=(%v, %v), want distinct error matching %v", got, err, tc.want)
			}
		})
	}

	// The three primary failure classes must be mutually distinguishable.
	e1 := funcErr(t, expired)
	e2 := funcErr(t, "not-a-jwt")
	e3 := funcErr(t, wrongSig)
	if errors.Is(e1, e2) || errors.Is(e2, e3) || errors.Is(e1, e3) {
		t.Fatal("failure classes are not distinct")
	}
}

func funcErr(t *testing.T, raw string) error {
	t.Helper()
	_, err := auth.ParseToken(jwtTestSecret, raw)
	if err == nil {
		t.Fatal("expected error")
	}
	return err
}

func TestMiddleware_Behaviour(t *testing.T) {
	newStack := func(isPublic func(*http.Request) bool) (http.Handler, **auth.Claims) {
		var captured *auth.Claims
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if c, ok := auth.FromContext(r.Context()); ok {
				cc := c
				captured = &cc
			}
			w.WriteHeader(http.StatusOK)
		})
		return auth.Middleware(jwtTestSecret, isPublic)(next), &captured
	}

	valid := jwtSign(t, jwtTestSecret, jwtValidClaims())

	t.Run("missing header -> pinned 401 body", func(t *testing.T) {
		h, _ := newStack(nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/products", nil))

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("content-type = %q", ct)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v (%s)", err, rec.Body.String())
		}
		if len(body) != 2 || body["statusCode"] != float64(401) ||
			body["message"] != "Invalid or missing session token" {
			t.Fatalf("body shape mismatch: %v", body)
		}
	})

	t.Run("garbage token -> 401", func(t *testing.T) {
		h, _ := newStack(nil)
		req := httptest.NewRequest(http.MethodGet, "/products", nil)
		req.Header.Set("Authorization", "Bearer garbage")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("valid bearer (case-insensitive scheme) -> claims in context", func(t *testing.T) {
		h, captured := newStack(nil)
		req := httptest.NewRequest(http.MethodGet, "/products", nil)
		req.Header.Set("Authorization", "bearer "+valid)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
		}
		if *captured == nil || (*captured).Phone != "+15550001111" || (*captured).BusinessID != "biz_123" {
			t.Fatalf("FromContext claims mismatch: %+v", captured)
		}
	})

	t.Run("public path bypasses auth even with bad token", func(t *testing.T) {
		h, _ := newStack(func(*http.Request) bool { return true })
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		req.Header.Set("Authorization", "Bearer garbage")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want public passthrough", rec.Code)
		}
	})
}

func TestFromContext_Absent(t *testing.T) {
	if _, ok := auth.FromContext(t.Context()); ok {
		t.Fatal("want absent on plain context")
	}
}
