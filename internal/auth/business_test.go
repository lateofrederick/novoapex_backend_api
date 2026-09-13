package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolveBusiness_DerivesBusinessFromDatabaseNotToken(t *testing.T) {
	const secret = "0123456789abcdef0123456789abcdef"
	owners := map[string]string{"+233200000001": "biz-live"}
	lookup := func(_ context.Context, phone string) (string, error) {
		if phone == "+233200000666" {
			return "", errors.New("db down")
		}
		return owners[phone], nil
	}

	var seen Claims
	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, _ = FromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})
	var errStatus int
	onError := func(w http.ResponseWriter, _ *http.Request, _ error) {
		errStatus = http.StatusInternalServerError
		w.WriteHeader(errStatus)
	}
	h := Middleware(secret, nil)(ResolveBusiness(lookup, onError)(final))

	call := func(c Claims) int {
		token, err := Mint(secret, c)
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		req := httptest.NewRequest(http.MethodGet, "/orders", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	// Token minted at verify-otp before onboarding carries no businessId; the
	// business created afterwards must be visible without re-login.
	if code := call(Claims{Phone: "+233200000001"}); code != http.StatusNoContent || seen.BusinessID != "biz-live" {
		t.Fatalf("pre-onboarding token: code=%d businessId=%q, want biz-live", code, seen.BusinessID)
	}

	// A stale or foreign claim never wins over the database.
	if code := call(Claims{Phone: "+233200000999", BusinessID: "someone-elses-biz"}); code != http.StatusNoContent || seen.BusinessID != "" {
		t.Fatalf("stale claim: code=%d businessId=%q, want empty", code, seen.BusinessID)
	}

	if code := call(Claims{Phone: "+233200000666"}); code != http.StatusInternalServerError {
		t.Fatalf("lookup failure: code=%d, want 500", code)
	}
}
