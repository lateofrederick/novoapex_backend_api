package auth_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

func s3DecodePayload(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token must have 3 segments, got %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	return payload
}

func TestS3_Mint_RoundTripThroughParseToken(t *testing.T) {
	const secret = "s3-mint-secret"

	token, err := auth.Mint(secret, auth.Claims{Phone: "+233200000010", BusinessID: "biz_1"})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	claims, err := auth.ParseToken(secret, token)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if claims.Phone != "+233200000010" || claims.BusinessID != "biz_1" {
		t.Fatalf("claims mismatch: %+v", claims)
	}

	// alg must be HS256.
	header := strings.Split(token, ".")[0]
	rawHdr, _ := base64.RawURLEncoding.DecodeString(header)
	if !strings.Contains(string(rawHdr), `"HS256"`) {
		t.Fatalf("alg header missing HS256: %s", rawHdr)
	}
}

func TestS3_Mint_ClaimNamesAndTTLMatchNode(t *testing.T) {
	token, err := auth.Mint("k", auth.Claims{Phone: "+233200000011", BusinessID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	payload := s3DecodePayload(t, token)

	if payload["phone"] != "+233200000011" || payload["businessId"] != "b" {
		t.Fatalf("claim names/values mismatch: %v", payload)
	}
	exp, ok := payload["exp"].(float64)
	if !ok {
		t.Fatalf("exp claim missing or non-numeric: %v", payload["exp"])
	}
	iat, _ := payload["iat"].(float64)
	delta := time.Duration(exp-iat) * time.Second
	if delta != auth.TokenTTL {
		t.Fatalf("exp-iat = %s, want TokenTTL (%s)", delta, auth.TokenTTL)
	}
	if delta != 7*24*time.Hour {
		t.Fatalf("TokenTTL must be 7d like signOptions expiresIn:'7d', got %s", delta)
	}
}

// jsonwebtoken drops undefined properties, so AuthService.verifyOtp's
// {phone, businessId: undefined} payload serialises WITHOUT businessId.
func TestS3_Mint_OmitsBusinessIdWhenAbsent(t *testing.T) {
	token, err := auth.Mint("k", auth.Claims{Phone: "+233200000012"})
	if err != nil {
		t.Fatal(err)
	}
	payload := s3DecodePayload(t, token)
	if _, present := payload["businessId"]; present {
		t.Fatalf("businessId must be omitted for business-less principals: %v", payload)
	}
	if _, present := payload["iat"]; !present {
		t.Fatalf("jsonwebtoken always stamps iat; Go Mint should too: %v", payload)
	}

	// ParseToken still yields empty BusinessID.
	claims, err := auth.ParseToken("k", token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.BusinessID != "" {
		t.Fatalf("BusinessID = %q, want empty", claims.BusinessID)
	}
}
