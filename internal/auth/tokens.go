package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TokenTTL mirrors AuthModule's signOptions {expiresIn: '7d'}
// (apps/mobile-api/src/auth/auth.module.ts:23): mobile sessions last 7 days.
const TokenTTL = 7 * 24 * time.Hour

// Mint ports AuthService.verifyOtp's jwtService.sign(payload)
// (auth.service.ts:61-66). The payload carries the same claim names
// ParseToken reads ({phone, businessId?}); jsonwebtoken drops undefined
// properties when serialising, so a business-less principal omits businessId
// entirely rather than emitting an empty string. jsonwebtoken also always
// stamps iat unless disabled, so it is included here to keep Go-minted tokens
// structurally indistinguishable from Node-minted ones.
func Mint(secret string, claims Claims) (string, error) {
	now := time.Now()
	mc := jwt.MapClaims{
		"phone": claims.Phone,
		"iat":   now.Unix(),
	}
	if claims.BusinessID != "" {
		mc["businessId"] = claims.BusinessID
	}
	mc["exp"] = now.Add(TokenTTL).Unix()

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, mc).SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("auth: mint token: %w", err)
	}
	return signed, nil
}
