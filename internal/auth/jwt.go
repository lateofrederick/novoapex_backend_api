package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// Claims ports JwtPayload from apps/mobile-api/src/auth/jwt.strategy.ts:8-11
// ({phone, businessId?}). The strategy's DB re-derivation of businessId is a
// service-layer concern; this package only surfaces what the token itself
// carries.
type Claims struct {
	Phone      string
	BusinessID string
}

// jwtCtxKey unexported context key holding *Claims after successful
// authentication.
type jwtCtxKey struct{}

// ParseToken verifies an HS256 bearer token and extracts its claims. It
// mirrors apps/mobile-api/src/auth/jwt.strategy.ts:19-23:
//   - secretOrKey = JWT_SECRET, algorithm HS256 (passport-jwt default for a
//     string secret); any other alg is rejected outright;
//   - ignoreExpiration: false -> exp must be present and not passed. Unlike
//     jsonwebtoken (which silently accepts tokens without exp), the port
//     enforces exp presence per the Stage 2 contract ("exp enforced").
//
// Returned errors keep the golang-jwt/v5 sentinel chain intact so callers can
// discriminate with errors.Is: jwt.ErrTokenExpired vs jwt.ErrTokenMalformed vs
// jwt.ErrTokenSignatureInvalid etc.
func ParseToken(secret, raw string) (*Claims, error) {
	tok, err := jwt.Parse(
		raw,
		func(*jwt.Token) (any, error) { return []byte(secret), nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}

	mapClaims, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("auth: unsupported claims type %T", tok.Claims)
	}

	return &Claims{
		Phone:      jwt_stringClaim(mapClaims, "phone"),
		BusinessID: jwt_stringClaim(mapClaims, "businessId"),
	}, nil
}

// Middleware ports JwtAuthGuard (apps/mobile-api/src/auth/jwt-auth.guard.ts):
// default-deny with two opt-outs folded into isPublic — @Public-decorated
// handlers (reflector check, guard lines 20-27) and ALWAYS_PUBLIC_PATHS
// ['/metrics'] exact-path matches (lines 12, 29-32). On failure it answers
// with the guard's 401 body from handleRequest (line 39), written locally so
// this package does not depend on internal/httpx envelope symbols.
// On success the parsed Claims are stored in the request context.
func Middleware(secret string, isPublic func(*http.Request) bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isPublic != nil && isPublic(r) {
				next.ServeHTTP(w, r)
				return
			}

			claims, err := ParseToken(secret, jwt_bearerToken(r.Header.Get("Authorization")))
			if err != nil {
				jwt_unauthorized(w)
				return
			}

			ctx := context.WithValue(r.Context(), jwtCtxKey{}, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// FromContext returns the authenticated Claims stored by Middleware and
// whether they were present.
func FromContext(ctx context.Context) (Claims, bool) {
	claims, ok := ctx.Value(jwtCtxKey{}).(*Claims)
	if !ok || claims == nil {
		return Claims{}, false
	}
	return *claims, true
}

// jwt_bearerToken reproduces passport-jwt's
// ExtractJwt.fromAuthHeaderAsBearerToken (lib/extract-jwt.js +
// lib/auth_header.js): "Authorization" header parsed as (\S+)\s+(\S+) with a
// case-insensitive "bearer" scheme.
func jwt_bearerToken(header string) string {
	fields := strings.Fields(header)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "bearer") {
		return ""
	}
	return fields[1]
}

// jwt_unauthorized writes the guard's UnauthorizedException('Invalid or
// missing session token') body (jwt-auth.guard.ts:39).
//
// Delta vs Node: Nest's built-in serializer appends `"error":"Unauthorized"`
// to string-constructed HttpExceptions; the pinned Stage 2 contract emits
// statusCode+message only, which is what is implemented here.
func jwt_unauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)

	body := struct {
		StatusCode int    `json:"statusCode"`
		Message    string `json:"message"`
	}{
		StatusCode: http.StatusUnauthorized,
		Message:    "Invalid or missing session token",
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

func jwt_stringClaim(claims jwt.MapClaims, key string) string {
	s, _ := claims[key].(string)
	return s
}
