package auth

import (
	"context"
	"net/http"
)

// BusinessLookup resolves the business owned by phone; "" means none exists.
type BusinessLookup func(ctx context.Context, phone string) (businessID string, err error)

// ResolveBusiness ports JwtStrategy.validate (jwt.strategy.ts): after the
// token is verified, businessId is re-derived from the database by the
// authenticated phone instead of trusting the claim baked into the 7-day
// token. That makes a token minted before onboarding (isNewUser, no
// businessId) valid for business routes as soon as the business is created,
// and stops a stale claim from outliving the business it named.
//
// Must run after Middleware. A lookup error is handed to onError (the API
// renders it through the standard error envelope).
func ResolveBusiness(lookup BusinessLookup, onError func(http.ResponseWriter, *http.Request, error)) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := FromContext(r.Context())
			if !ok {
				next.ServeHTTP(w, r)
				return
			}
			businessID, err := lookup(r.Context(), claims.Phone)
			if err != nil {
				onError(w, r, err)
				return
			}
			claims.BusinessID = businessID
			ctx := context.WithValue(r.Context(), jwtCtxKey{}, &claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
