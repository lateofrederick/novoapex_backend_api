package httpx

import (
	"net/http"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// RequireAuth composes auth.Middleware with no public-path opt-outs for
// chi-style mounting: r.Route("/products", httpx.RequireAuth(secret).(...)).
func RequireAuth(secret string) func(http.Handler) http.Handler {
	return auth.Middleware(secret, func(*http.Request) bool { return false })
}

// PublicPaths builds the isPublic callback for auth.Middleware from path
// literals, mirroring ALWAYS_PUBLIC_PATHS in
// apps/mobile-api/src/auth/jwt-auth.guard.ts:12,29-32 — an EXACT
// request.path match (`ALWAYS_PUBLIC_PATHS.includes(request.path)`), not a
// prefix match. With no arguments it defaults to ['/metrics'], matching the
// guard's always-open infrastructure route.
func PublicPaths(paths ...string) func(*http.Request) bool {
	if len(paths) == 0 {
		paths = []string{"/metrics"}
	}

	set := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		set[p] = struct{}{}
	}

	return func(r *http.Request) bool {
		_, ok := set[r.URL.Path]
		return ok
	}
}
