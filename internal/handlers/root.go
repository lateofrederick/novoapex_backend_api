package handlers

import (
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NewRoot ports MobileApiController.getHello
// (mobile-api.controller.ts + mobile-api.service.ts): GET / returns the bare
// string 'Hello World!'. Nest serialises returned strings via res.send, so
// the response is text/html — not JSON — with no trailing newline.
func NewRoot() http.Handler {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "Hello World!")
	})
	return r
}
