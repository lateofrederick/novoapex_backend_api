package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// SentryHook mirrors the DSN-gated global SentryFilter registration in
// apps/api/src/main.ts:32-37. When non-nil it is invoked for captured
// errors, except on health probe paths (sentry.filter.ts HEALTH_PATHS).
var SentryHook func(r *http.Request, err error)

// envHealthPaths reproduces libs/observability/src/sentry.filter.ts:11.
var envHealthPaths = []string{"/health", "/health/live", "/health/ready"}

// HTTPException ports NestJS HttpException: an HTTP status plus the exact
// payload HttpException.getResponse() would return — a string, or an
// arbitrary JSON value (object with message/error/statusCode keys, arrays
// of validation messages, Terminus health results, ...).
type HTTPException struct {
	Status  int
	Payload any
}

func NewHTTPException(status int, payload any) *HTTPException {
	return &HTTPException{Status: status, Payload: payload}
}

func (e *HTTPException) Error() string {
	if s, ok := e.Payload.(string); ok {
		return s
	}
	return fmt.Sprintf("http exception: status=%d", e.Status)
}

// StatusCode lets generic handlers detect the HTTP status without knowing
// the concrete error type.
func (e *HTTPException) StatusCode() int { return e.Status }

// StatusCoder is satisfied by foreign error types carrying an HTTP status.
type StatusCoder interface {
	StatusCode() int
}

// WriteError ports AllExceptionsFilter / SentryFilter. Both produce the
// identical envelope:
//
//	{"statusCode":N,"timestamp":"<ISO8601>","path":"<request.url>","message":<getResponse()>}
//
// HttpException -> its status and response verbatim; anything else ->
// 500 with "Internal server error".
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	message := any("Internal server error")

	var he *HTTPException
	if errors.As(err, &he) {
		status = he.Status
		message = he.Payload
	} else {
		var sc StatusCoder
		if errors.As(err, &sc) {
			status = sc.StatusCode()
			message = err.Error()
		}
	}

	env_respond(w, r, err, status, message)
}

// env_respond reproduces the shared filter side effects in order:
// optional Sentry capture (SentryFilter only), an error log line
// (AllExceptionsFilter logger.error), then the JSON envelope.
func env_respond(w http.ResponseWriter, r *http.Request, err error, status int, message any) {
	url := env_requestURL(r)

	if SentryHook != nil && !env_isHealthPath(url) {
		SentryHook(r, err)
	}

	slog.Error(fmt.Sprintf("%s %s %d - %s", r.Method, url, status, env_jsonString(message)))

	body := struct {
		StatusCode int    `json:"statusCode"`
		Timestamp  string `json:"timestamp"`
		Path       string `json:"path"`
		Message    any    `json:"message"`
	}{
		StatusCode: status,
		Timestamp:  time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		Path:       url,
		Message:    message,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

func env_isHealthPath(url string) bool {
	for _, p := range envHealthPaths {
		if url == p || strings.HasPrefix(url, p+"/") {
			return true
		}
	}
	return false
}

func env_requestURL(r *http.Request) string {
	return r.URL.RequestURI()
}

func env_jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
