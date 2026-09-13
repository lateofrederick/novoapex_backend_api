package observability

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/getsentry/sentry-go"
	"github.com/go-chi/chi/v5"
)

// healthPaths are never reported (sentry.filter.ts HEALTH_PATHS).
var healthPaths = []string{"/health", "/health/live", "/health/ready"}

// maxCapturedBody bounds the request body kept for error reports, like the
// Sentry Node SDK's 1 MiB request-body limit.
const maxCapturedBody = 1 << 20

// HTTPMiddleware is the API's Sentry instrumentation (instrument.ts's HTTP
// auto-instrumentation plus the global SentryFilter):
//
//   - every request runs on its own hub, with the request attached, so errors
//     and structured logs emitted while serving it carry the request and trace;
//   - sampled requests become "http.server" transactions named by their route
//     pattern ("GET /orders/{id}"), continuing an incoming sentry-trace/baggage,
//     with profiling when the transaction is picked for it;
//   - every exception raised while serving the request — handler errors, 4xx
//     HttpExceptions, auth guard and validation pipe rejections — is captured
//     with path/method tags, the request body as the "body" extra and the
//     X-User-Id user, except on health paths;
//   - panics are captured before the kernel's recoverer answers 500.
//
// Mount it only when Sentry is enabled.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub := sentry.GetHubFromContext(r.Context())
		if hub == nil {
			hub = sentry.CurrentHub().Clone()
		}
		ctx := sentry.SetHubOnContext(r.Context(), hub)

		body := captureRequestBody(r)
		hub.Scope().SetRequest(r)
		if len(body) > 0 {
			hub.Scope().SetRequestBody(body)
		}

		var tx *sentry.Span
		var prof *profileSession
		if tracingEnabled(hub) && r.Method != http.MethodOptions && r.Method != http.MethodHead {
			tx = sentry.StartTransaction(ctx, r.Method+" "+r.URL.Path,
				sentry.WithOpName("http.server"),
				sentry.ContinueFromRequest(r),
				sentry.WithTransactionSource(sentry.SourceURL),
			)
			tx.SetData("http.request.method", r.Method)
			ctx = tx.Context()
			prof = startProfile(tx)
		}

		sw := &sentryResponseWriter{ResponseWriter: w, status: http.StatusOK, hub: hub, r: r, body: body}
		r = r.WithContext(ctx)

		finish := func(status int) {
			if tx == nil {
				return
			}
			if pattern := routePattern(r); pattern != "" {
				tx.Name = r.Method + " " + pattern
				tx.Source = sentry.SourceRoute
			}
			tx.Status = sentry.HTTPtoSpanStatus(status)
			tx.SetData("http.response.status_code", status)
			prof.stop()
			tx.Finish()
		}
		defer func() {
			if rec := recover(); rec != nil {
				if !isHealthPath(r.URL.RequestURI()) {
					hub.WithScope(func(scope *sentry.Scope) {
						applyRequestScope(scope, r, body)
						hub.RecoverWithContext(ctx, rec)
					})
				}
				finish(http.StatusInternalServerError)
				panic(rec)
			}
		}()

		next.ServeHTTP(sw, r)
		finish(sw.status)
	})
}

func tracingEnabled(hub *sentry.Hub) bool {
	client := hub.Client()
	return client != nil && client.Options().EnableTracing
}

// routePattern is the matched chi route ("/orders/{id}"), or "" when no route
// matched.
func routePattern(r *http.Request) string {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return ""
	}
	return rctx.RoutePattern()
}

// sentryResponseWriter records the response status and receives the
// request's exceptions (errreport.Reporter).
type sentryResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool

	hub  *sentry.Hub
	r    *http.Request
	body []byte

	once sync.Once
}

func (w *sentryResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *sentryResponseWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(b)
}

func (w *sentryResponseWriter) Flush() {
	w.wroteHeader = true
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *sentryResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// ReportException captures the request's exception once (a guard rejection
// and the envelope that renders it are one exception).
func (w *sentryResponseWriter) ReportException(err error) {
	if err == nil || isHealthPath(w.r.URL.RequestURI()) {
		return
	}
	w.once.Do(func() {
		w.hub.WithScope(func(scope *sentry.Scope) {
			applyRequestScope(scope, w.r, w.body)
			w.hub.CaptureException(err)
		})
	})
}

// applyRequestScope mirrors SentryFilter's scope: path and method tags, the
// parsed request body as the "body" extra, and the X-User-Id user. sentry-go
// has no event extras, so the body travels in the "extra" context (scrubbed
// by BeforeSend like extras were).
func applyRequestScope(scope *sentry.Scope, r *http.Request, body []byte) {
	scope.SetTag("path", r.URL.RequestURI())
	scope.SetTag("method", r.Method)
	if uid := r.Header.Get("X-User-Id"); uid != "" {
		scope.SetUser(sentry.User{ID: uid})
	}
	scope.SetContext("extra", sentry.Context{"body": requestBodyExtra(r, body)})
	scope.AddEventProcessor(func(event *sentry.Event, hint *sentry.EventHint) *sentry.Event {
		nameException(event, hint)
		return event
	})
}

// nameException titles HTTP exceptions by their Nest class
// (UnauthorizedException, NotFoundException, ...) instead of the Go type.
func nameException(event *sentry.Event, hint *sentry.EventHint) {
	if hint == nil || hint.OriginalException == nil || len(event.Exception) == 0 {
		return
	}
	name := ""
	var named interface{ ExceptionName() string }
	var coded interface{ StatusCode() int }
	switch {
	case errors.As(hint.OriginalException, &named):
		name = named.ExceptionName()
	case errors.As(hint.OriginalException, &coded):
		name = nestExceptionName(coded.StatusCode())
	}
	if name != "" {
		event.Exception[len(event.Exception)-1].Type = name
	}
}

// nestExceptionName maps a status to the built-in Nest exception class that
// raises it.
func nestExceptionName(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "BadRequestException"
	case http.StatusUnauthorized:
		return "UnauthorizedException"
	case http.StatusForbidden:
		return "ForbiddenException"
	case http.StatusNotFound:
		return "NotFoundException"
	case http.StatusMethodNotAllowed:
		return "MethodNotAllowedException"
	case http.StatusNotAcceptable:
		return "NotAcceptableException"
	case http.StatusRequestTimeout:
		return "RequestTimeoutException"
	case http.StatusConflict:
		return "ConflictException"
	case http.StatusGone:
		return "GoneException"
	case http.StatusRequestEntityTooLarge:
		return "PayloadTooLargeException"
	case http.StatusUnsupportedMediaType:
		return "UnsupportedMediaTypeException"
	case http.StatusUnprocessableEntity:
		return "UnprocessableEntityException"
	case http.StatusTooManyRequests:
		return "ThrottlerException"
	case http.StatusInternalServerError:
		return "InternalServerErrorException"
	case http.StatusNotImplemented:
		return "NotImplementedException"
	case http.StatusBadGateway:
		return "BadGatewayException"
	case http.StatusServiceUnavailable:
		return "ServiceUnavailableException"
	case http.StatusGatewayTimeout:
		return "GatewayTimeoutException"
	}
	return "HttpException"
}

func isHealthPath(url string) bool {
	for _, p := range healthPaths {
		if url == p || strings.HasPrefix(url, p+"/") {
			return true
		}
	}
	return false
}

// captureRequestBody buffers a JSON or form body (what Express's body parsers
// turn into request.body) so it can be reported, and puts the bytes back for
// the handler. Other bodies (uploads) are left streaming.
func captureRequestBody(r *http.Request) []byte {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength > maxCapturedBody {
		return nil
	}
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaType != "application/json" && !strings.HasSuffix(mediaType, "+json") && mediaType != "application/x-www-form-urlencoded" {
		return nil
	}
	buf, err := io.ReadAll(io.LimitReader(r.Body, maxCapturedBody+1))
	r.Body = readCloser{Reader: io.MultiReader(bytes.NewReader(buf), r.Body), Closer: r.Body}
	if err != nil || len(buf) > maxCapturedBody {
		return nil
	}
	return buf
}

type readCloser struct {
	io.Reader
	io.Closer
}

// requestBodyExtra renders request.body: the parsed JSON or form fields, and
// {} when there is none (body-parser's default).
func requestBodyExtra(r *http.Request, body []byte) any {
	if len(bytes.TrimSpace(body)) == 0 {
		return map[string]any{}
	}
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaType == "application/x-www-form-urlencoded" {
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return map[string]any{}
		}
		fields := make(map[string]any, len(values))
		for k, v := range values {
			if len(v) == 1 {
				fields[k] = v[0]
			} else {
				fields[k] = v
			}
		}
		return fields
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return map[string]any{}
	}
	return decoded
}
