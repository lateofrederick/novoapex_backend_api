package httpx

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"testing"
)

func envTestRequest(t *testing.T, url string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	WriteError(w, r, nil)
	return w
}

func envDecode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode body %q: %v", body, err)
	}
	return out
}

var envTimestampRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)

func TestWriteError_UnknownError500Shape(t *testing.T) {
	// AllExceptionsFilter: non-HttpException -> 500 + 'Internal server error'.
	w := envTestRequest(t, "/anything")
	if w.Code != 500 {
		t.Fatalf("code = %d, want 500", w.Code)
	}
	got := envDecode(t, w.Body.Bytes())
	want := map[string]any{
		"statusCode": float64(500),
		"path":       "/anything",
		"message":    "Internal server error",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("body[%q] = %#v, want %#v", k, got[k], v)
		}
	}
	ts, _ := got["timestamp"].(string)
	if !envTimestampRe.MatchString(ts) {
		t.Errorf("timestamp = %q, want ISO8601 with ms + Z", ts)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q", ct)
	}
}

func TestWriteError_HTTPExceptionTable(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		status  int
		message any
	}{
		{
			// new NotFoundException() -> {message:'Not Found', statusCode:404}
			name:    "builtin string response",
			err:     NewHTTPException(404, "Not Found"),
			status:  404,
			message: "Not Found",
		},
		{
			// new UnauthorizedException('custom') -> string message form
			name:    "custom string",
			err:     NewHTTPException(401, "custom message"),
			status:  401,
			message: "custom message",
		},
		{
			// ValidationPipe-style array message form
			name:   "array message object",
			status: 400,
			err: NewHTTPException(400, map[string]any{
				"statusCode": 400,
				"message":    []any{"email must be an email", "name should not be empty"},
				"error":      "Bad Request",
			}),
			message: map[string]any{
				"statusCode": float64(400),
				"message":    []any{"email must be an email", "name should not be empty"},
				"error":      "Bad Request",
			},
		},
		{
			name:   "arbitrary payload object verbatim",
			status: 418,
			err:    NewHTTPException(418, map[string]any{"foo": "bar"}),
			message: map[string]any{
				"foo": "bar",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/items?x=1&y=2", nil)
			w := httptest.NewRecorder()
			WriteError(w, r, tt.err)

			if w.Code != tt.status {
				t.Fatalf("code = %d, want %d", w.Code, tt.status)
			}
			got := envDecode(t, w.Body.Bytes())
			if got["statusCode"] != float64(tt.status) {
				t.Errorf("statusCode = %v, want %d", got["statusCode"], tt.status)
			}
			if got["path"] != "/items?x=1&y=2" {
				t.Errorf("path = %v, want query-inclusive request URL", got["path"])
			}
			if !envTimestampRe.MatchString(got["timestamp"].(string)) {
				t.Errorf("timestamp = %v", got["timestamp"])
			}
			if !reflect.DeepEqual(got["message"], tt.message) {
				t.Errorf("message = %#v, want %#v", got["message"], tt.message)
			}
		})
	}
}

func TestWriteError_StatusCoderForeignError(t *testing.T) {
	e := &codedStatusError{code: 429, msg: "too many"}
	r := httptest.NewRequest("GET", "/x", nil)
	w := httptest.NewRecorder()
	WriteError(w, r, e)

	if w.Code != 429 {
		t.Fatalf("code = %d, want 429", w.Code)
	}
	got := envDecode(t, w.Body.Bytes())
	if got["message"] != "too many" || got["statusCode"] != float64(429) {
		t.Errorf("body = %v", got)
	}
}

type codedStatusError struct {
	code int
	msg  string
}

func (e *codedStatusError) StatusCode() int { return e.code }
func (e *codedStatusError) Error() string   { return e.msg }

// reportingRecorder stands in for the Sentry middleware's response writer.
type reportingRecorder struct {
	*httptest.ResponseRecorder
	reported []error
}

func (r *reportingRecorder) ReportException(err error) { r.reported = append(r.reported, err) }

// wrappedWriter is an unrelated middleware wrapper between the handler and
// the reporter.
type wrappedWriter struct{ http.ResponseWriter }

func (w wrappedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestExceptionsReachErrorTracking(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name     string
		write    func(w http.ResponseWriter)
		wantName string
		wantErr  error
	}{
		{"handler errors (WriteError)", func(w http.ResponseWriter) {
			WriteError(w, httptest.NewRequest("GET", "/users", nil), boom)
		}, "", boom},
		{"http exceptions", func(w http.ResponseWriter) {
			WriteError(w, httptest.NewRequest("GET", "/users", nil), NewHTTPException(http.StatusNotFound, "nope"))
		}, "", nil},
		{"validation pipe", func(w http.ResponseWriter) {
			WriteZodValidationError(w, []FieldIssue{{Code: "custom", Path: "x", Message: "bad"}})
		}, "ZodValidationException", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &reportingRecorder{ResponseRecorder: httptest.NewRecorder()}
			c.write(wrappedWriter{rec})
			if len(rec.reported) != 1 {
				t.Fatalf("reported %d exceptions, want 1", len(rec.reported))
			}
			if c.wantErr != nil && !errors.Is(rec.reported[0], c.wantErr) {
				t.Errorf("reported %v, want %v", rec.reported[0], c.wantErr)
			}
			if c.wantName != "" {
				named, ok := rec.reported[0].(interface{ ExceptionName() string })
				if !ok || named.ExceptionName() != c.wantName {
					t.Errorf("reported %#v, want a %s", rec.reported[0], c.wantName)
				}
			}
		})
	}

	// No error tracking wired: nothing to report to, the response is intact.
	w := httptest.NewRecorder()
	WriteError(w, httptest.NewRequest("GET", "/users", nil), boom)
	if w.Code != 500 {
		t.Errorf("code = %d", w.Code)
	}
}
