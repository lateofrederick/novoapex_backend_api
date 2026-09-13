package handlers

// Stage 3 authentication tests. Prefixes: TestS3_*, helpers s3*.
//
// Pure-unit handler behaviour against fakes (validation shapes, delivery
// branches, error envelopes, /auth/me guard).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/config"
)

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type s3FakeKV struct {
	mu      sync.Mutex
	strings map[string]string
	expires map[string]time.Time
}

func newS3FakeKV() *s3FakeKV {
	return &s3FakeKV{strings: map[string]string{}, expires: map[string]time.Time{}}
}

func (f *s3FakeKV) Get(_ context.Context, key string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.strings[key]
	return v, ok, nil
}

func (f *s3FakeKV) Incr(_ context.Context, key string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.strings[key] += "x"
	return int64(len(f.strings[key])), nil
}

func (f *s3FakeKV) Expire(_ context.Context, key string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.strings[key]; !ok {
		return errors.New("no such key")
	}
	f.expires[key] = time.Now().Add(ttl)
	return nil
}

func (f *s3FakeKV) Del(_ context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.strings, k)
		delete(f.expires, k)
	}
	return nil
}

func (f *s3FakeKV) MultiSetWithDelete(_ context.Context, setKey, setValue string, ttl time.Duration, delKeys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.strings[setKey] = setValue
	f.expires[setKey] = time.Now().Add(ttl)
	for _, k := range delKeys {
		delete(f.strings, k)
	}
	return nil
}

func (f *s3FakeKV) value(key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.strings[key]
	return v, ok
}

type s3MailerFunc func(to, code string) error

func (f s3MailerFunc) SendOtpEmail(to, code string) error { return f(to, code) }

type s3WAFunc func(phoneNumberID, to, text string) error

func (f s3WAFunc) SendTextMessage(phoneNumberID, to, text string) error {
	return f(phoneNumberID, to, text)
}

// ---------------------------------------------------------------------------
// mounting + request sugar
// ---------------------------------------------------------------------------

func s3_mount(t *testing.T, deps AuthDeps) http.Handler {
	t.Helper()
	sub := chi.NewRouter()
	MountAuth(sub, deps)
	root := chi.NewRouter()
	root.Mount("/auth", sub)
	// Production welding wraps the whole tree in the JWT middleware with the
	// two public OTP endpoints opted out (mirrors @Public decorators).
	return auth.Middleware(deps.Secret, func(r *http.Request) bool {
		return r.URL.Path == "/auth/request-otp" || r.URL.Path == "/auth/verify-otp"
	})(root)
}

func s3_request(h http.Handler, method, path string, body any, token, xff string) *httptest.ResponseRecorder {
	var rdr io.Reader = bytes.NewReader(nil)
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if xff != "" {
		req.Header.Set("X-Forwarded-For", xff)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func s3_decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var v map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("decode status=%d: %v\nbody: %s", rec.Code, err, rec.Body.String())
	}
	return v
}

func s3_deps(kv *s3FakeKV, pool *pgxpool.Pool, mailer Mailer, wa WhatsAppSender) AuthDeps {
	return AuthDeps{
		Redis:      kv,
		Pool:       pool,
		Secret:     epTestJWTSecret,
		Mailer:     mailer,
		WhatsApp:   wa,
		WhatsAppID: "wni_unit_001",
	}
}

// ---------------------------------------------------------------------------
// 1. unit-level behaviour
// ---------------------------------------------------------------------------

func TestS3_RequestOTP_EmailDeliveryHappyPath(t *testing.T) {
	kv := newS3FakeKV()
	var gotTo, gotCode string
	deps := s3_deps(kv, nil, s3MailerFunc(func(to, code string) error {
		gotTo, gotCode = to, code
		return nil
	}), s3WAFunc(func(string, string, string) error {
		t.Fatal("whatsapp must not be used for the email branch")
		return nil
	}))
	h := s3_mount(t, deps)

	const phone = "+233200000201"
	rec := s3_request(h, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": phone, "deliveryMethod": "email", "email": "vendor@example.com"}, "", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := s3_decode(t, rec)
	if body["message"] != "OTP sent successfully" {
		t.Fatalf("body=%v", body)
	}
	code, ok := kv.value(auth.OTPCodeKey(phone))
	if !ok || len(code) != 6 {
		t.Fatalf("redis code = %q", code)
	}
	if gotTo != "vendor@example.com" || gotCode != code {
		t.Fatalf("mailer args = (%q,%q), want (%q,%q)", gotTo, gotCode, "vendor@example.com", code)
	}
	if _, ok := kv.value(auth.OTPAttemptsKey(phone)); ok {
		t.Fatal("attempts key must be reset by generate")
	}
}

func TestS3_RequestOTP_WhatsappBranchUsesExactTemplate(t *testing.T) {
	kv := newS3FakeKV()
	var gotID, gotTo, gotText string
	deps := s3_deps(kv, nil, s3MailerFunc(func(string, string) error {
		t.Fatal("mailer must not be used for the whatsapp branch")
		return nil
	}), s3WAFunc(func(id, to, text string) error {
		gotID, gotTo, gotText = id, to, text
		return nil
	}))
	h := s3_mount(t, deps)

	const phone = "+233200000202"
	rec := s3_request(h, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": phone, "deliveryMethod": "whatsapp"}, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	code, _ := kv.value(auth.OTPCodeKey(phone))
	want := fmt.Sprintf("Your NovoApex login code is: *%s*. It will expire in 5 minutes.", code)
	if gotText != want || gotID != "wni_unit_001" || gotTo != phone {
		t.Fatalf("wa args=(%q,%q,%q)\nwant text %q", gotID, gotTo, gotText, want)
	}
}

func TestS3_RequestOTP_DeliveryFailureIsBare500AndKeepsCode(t *testing.T) {
	kv := newS3FakeKV()
	deps := s3_deps(kv, nil, s3MailerFunc(func(string, string) error {
		return errors.New("smtp connection refused")
	}), s3WAFunc(nil))
	h := s3_mount(t, deps)

	const phone = "+233200000203"
	rec := s3_request(h, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": phone, "deliveryMethod": "email", "email": "v@example.com"}, "", "")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", rec.Code)
	}
	body := s3_decode(t, rec)
	if body["statusCode"] != json.Number("500") || body["message"] != "Internal server error" ||
		body["path"] != "/auth/request-otp" || body["timestamp"] == "" {
		t.Fatalf("body=%v", body)
	}
	// Flow order: generate THEN deliver — failed delivery leaves the code.
	if c, ok := kv.value(auth.OTPCodeKey(phone)); !ok || len(c) != 6 {
		t.Fatalf("code must survive delivery failure, got %q", c)
	}
}

// Validation matrix pinned against zod v4.3.6 executing RequestOtpSchema /
// VerifyOtpSchema (see research notes atop auth.go).
func TestS3_RequestOTP_ZodValidationMatrix(t *testing.T) {
	kv := newS3FakeKV()
	deps := s3_deps(kv, nil, s3MailerFunc(nil), s3WAFunc(nil))
	h := s3_mount(t, deps)

	cases := []struct {
		name string
		body any
		want []httpIssue
	}{
		{
			name: "missing everything",
			body: map[string]any{},
			want: []httpIssue{{"invalid_type", []string{"phone"}, "Invalid input: expected string, received undefined"}},
		},
		{
			name: "short phone + refine fires",
			body: map[string]any{"phone": "+233"},
			want: []httpIssue{
				{"too_small", []string{"phone"}, "Too small: expected string to have >=10 characters"},
				{"custom", []string{"email"}, "Email is required when deliveryMethod is email"},
			},
		},
		{
			name: "bad email format",
			body: map[string]any{"phone": "+233200000000", "email": "nope", "deliveryMethod": "email"},
			want: []httpIssue{{"invalid_format", []string{"email"}, "Invalid email address"}},
		},
		{
			name: "bad enum skips refine",
			body: map[string]any{"phone": "+233200000000", "deliveryMethod": "fax"},
			want: []httpIssue{{"invalid_value", []string{"deliveryMethod"}, `Invalid option: expected one of "email"|"whatsapp"`}},
		},
		{
			name: "empty email fails format and refine",
			body: map[string]any{"phone": "+233200000000", "email": ""},
			want: []httpIssue{
				{"invalid_format", []string{"email"}, "Invalid email address"},
				{"custom", []string{"email"}, "Email is required when deliveryMethod is email"},
			},
		},
		{
			name: "null phone",
			body: map[string]any{"phone": nil},
			want: []httpIssue{{"invalid_type", []string{"phone"}, "Invalid input: expected string, received null"}},
		},
		{
			name: "number phone",
			body: map[string]any{"phone": 233200000},
			want: []httpIssue{{"invalid_type", []string{"phone"}, "Invalid input: expected string, received number"}},
		},
		{
			name: "array body",
			body: []any{"x"},
			want: []httpIssue{{"invalid_type", []string{}, "Invalid input: expected object, received array"}},
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := s3_request(h, http.MethodPost, "/auth/request-otp", tc.body, "",
				fmt.Sprintf("203.0.114.%d", 4+i))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			assertIssues(t, rec.Body.Bytes(), tc.want)
		})
	}
}

func TestS3_VerifyOTP_ZodValidationMatrix(t *testing.T) {
	deps := s3_deps(newS3FakeKV(), nil, s3MailerFunc(nil), s3WAFunc(nil))
	h := s3_mount(t, deps)

	cases := []struct {
		name string
		body any
		want []httpIssue
	}{
		{
			name: "short code",
			body: map[string]any{"phone": "+233200000000", "code": "123"},
			want: []httpIssue{{"too_small", []string{"code"}, "Too small: expected string to have >=6 characters"}},
		},
		{
			name: "long code",
			body: map[string]any{"phone": "+233200000000", "code": "1234567"},
			want: []httpIssue{{"too_big", []string{"code"}, "Too big: expected string to have <=6 characters"}},
		},
		{
			name: "missing code",
			body: map[string]any{"phone": "+233200000000"},
			want: []httpIssue{{"invalid_type", []string{"code"}, "Invalid input: expected string, received undefined"}},
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := s3_request(h, http.MethodPost, "/auth/verify-otp", tc.body, "",
				fmt.Sprintf("203.0.115.%d", 4+i))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			assertIssues(t, rec.Body.Bytes(), tc.want)
		})
	}
}

// httpIssue is the rendered subset of a zod issue (WriteZodValidationError's
// pinned Stage 2 shape).
type httpIssue struct {
	Code    string
	Path    []string
	Message string
}

func assertIssues(t *testing.T, raw []byte, want []httpIssue) {
	t.Helper()
	var body struct {
		StatusCode int    `json:"statusCode"`
		Message    string `json:"message"`
		Errors     []struct {
			Code    string   `json:"code"`
			Path    []string `json:"path"`
			Message string   `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode: %v (%s)", err, raw)
	}
	if body.StatusCode != 400 || body.Message != "Validation failed" {
		t.Fatalf("envelope mismatch: %+v", body)
	}
	if len(body.Errors) != len(want) {
		t.Fatalf("issues = %+v\nwant %+v", body.Errors, want)
	}
	for i, w := range want {
		got := body.Errors[i]
		if got.Code != w.Code || got.Message != w.Message || !equalStrings(got.Path, w.Path) {
			t.Fatalf("issue[%d] = {%s %v %q}\nwant {%s %v %q}", i, got.Code, got.Path, got.Message, w.Code, w.Path, w.Message)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestS3_VerifyOTP_FailureShapes(t *testing.T) {
	kv := newS3FakeKV()
	_ = kv.MultiSetWithDelete(context.Background(), auth.OTPCodeKey("+233200000210"), "654321", auth.OTPTTL, auth.OTPAttemptsKey("+233200000210"))
	deps := s3_deps(kv, nil, s3MailerFunc(nil), s3WAFunc(nil))
	h := s3_mount(t, deps)

	rec := s3_request(h, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": "+233200000210", "code": "111111"}, "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var env struct {
		StatusCode int `json:"statusCode"`
		Message    struct {
			Message    string `json:"message"`
			Error      string `json:"error"`
			StatusCode int    `json:"statusCode"`
		} `json:"message"`
		Timestamp string `json:"timestamp"`
		Path      string `json:"path"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.StatusCode != 401 || env.Path != "/auth/verify-otp" || env.Timestamp == "" ||
		env.Message.Message != "Invalid or expired OTP" ||
		env.Message.Error != "Unauthorized" || env.Message.StatusCode != 401 {
		t.Fatalf("unauthorized envelope mismatch: %s", rec.Body.String())
	}
	// Wrong code must still have consumed an attempt (counter incremented,
	// code NOT burned — parity with otp.service.ts).
	if _, ok := kv.value(auth.OTPCodeKey("+233200000210")); !ok {
		t.Fatal("wrong code must not burn the stored OTP")
	}
}

func TestS3_Me_GuardedAndBusinessLookup(t *testing.T) {
	kv := newS3FakeKV()
	deps := s3_deps(kv, nil, s3MailerFunc(nil), s3WAFunc(nil))
	// Pool nil would panic on /me query — unit test only the guard path here.
	h := s3_mount(t, deps)

	rec := s3_request(h, http.MethodGet, "/auth/me", nil, "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401 from welded middleware", rec.Code)
	}
	// Pinned Stage 2 guard body (flat, statusCode+message).
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 2 || body["statusCode"] != float64(401) || body["message"] != "Invalid or missing session token" {
		t.Fatalf("guard body mismatch: %v", body)
	}
}

// TestS3_Me_HappyPathReturnsBusinessByOwnerPhone is the DB-backed twin of the
// guard-path test above: a valid token whose phone matches a seeded
// business's ownerPhone must return that business, field-for-field.
func TestS3_Me_HappyPathReturnsBusinessByOwnerPhone(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()

	deps := s3_deps(newS3FakeKV(), e.Pool, s3MailerFunc(nil), s3WAFunc(nil))
	h := s3_mount(t, deps)
	token := ep_mintToken(t, auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})

	rec := s3_request(h, http.MethodGet, "/auth/me", nil, token, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	body := s3_decode(t, rec)
	if body["id"] != biz.ID {
		t.Errorf("id = %v, want %s", body["id"], biz.ID)
	}
	if body["ownerPhone"] != biz.OwnerPhone {
		t.Errorf("ownerPhone = %v, want %s", body["ownerPhone"], biz.OwnerPhone)
	}
	if body["whatsappPhoneNumberId"] != biz.WhatsAppPhoneNumberID {
		t.Errorf("whatsappPhoneNumberId = %v, want %s", body["whatsappPhoneNumberId"], biz.WhatsAppPhoneNumberID)
	}
	if body["currency"] != biz.Currency {
		t.Errorf("currency = %v, want %s", body["currency"], biz.Currency)
	}
}

// T3.11: short JWT_SECRET must fail startup exactly like the Node config
// schema (config.schema.ts refines JWT_SECRET min length 32).
func TestS3_Config_ShortJWTSecretFailsLoad(t *testing.T) {
	env := map[string]string{
		"NODE_ENV":     "test",
		"DATABASE_URL": "postgresql://u:p@localhost:5432/db",
		"JWT_SECRET":   "short-secret",
	}
	_, err := config.LoadFrom(env)
	if err == nil {
		t.Fatal("Load must reject a short JWT_SECRET")
	}
	if !strings.Contains(err.Error(), "JWT_SECRET must be at least 32 characters") {
		t.Fatalf("error missing pinned message: %v", err)
	}
}
