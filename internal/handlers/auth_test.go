package handlers

// Stage 3 authentication tests. Prefixes: TestS3_*, helpers s3*.
//
// Layers:
//  1. pure-unit handler behaviour against fakes (validation shapes, delivery
//     branches, error envelopes, /auth/me guard);
//  2. cross-validation against the REAL node stack (OTP both directions,
//     JWT both directions incl. `node -e` jsonwebtoken verification,
//     /auth/me parity, throttler parity).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
	smtp "github.com/novoapex/novoapex-backend-api/internal/integrations/smtp"
	whatsapp "github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
)

func s3RedisClient(t *testing.T, h *harness.Harness) *redis.Client {
	t.Helper()
	host, port, err := net.SplitHostPort(h.RedisAddr)
	if err != nil {
		t.Fatalf("split redis addr: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(host, port), Password: harness.RedisPassword})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	return client
}

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
	// Pool nil would panic on /me query — unit test only the guard path here;
	// the DB-backed happy path runs in the e2e section.
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

// ---------------------------------------------------------------------------
// 2. cross-validation against the real node stack
// ---------------------------------------------------------------------------

// s3SMTPSink is a minimal DATA-capturing SMTP stub for the Go mailer (same
// protocol shape as the harness stub).
type s3SMTPSink struct {
	listener net.Listener
	mu       sync.Mutex
	messages []string
}

func s3StartSMTPSink(t *testing.T) *s3SMTPSink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("smtp sink listen: %v", err)
	}
	sink := &s3SMTPSink{listener: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go sink.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return sink
}

func (s *s3SMTPSink) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	writeLine := func(l string) { _, _ = io.WriteString(conn, l+"\r\n") }
	writeLine("220 sink.smtp ESMTP")
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	inData := false
	var data strings.Builder
	for sc.Scan() {
		line := sc.Text()
		if inData {
			if line == "." {
				s.mu.Lock()
				s.messages = append(s.messages, data.String())
				s.mu.Unlock()
				data.Reset()
				inData = false
				writeLine("250 OK queued")
				continue
			}
			data.WriteString(line)
			data.WriteString("\n")
			continue
		}
		upper := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(upper, "EHLO"):
			writeLine("250-sink.smtp")
			writeLine("250-8BITMIME")
			writeLine("250 OK")
		case upper == "DATA":
			inData = true
			writeLine("354 End data with <CR><LF>.<CR><LF>")
		case strings.HasPrefix(upper, "AUTH"):
			writeLine("235 2.7.0 Accepted")
		case upper == "QUIT":
			writeLine("221 Bye")
			return
		default:
			writeLine("250 OK")
		}
	}
}

func (s *s3SMTPSink) waitFor(t *testing.T, substr string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for _, m := range s.messages {
			if strings.Contains(m, substr) {
				s.mu.Unlock()
				return m
			}
		}
		s.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no smtp message containing %q within %s", substr, timeout)
	return ""
}

// smtpAdapter wires the sink into the Mailer handler dependency.
type smtpAdapter struct{ sink *s3SMTPSink }

func (a smtpAdapter) SendOtpEmail(to, code string) error {
	cfg := smtp.Config{Host: "127.0.0.1", Port: a.sink.listener.Addr().(*net.TCPAddr).Port,
		User: "stub@example.com", Pass: "stub-pass"}
	return smtp.SendOTPEmail(cfg, to, code)
}

// waAdapter wires a configured whatsapp.Client into WhatsAppSender.
type waAdapter struct{ c *whatsapp.Client }

func (a waAdapter) SendTextMessage(phoneNumberID, to, text string) error {
	return a.c.SendTextMessage(context.Background(), phoneNumberID, to, text)
}

// s3WASpy records WhatsApp sends behind an httptest server.
type s3WASpy struct {
	server *httptest.Server
	mu     sync.Mutex
	sends  []s3WASend
}

type s3WASend struct {
	Path string
	Auth string
	Body map[string]any
}

func s3StartWASpy(t *testing.T) *s3WASpy {
	t.Helper()
	spy := &s3WASpy{}
	spy.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		send := s3WASend{Path: r.URL.Path, Auth: r.Header.Get("Authorization")}
		_ = json.NewDecoder(r.Body).Decode(&send.Body)
		spy.mu.Lock()
		spy.sends = append(spy.sends, send)
		spy.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messaging_product": "whatsapp",
			"messages":          []any{map[string]any{"id": "wamid.spy"}},
		})
	}))
	t.Cleanup(spy.server.Close)
	return spy
}

func (s *s3WASpy) last(t *testing.T) s3WASend {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sends) == 0 {
		t.Fatal("no whatsapp sends captured")
	}
	return s.sends[len(s.sends)-1]
}

// apiResponseLike mirrors the harness apiRequest result for node calls.
type apiResponseLike struct {
	Status  int
	Body    []byte
	Headers http.Header
}

func (r apiResponseLike) JSON(t *testing.T) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(r.Body, &v); err != nil {
		t.Fatalf("non-JSON response %d: %s", r.Status, r.Body)
	}
	return v
}

// s3NodeClient talks to the live node stack with trust-proxy-friendly unique
// XFF addresses so each logical flow gets its own throttler bucket.
type s3NodeClient struct {
	baseURL string
	ipSeq   int
}

func (c *s3NodeClient) do(t *testing.T, method, path string, body any, token string) apiResponseLike {
	t.Helper()
	c.ipSeq++
	xff := fmt.Sprintf("198.51.100.%d", 4+c.ipSeq%250)

	var rdr io.Reader = bytes.NewReader(nil)
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.baseURL+path, rdr)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("X-Forwarded-For", xff)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("node %s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read node response: %v", err)
	}
	t.Logf("node %s %s -> %d %s", method, path, resp.StatusCode, s3Truncate(raw, 400))
	return apiResponseLike{Status: resp.StatusCode, Body: raw, Headers: resp.Header}
}

func s3Truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

// s3NodeVerifyJWT validates a token with the novoapex repo's own jsonwebtoken
// install — the exact library that mints Node-side tokens.
func s3NodeVerifyJWT(t *testing.T, repoDir, token, secret string) map[string]any {
	t.Helper()
	const script = `const jwt=require("jsonwebtoken");
try {
  const d = jwt.verify(process.argv[1], process.argv[2], { algorithms: ["HS256"] });
  console.log(JSON.stringify({ phone: d.phone ?? null, businessId: d.businessId ?? null, iat: d.iat ?? null, exp: d.exp ?? null }));
} catch (e) {
  console.log(JSON.stringify({ error: e.message }));
}`
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "-e", script, token, secret)
	cmd.Dir = repoDir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("node -e jwt verify: %v (%s)", err, out)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("parse node output %q: %v", out, err)
	}
	if parsed["error"] != nil {
		t.Fatalf("node jwt.verify rejected GO-minted token: %v", parsed["error"])
	}
	t.Logf("node jsonwebtoken.verify(GO token) -> %s", s3Truncate(out, 200))
	return parsed
}

func s3Num(t *testing.T, v any) float64 {
	t.Helper()
	n, ok := v.(float64)
	if !ok {
		t.Fatalf("expected number, got %T (%v)", v, v)
	}
	return n
}

func TestS3_AuthCrossValidationAgainstNodeStack(t *testing.T) {
	repoDir, err := harness.NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repoDir, "dist/apps/api/apps/api/src/main.js")); err != nil {
		t.Skipf("node dist missing - run npm run build:all first: %v", err)
	}

	e := ep_startTest(t)
	h := e.H
	stack := harness.StartNodeStack(t, h)

	rdb := s3RedisClient(t, h)
	defer func() { _ = rdb.Close() }()

	sink := s3StartSMTPSink(t)
	spy := s3StartWASpy(t)

	deps := AuthDeps{
		Redis:  auth.NewGoRedisAdapter(rdb),
		Pool:   e.Pool,
		Secret: epTestJWTSecret,
		Mailer: smtpAdapter{sink},
		WhatsApp: waAdapter{&whatsapp.Client{
			BaseURL: spy.server.URL, Version: "v25.0", Token: "spy-token",
		}},
		WhatsAppID: "wni_test_001",
	}
	goAuth := s3_mount(t, deps)
	node := &s3NodeClient{baseURL: stack.BaseURL}

	phoneSeq := 0
	nextPhone := func() string {
		phoneSeq++
		return fmt.Sprintf("+2337300%04d00", phoneSeq)
	}
	otpFromRedis := func(phone string) string {
		t.Helper()
		code, err := rdb.Get(context.Background(), auth.OTPCodeKey(phone)).Result()
		if err != nil || len(code) != 6 {
			t.Fatalf("otp code for %s not in redis (%q, %v)", phone, code, err)
		}
		return code
	}

	// ---- (a) Node issues OTP -> Go verifies ---------------------------------
	aPhone := nextPhone()
	if r := node.do(t, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": aPhone, "deliveryMethod": "email", "email": "a@example.com"}, ""); r.Status != 200 {
		t.Fatalf("(a) node request-otp status=%d body=%s", r.Status, r.Body)
	}
	aCode := otpFromRedis(aPhone)

	goVerify := s3_request(goAuth, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": aPhone, "code": aCode}, "", "")
	if goVerify.Code != 200 {
		t.Fatalf("(a) GO verify-otp status=%d body=%s", goVerify.Code, goVerify.Body.String())
	}
	parsed := s3_decode(t, goVerify)
	goTokenA, _ := parsed["token"].(string)
	if goTokenA == "" || parsed["isNewUser"] != true || parsed["business"] != nil {
		t.Fatalf("(a) GO verify response mismatch: %v", parsed)
	}

	// Go-minted token must satisfy Node's guard+strategy: a fresh signup has no
	// business yet so /auth/me answers the business-not-found 401 (a REJECTED
	// token would get the invalid-token 401 instead).
	meA := node.do(t, http.MethodGet, "/auth/me", nil, goTokenA)
	if meA.Status != 401 || !strings.Contains(string(meA.Body), "Business profile not found") {
		t.Fatalf("(a) node /auth/me with GO token = %d %s", meA.Status, meA.Body)
	}

	// ---- (b) Go issues OTP -> Node verifies ---------------------------------
	bPhone := nextPhone()
	br := s3_request(goAuth, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": bPhone, "deliveryMethod": "email", "email": "b@example.com"}, "", "")
	if br.Code != 200 {
		t.Fatalf("(b) GO request-otp status=%d body=%s", br.Code, br.Body.String())
	}
	msg := sink.waitFor(t, "b@example.com", 20*time.Second)
	for _, want := range []string{
		"Subject: Your NovoApex Login Code",
		`"NovoApex Auth" <stub@example.com>`,
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("(b) sink message missing %q", want)
		}
	}

	nv := node.do(t, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": bPhone, "code": otpFromRedis(bPhone)}, "")
	if nv.Status != 200 {
		t.Fatalf("(b) node verify-otp status=%d body=%s", nv.Status, nv.Body)
	}
	nvBody := nv.JSON(t)
	if nvBody["token"] == "" || nvBody["isNewUser"] != true {
		t.Fatalf("(b) node verify response = %v", nvBody)
	}

	// WhatsApp delivery branch through the spy.
	cPhone := nextPhone()
	cr := s3_request(goAuth, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": cPhone, "deliveryMethod": "whatsapp"}, "", "")
	if cr.Code != 200 {
		t.Fatalf("(b-wa) GO request-otp status=%d body=%s", cr.Code, cr.Body.String())
	}
	waSend := spy.last(t)
	wantText := fmt.Sprintf("Your NovoApex login code is: *%s*. It will expire in 5 minutes.", otpFromRedis(cPhone))
	if waSend.Path != "/v25.0/wni_test_001/messages" || waSend.Auth != "Bearer spy-token" {
		t.Fatalf("(b-wa) request line/auth mismatch: %+v", waSend)
	}
	textObj, _ := waSend.Body["text"].(map[string]any)
	if waSend.Body["messaging_product"] != "whatsapp" || waSend.Body["to"] != cPhone ||
		textObj == nil || textObj["body"] != wantText {
		t.Fatalf("(b-wa) template mismatch:\nhave %+v\nwant body %q", waSend.Body, wantText)
	}
	cv := node.do(t, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": cPhone, "code": otpFromRedis(cPhone)}, "")
	if cv.Status != 200 {
		t.Fatalf("(b-wa) node verify status=%d body=%s", cv.Status, cv.Body)
	}

	// ---- (c) Go mints JWT -> validated by node jsonwebtoken ------------------
	biz := e.F.Business()
	goTokenBiz, err := auth.Mint(epTestJWTSecret, auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
	if err != nil {
		t.Fatal(err)
	}
	nodeClaims := s3NodeVerifyJWT(t, repoDir, goTokenBiz, epTestJWTSecret)
	if nodeClaims["phone"] != biz.OwnerPhone || nodeClaims["businessId"] != biz.ID {
		t.Fatalf("(c) node jwt.verify claims = %v", nodeClaims)
	}
	if delta := s3Num(t, nodeClaims["exp"]) - s3Num(t, nodeClaims["iat"]); delta != 604800 {
		t.Fatalf("(c) exp-iat = %v, want 604800 (7d)", delta)
	}
	// Same token accepted end-to-end by the running stack.
	meC := node.do(t, http.MethodGet, "/auth/me", nil, goTokenBiz)
	if meC.Status != 200 {
		t.Fatalf("(c) node /auth/me with GO-minted token status=%d body=%s", meC.Status, meC.Body)
	}

	// ---- (d) Node mints JWT -> parsed by ParseToken --------------------------
	dPhone := nextPhone()
	node.do(t, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": dPhone, "deliveryMethod": "email", "email": "d@example.com"}, "")
	dTokenResp := node.do(t, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": dPhone, "code": otpFromRedis(dPhone)}, "")
	if dTokenResp.Status != 200 {
		t.Fatalf("(d) node verify status=%d body=%s", dTokenResp.Status, dTokenResp.Body)
	}
	nodeToken := dTokenResp.JSON(t)["token"].(string)

	goClaims, err := auth.ParseToken(epTestJWTSecret, nodeToken)
	if err != nil {
		t.Fatalf("(d) ParseToken on node-minted token: %v", err)
	}
	if goClaims.Phone != dPhone || goClaims.BusinessID != "" {
		t.Fatalf("(d) parsed claims = %+v", goClaims)
	}

	// ---- /auth/me happy-path payload parity (same seeded row, both stacks) ---
	goMe := s3_request(goAuth, http.MethodGet, "/auth/me", nil, goTokenBiz, "")
	if goMe.Code != 200 {
		t.Fatalf("GO /auth/me status=%d body=%s", goMe.Code, goMe.Body.String())
	}
	var nodeMeAny, goMeAny any
	decN := json.NewDecoder(bytes.NewReader(meC.Body))
	decN.UseNumber()
	decG := json.NewDecoder(bytes.NewReader(goMe.Body.Bytes()))
	decG.UseNumber()
	if err := decN.Decode(&nodeMeAny); err != nil {
		t.Fatalf("decode node me: %v (%s)", err, meC.Body)
	}
	if err := decG.Decode(&goMeAny); err != nil {
		t.Fatalf("decode go me: %v (%s)", err, goMe.Body.String())
	}
	if !reflect.DeepEqual(nodeMeAny, goMeAny) {
		t.Errorf("/auth/me payload mismatch\n--- go ---\n%s\n--- node ---\n%s",
			ep_pretty(goMeAny), ep_pretty(nodeMeAny))
	}

	// ---- verify-otp wrong-code 401 envelope parity ---------------------------
	wPhone := nextPhone()
	node.do(t, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": wPhone, "deliveryMethod": "email", "email": "w@example.com"}, "")
	nodeWrong := node.do(t, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": wPhone, "code": "000000"}, "")
	goWrong := s3_request(goAuth, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": wPhone, "code": "000000"}, "", "")
	if nodeWrong.Status != goWrong.Code {
		t.Errorf("wrong-code verify: node=%d go=%d", nodeWrong.Status, goWrong.Code)
	}
	if diffs := s3diffSansTimestamp(t, nodeWrong.Body, goWrong.Body.Bytes()); len(diffs) > 0 {
		t.Errorf("wrong-code verify bodies differ beyond timestamp:\n%s\n--- node ---\n%s\n--- go ---\n%s",
			strings.Join(diffs, "\n"), nodeWrong.Body, goWrong.Body.String())
	}

	// Malformed JSON: the live stack answers 400 Bad Request envelope
	// (body-parser SyntaxError through AllExceptionsFilter). Assert the GO
	// side matches shape and status; the embedded parser detail is engine
	// specific (V8 vs encoding/json) and only its presence is asserted.
	badReq, _ := http.NewRequest(http.MethodPost, stack.BaseURL+"/auth/request-otp",
		strings.NewReader("{not json"))
	badReq.Header.Set("Content-Type", "application/json")
	badReq.Header.Set("X-Forwarded-For", "198.51.100.240")
	badResp, err := http.DefaultClient.Do(badReq)
	if err != nil {
		t.Fatalf("malformed json probe: %v", err)
	}
	rawBad, _ := io.ReadAll(badResp.Body)
	_ = badResp.Body.Close()
	t.Logf("EVIDENCE malformed-JSON on node -> %d ct=%q body=%.200s",
		badResp.StatusCode, badResp.Header.Get("Content-Type"), rawBad)

	goBad := httptest.NewRequest(http.MethodPost, "/auth/request-otp", strings.NewReader("{not json"))
	goBad.Header.Set("Content-Type", "application/json")
	goRec := httptest.NewRecorder()
	goAuth.ServeHTTP(goRec, goBad)
	if goRec.Code != badResp.StatusCode {
		t.Errorf("malformed JSON: go=%d node=%d", goRec.Code, badResp.StatusCode)
	}
	var badEnv struct {
		StatusCode int `json:"statusCode"`
		Message    struct {
			Message    string `json:"message"`
			Error      string `json:"error"`
			StatusCode int    `json:"statusCode"`
		} `json:"message"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal(goRec.Body.Bytes(), &badEnv); err != nil {
		t.Errorf("GO malformed-JSON body not the filter envelope: %s", goRec.Body.String())
	} else if badEnv.StatusCode != 400 || badEnv.Message.Error != "Bad Request" ||
		badEnv.Message.Message == "" || badEnv.Path != "/auth/request-otp" {
		t.Errorf("GO malformed-JSON envelope mismatch: %s", goRec.Body.String())
	}
}

// s3diffSansTimestamp diffs two JSON bodies after dropping the volatile
// timestamp field.
func s3diffSansTimestamp(t *testing.T, a, b []byte) []string {
	t.Helper()
	strip := func(raw []byte) []byte {
		var v map[string]any
		if err := json.Unmarshal(raw, &v); err != nil {
			return raw
		}
		delete(v, "timestamp")
		out, err := json.Marshal(v)
		if err != nil {
			return raw
		}
		return out
	}
	return harness.DiffJSON(strip(a), strip(b))
}

// T3.10: throttling parity on the OTP endpoints, keyed off the real client IP
// via X-Forwarded-For (trust proxy = 1).
func TestS3_AuthThrottleParityAgainstNodeStack(t *testing.T) {
	repoDir, err := harness.NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repoDir, "dist/apps/api/apps/api/src/main.js")); err != nil {
		t.Skipf("node dist missing: %v", err)
	}
	e := ep_startTest(t)
	stack := harness.StartNodeStack(t, e.H)
	rdb := s3RedisClient(t, e.H)
	defer func() { _ = rdb.Close() }()

	sink := s3StartSMTPSink(t)
	deps := AuthDeps{
		Redis:      auth.NewGoRedisAdapter(rdb),
		Pool:       e.Pool,
		Secret:     epTestJWTSecret,
		Mailer:     smtpAdapter{sink},
		WhatsApp:   waAdapter{&whatsapp.Client{BaseURL: "http://127.0.0.1:9", Version: "v25.0"}},
		WhatsAppID: "wni_test_001",
	}
	goAuth := s3_mount(t, deps)

	const pinnedIP = "203.0.113.77"
	const nodePhone = "+233999999901"
	const goPhone = "+233999999903"

	// Seed an OTP for the GO-side burst so early verifies answer 401 (wrong
	// code) rather than anything else.
	if err := rdb.Set(context.Background(), auth.OTPCodeKey(goPhone), "654321",
		auth.OTPTTL).Err(); err != nil {
		t.Fatal(err)
	}

	// Node side: five wrong verifies (401 each), the sixth must be throttled.
	nodeStatuses := make([]int, 0, 6)
	var node429 apiResponseLike
	for i := 0; i < 6; i++ {
		req, _ := http.NewRequest(http.MethodPost, stack.BaseURL+"/auth/verify-otp",
			bytes.NewReader([]byte(fmt.Sprintf(`{"phone":%q,"code":"000000"}`, nodePhone))))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Forwarded-For", pinnedIP)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Logf("node verify #%d -> %d %s retry-after=%q ratelimit-limit=%q",
			i+1, resp.StatusCode, s3Truncate(raw, 220),
			resp.Header.Get("Retry-After"), resp.Header.Get("X-RateLimit-Limit"))
		if i < 5 && resp.StatusCode == 429 {
			t.Fatalf("node blocked too early at request %d", i+1)
		}
		nodeStatuses = append(nodeStatuses, resp.StatusCode)
		if i == 5 {
			node429 = apiResponseLike{Status: resp.StatusCode, Body: raw, Headers: resp.Header}
		}
	}
	for i, s := range nodeStatuses[:5] {
		if s != http.StatusUnauthorized {
			t.Fatalf("node wrong-code verify #%d = %d, want 401", i+1, s)
		}
	}
	if node429.Status != http.StatusTooManyRequests {
		t.Fatalf("node 6th verify = %d, want 429", node429.Status)
	}

	// GO side: identical burst shape on the same endpoint.
	var go429 *httptest.ResponseRecorder
	for i := 0; i < 6; i++ {
		rec := s3_request(goAuth, http.MethodPost, "/auth/verify-otp",
			map[string]any{"phone": goPhone, "code": "000000"}, "", pinnedIP)
		if i < 5 && rec.Code != http.StatusUnauthorized {
			t.Fatalf("GO verify #%d = %d (%s), want 401", i+1, rec.Code, rec.Body.String())
		}
		if i == 5 {
			go429 = rec
		}
	}
	if go429.Code != http.StatusTooManyRequests {
		t.Fatalf("GO 6th verify = %d, want 429", go429.Code)
	}

	// Bodies must match beyond timestamp; Retry-After present on both.
	diffs := s3diffSansTimestamp(t, node429.Body, go429.Body.Bytes())
	if len(diffs) > 0 {
		t.Errorf("429 bodies differ beyond timestamp: %s\n--- node ---\n%s\n--- go ---\n%s",
			strings.Join(diffs, "\n"), node429.Body, go429.Body.String())
	}
	if node429.Headers.Get("Retry-After") == "" || go429.Header().Get("Retry-After") == "" {
		t.Errorf("Retry-After headers: node=%q go=%q, both required",
			node429.Headers.Get("Retry-After"), go429.Header().Get("Retry-After"))
	}

	// IP keying end-to-end: another XFF client keeps a fresh budget on the
	// same endpoint that just throttled pinnedIP.
	fresh := s3_request(goAuth, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": "+233999999904", "code": "000000"}, "", "203.0.113.78")
	if fresh.Code != http.StatusUnauthorized {
		t.Fatalf("second IP got %d, want fresh non-throttled 401", fresh.Code)
	}
}
