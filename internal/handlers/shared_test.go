package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

const epTestJWTSecret = "0123456789abcdef0123456789abcdef"

type epEnv struct {
	T    *testing.T
	H    *harness.Harness
	DB   *sql.DB
	Pool *pgxpool.Pool
	F    *harness.Factory
}

// ep_startTest boots the shared containers, applies the Go-owned baseline
// schema and opens both a fixture handle and a handler-facing pgx pool on the
// same DSN (the exact wiring cmd/api uses).
func ep_startTest(t *testing.T) *epEnv {
	t.Helper()
	ctx := context.Background()

	h, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { h.Terminate(context.Background()) })

	if err := harness.ApplyBaselineSchema(ctx, h.PostgresDSN); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open fixtures db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	pool, err := pgxpool.New(ctx, h.PostgresDSN)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return &epEnv{T: t, H: h, DB: db, Pool: pool, F: harness.NewFactory(t, db)}
}

// ep_mintToken mints an HS256 token with the exact claim set the Node auth
// service issues ({phone, businessId} + exp).
func ep_mintToken(t *testing.T, claims auth.Claims) string {
	t.Helper()
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"phone":      claims.Phone,
		"businessId": claims.BusinessID,
		"exp":        time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(epTestJWTSecret))
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	return signed
}

// ep_withClaims wraps a handler tree with the production auth.Middleware so
// requests carry claims through the SAME context key mechanism the real
// chain uses — no test-only context injection.
func ep_withClaims(h http.Handler) http.Handler {
	return auth.Middleware(epTestJWTSecret, nil)(h)
}

// ep_mount attaches a module subtree under its real vendor path exactly the
// way central integration will (chi Mount strips the prefix), then wraps the
// whole thing with auth.
func ep_mount(t *testing.T, pattern string, h http.Handler) http.Handler {
	t.Helper()
	r := chi.NewRouter()
	r.Mount(pattern, h)
	return ep_withClaims(r)
}

// ep_do performs an authenticated request against h and returns the recorder.
func ep_do(t *testing.T, h http.Handler, method, target string, claims *auth.Claims) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	if claims != nil {
		req.Header.Set("Authorization", "Bearer "+ep_mintToken(t, *claims))
	}
	rec := httptest.NewRecorder()
	ep_withClaims(h).ServeHTTP(rec, req)
	return rec
}

// ep_decode decodes a JSON response body preserving json.Number for money.
func ep_decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var v map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("decode status=%d: %v\nbody: %s", rec.Code, err, rec.Body.String())
	}
	return v
}

func ep_decodeArray(t *testing.T, rec *httptest.ResponseRecorder) []any {
	t.Helper()
	var v []any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("decode array status=%d: %v\nbody: %s", rec.Code, err, rec.Body.String())
	}
	return v
}

// ep_keys returns an object's sorted key set — the null-vs-omitted contract
// is checked by comparing full key sets against the Prisma payload shape.
func ep_keys(t *testing.T, v any) []string {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected object, got %T", v)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ep_assertMoney asserts a decoded value IS a bare JSON number (never a
// quoted string — T2.6 DecimalSerializerInterceptor contract) numerically
// equal to want (exact decimal comparison, no float slop).
func ep_assertMoney(t *testing.T, got any, want string) {
	t.Helper()
	n, ok := got.(json.Number)
	if !ok {
		t.Fatalf("money value must be a bare JSON number, got %T (%v)", got, got)
	}
	gotD, err := decimal.NewFromString(n.String())
	if err != nil {
		t.Fatalf("bad number %q: %v", n, err)
	}
	wantD, err := decimal.NewFromString(want)
	if err != nil {
		t.Fatalf("bad expectation %q: %v", want, err)
	}
	if !gotD.Equal(wantD) {
		t.Fatalf("money = %s, want %s", n, want)
	}
}

var ep_isoRe = `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`

var ep_isoPattern = regexp.MustCompile(ep_isoRe)

// json_Number is decode-comparison sugar: decoded numbers are json.Number.
func json_Number(s string) json.Number { return json.Number(s) }

// ep_assertISO asserts the JS Date JSON rendering: ISO-8601 with exactly
// millisecond precision and a Z suffix ("2026-08-22T09:30:00.000Z").
func ep_assertISO(t *testing.T, label, v any) {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("%s: expected ISO string, got %T (%v)", label, v, v)
	}
	if !ep_isoPattern.MatchString(s) {
		t.Fatalf("%s: %q does not match toISOString() shape %s", label, s, ep_isoRe)
	}
}

// ---- direct-SQL seed helpers for rows the Factory does not cover ----------

func ep_seedOrder(t *testing.T, e *epEnv, id, bizID, custID, convID, status, total string, createdAt time.Time) string {
	e.T.Helper()
	key := "wamid-" + id
	if _, err := e.DB.Exec(`INSERT INTO orders
		(id, business_id, customer_id, conversation_id, idempotency_key, status, total_amount, currency, created_at, updated_at)
		VALUES ($1, $2, $3, NULLIF($4,''), $5, $6, $7, 'GHS', $8, $8)`,
		id, bizID, custID, convID, key, status, total, createdAt); err != nil {
		t.Fatalf("seed order %s: %v", id, err)
	}
	return key
}

func ep_seedOrderItem(t *testing.T, e *epEnv, id, orderID, productID, name string, qty int, unitPrice string) {
	e.T.Helper()
	if _, err := e.DB.Exec(`INSERT INTO order_items
		(id, order_id, product_id, product_name, quantity, unit_price)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id, orderID, productID, name, qty, unitPrice); err != nil {
		t.Fatalf("seed order item %s: %v", id, err)
	}
}

func ep_seedPayment(t *testing.T, e *epEnv, id, bizID, orderID, status, amount string, paidAt, createdAt time.Time) {
	e.T.Helper()
	if _, err := e.DB.Exec(`INSERT INTO payments
		(id, business_id, order_id, amount, currency, provider, status, paid_at, created_at)
		VALUES ($1, $2, NULLIF($3,''), $4, 'GHS', 'paystack', $5, $6, $7)`,
		id, bizID, orderID, amount, status, paidAt, createdAt); err != nil {
		t.Fatalf("seed payment %s: %v", id, err)
	}
}

func ep_seedPayout(t *testing.T, e *epEnv, id, bizID, status, amount, reference string, createdAt time.Time) {
	e.T.Helper()
	if _, err := e.DB.Exec(`INSERT INTO payouts
		(id, business_id, amount, currency, status, reference, created_at, updated_at)
		VALUES ($1, $2, $3, 'GHS', $4, NULLIF($5,''), $6, $6)`,
		id, bizID, amount, status, reference, createdAt); err != nil {
		t.Fatalf("seed payout %s: %v", id, err)
	}
}

func ep_seedConversation(t *testing.T, e *epEnv, id, bizID, custID, phone, state string, escalated bool, reason string, updatedAt time.Time) {
	e.T.Helper()
	if _, err := e.DB.Exec(`INSERT INTO conversations
		(id, business_id, customer_id, customer_phone, state, is_escalated_to_human, escalation_reason, language, created_at, updated_at)
		VALUES ($1, $2, NULLIF($3,''), $4, $5, $6, NULLIF($7,''), 'en', $8, $8)`,
		id, bizID, custID, phone, state, escalated, reason, updatedAt); err != nil {
		t.Fatalf("seed conversation %s: %v", id, err)
	}
}

func ep_seedInboundMessage(t *testing.T, e *epEnv, id, convID, bizID, senderPhone, text string, ts time.Time) {
	e.T.Helper()
	payload := fmt.Sprintf(`{"type":"text","text":%q,"from_me":false}`, text)
	if _, err := e.DB.Exec(`INSERT INTO inbound_messages
		(id, whatsapp_message_id, sender_phone, recipient_phone, message_type, text_content, raw_payload, timestamp, business_id, conversation_id, created_at)
		VALUES ($1, $2, $3, '+233555000000', 'text', $4, $5::jsonb, $6, NULLIF($7,''), NULLIF($8,''), $6)`,
		id, "wain-"+id, senderPhone, text, payload, ts, bizID, convID); err != nil {
		t.Fatalf("seed inbound %s: %v", id, err)
	}
}

func ep_seedOutboundMessage(t *testing.T, e *epEnv, id, convID, bizID, recipientPhone, text, status string, ts time.Time) {
	e.T.Helper()
	payload := fmt.Sprintf(`{"type":"text","text":%q}`, text)
	if _, err := e.DB.Exec(`INSERT INTO outbound_messages
		(id, whatsapp_message_id, recipient_phone, message_type, text_content, raw_payload, meta_response, status, business_id, conversation_id, created_at)
		VALUES ($1, NULLIF($2,''), $3, 'text', $4, $5::jsonb, NULL, $6, NULLIF($7,''), NULLIF($8,''), $9)`,
		id, "waout-"+id, recipientPhone, text, payload, status, bizID, convID, ts); err != nil {
		t.Fatalf("seed outbound %s: %v", id, err)
	}
}

func ep_setProductCreated(t *testing.T, e *epEnv, id string, createdAt time.Time) {
	e.T.Helper()
	if _, err := e.DB.Exec(`UPDATE products SET created_at = $2 WHERE id = $1`, id, createdAt); err != nil {
		t.Fatalf("touch product %s: %v", id, err)
	}
}

func ep_setCustomerLastContact(t *testing.T, e *epEnv, id string, at time.Time) {
	e.T.Helper()
	if _, err := e.DB.Exec(`UPDATE customers SET last_contact_at = $2 WHERE id = $1`, id, at); err != nil {
		t.Fatalf("touch customer %s: %v", id, err)
	}
}
