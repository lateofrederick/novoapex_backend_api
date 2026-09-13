// Command e2e runs the whole Go backend end to end: it builds the migrate,
// api and worker binaries, migrates a fresh database, starts both services
// against real Postgres + Redis with every external provider (Meta, OpenAI,
// Gemini, Paystack, Cloudinary) served by an in-process mock, then drives the
// complete vendor and customer journey and asserts on HTTP responses, database
// state and the messages that actually left the system.
//
// Usage (see scripts/e2e/README.md):
//
//	go run ./scripts/e2e -pg postgresql://postgres:postgres@127.0.0.1:55432/postgres?sslmode=disable \
//	                     -redis 127.0.0.1:56379 -redis-pass e2epass
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"image/color"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
)

const (
	jwtSecret       = "e2e-secret-0123456789abcdef-0123456789"
	waAppSecret     = "wa-app-secret-e2e"
	waVerifyToken   = "wa-verify-e2e"
	waPhoneNumberID = "PNID-E2E-1"
	paystackSecret  = "sk_test_e2e"
	ownerPhone      = "+233200000001"
	customerPhone   = "233244111222"
	customer2Phone  = "233244333444"
	dashboardUser   = "ops"
	dashboardPass   = "dash-e2e-pass"
)

type runner struct {
	ctx      context.Context
	mock     *providerMock
	db       *pgx.Conn
	api      string
	token    string
	failures int
	steps    int
	logDir   string
	redis    asynq.RedisClientOpt
	bin      map[string]string
	env      []string
}

func main() {
	pgAdmin := flag.String("pg", "postgresql://postgres:postgres@127.0.0.1:55432/postgres?sslmode=disable", "admin DSN of a pgvector Postgres")
	redisAddr := flag.String("redis", "127.0.0.1:56379", "redis host:port")
	redisPass := flag.String("redis-pass", "e2epass", "redis password")
	logDir := flag.String("logs", filepath.Join(os.TempDir(), "novoapex-e2e"), "directory for service logs and binaries")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	must(os.MkdirAll(*logDir, 0o755))

	mock, err := startMock()
	must(err)

	dbName := "novoapex_e2e"
	dsn := resetDatabase(ctx, *pgAdmin, dbName)
	// Prisma-style ?schema= suffix on purpose: existing DATABASE_URLs carry it.
	appDSN := dsn + "&schema=public"

	bin := buildBinaries(*logDir)
	r := &runner{ctx: ctx, mock: mock, logDir: *logDir,
		redis: asynq.RedisClientOpt{Addr: *redisAddr, Password: *redisPass}}
	flushRedis(r.redis)

	env := serviceEnv(appDSN, *redisAddr, *redisPass, mock.URL)
	r.bin, r.env = bin, env

	r.section("Migrations")
	r.check("migrate applies the baseline on a fresh database", runOnce(bin["migrate"], env, filepath.Join(*logDir, "migrate-1.log")) == nil)
	r.check("migrate is a no-op on re-run", runOnce(bin["migrate"], env, filepath.Join(*logDir, "migrate-2.log")) == nil)

	conn, err := pgx.Connect(ctx, dsn)
	must(err)
	r.db = conn
	r.check("schema_migrations records 0001_baseline", r.scalar(`SELECT count(*) FROM schema_migrations WHERE version = '0001_baseline'`) == "1")

	port := freePort()
	r.api = "http://127.0.0.1:" + strconv.Itoa(port)
	api := startService(bin["api"], append(env, "PORT="+strconv.Itoa(port)), filepath.Join(*logDir, "api.log"))
	worker := startService(bin["worker"], append(env, "WORKER_CONCURRENCY=8"), filepath.Join(*logDir, "worker.log"))
	defer func() {
		_ = api.Process.Kill()
		_ = worker.Process.Kill()
	}()

	r.scenario()

	r.section("Graceful shutdown")
	r.check("api exits cleanly on SIGTERM", stopService(api, 15*time.Second))
	r.check("worker exits cleanly on SIGTERM", stopService(worker, 45*time.Second))

	fmt.Printf("\n%d checks, %d failed — logs in %s\n", r.steps, r.failures, *logDir)
	if r.failures > 0 {
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// the journey
// ---------------------------------------------------------------------------

func (r *runner) scenario() {
	r.section("Platform")
	r.check("GET /health/live is up", r.waitFor(30*time.Second, func() bool {
		code, _ := r.do("GET", "/health/live", nil, "")
		return code == 200
	}))
	code, body := r.do("GET", "/health", nil, "")
	r.check("GET /health reports db+redis up", code == 200 && strings.Contains(body, `"status":"ok"`), body)
	code, _ = r.do("GET", "/metrics", nil, "")
	r.check("GET /metrics is exposed", code == 200)
	code, _ = r.do("GET", "/orders", nil, "")
	r.check("vendor routes require a session (401)", code == 401)
	code, _ = r.do("POST", "/messages/send/text", map[string]any{"to": "233201234567", "text": "spam"}, "")
	r.check("POST /messages/send/text requires a session (401) — no open relay", code == 401)

	r.section("API docs, queue dashboard, request logs, metrics")
	code, body = r.do("GET", "/api-docs", nil, "")
	r.check("Swagger UI served at /api-docs (ENABLE_SWAGGER in production)", code == 200 && strings.Contains(body, "swagger-ui-bundle.js"))
	code, _ = r.do("GET", "/api-docs/swagger-ui-bundle.js", nil, "")
	r.check("Swagger UI assets are served locally", code == 200)
	code, body = r.do("GET", "/api-docs-json", nil, "")
	r.check("OpenAPI document served at /api-docs-json", code == 200 && strings.Contains(body, `"title": "NovOApex API"`) && strings.Contains(body, `"/orders/{id}/fulfillment"`))
	code, _ = r.do("GET", "/admin/queues", nil, "")
	r.check("queue dashboard requires basic auth (401)", code == 401)
	dashReq, _ := http.NewRequestWithContext(r.ctx, http.MethodGet, r.api+"/admin/queues", nil)
	dashReq.SetBasicAuth(dashboardUser, dashboardPass)
	code, body = send(dashReq)
	r.check("queue dashboard served with valid credentials", code == 200 && strings.Contains(strings.ToLower(body), "<html"))
	dashAPI, _ := http.NewRequestWithContext(r.ctx, http.MethodGet, r.api+"/admin/queues/api/queues", nil)
	dashAPI.SetBasicAuth(dashboardUser, dashboardPass)
	code, body = send(dashAPI)
	r.check("queue dashboard reads live queue state from Redis", code == 200 && strings.Contains(body, "queues"), body)
	r.check("queue dashboard lists every declared queue, including example-queue", strings.Contains(body, `"example-queue"`) && strings.Contains(body, `"payment-events"`), body)
	_, body = r.do("GET", "/metrics", nil, "")
	r.check("/metrics exposes Go runtime and process metrics", strings.Contains(body, "go_goroutines") && strings.Contains(body, "process_resident_memory_bytes"))
	r.check("requests are logged pino-http style without auth headers", r.waitFor(5*time.Second, func() bool {
		raw, _ := os.ReadFile(filepath.Join(r.logDir, "api.log"))
		log := string(raw)
		return strings.Contains(log, `"msg":"request completed"`) && strings.Contains(log, `"url":"/api-docs-json"`) &&
			strings.Contains(log, `"statusCode":401`) && !strings.Contains(log, "Basic ")
	}))

	r.section("Example queue")
	r.enqueueExample("example-e2e-1", "e2e-hello", map[string]any{"hello": "e2e"})
	r.check("example-queue jobs are processed and logged by the ExampleProcessor", r.waitFor(15*time.Second, func() bool {
		raw, _ := os.ReadFile(filepath.Join(r.logDir, "worker.log"))
		return strings.Contains(string(raw), `Processing job example-e2e-1 (e2e-hello): {\"hello\":\"e2e\"}`) &&
			strings.Contains(string(raw), `"processor":"ExampleProcessor"`)
	}))

	r.section("Vendor onboarding")
	code, body = r.do("POST", "/auth/request-otp", map[string]any{"phone": ownerPhone, "deliveryMethod": "pigeon"}, "")
	r.check("request-otp validates the delivery method (400)", code == 400 && strings.Contains(body, "Validation failed"), body)
	code, body = r.do("POST", "/auth/request-otp", map[string]any{"phone": ownerPhone, "deliveryMethod": "whatsapp"}, "")
	r.check("request-otp over WhatsApp", code == 200, body)
	otp := ""
	for _, m := range r.mock.messagesTo(ownerPhone) {
		if match := regexp.MustCompile(`\*(\d{6})\*`).FindStringSubmatch(m.Text); match != nil {
			otp = match[1]
		}
	}
	r.check("OTP delivered through Meta with the platform phone number id", otp != "")
	code, body = r.do("POST", "/auth/verify-otp", map[string]any{"phone": ownerPhone, "code": otp}, "")
	var verify struct {
		Token     string `json:"token"`
		IsNewUser bool   `json:"isNewUser"`
	}
	_ = json.Unmarshal([]byte(body), &verify)
	r.check("verify-otp issues a token for a new user", code == 200 && verify.Token != "" && verify.IsNewUser, body)
	r.token = verify.Token

	code, _ = r.do("GET", "/orders", nil, r.token)
	r.check("business routes 403 before a business exists", code == 403)
	code, body = r.do("POST", "/businesses", map[string]any{"name": "Ama's Naturals", "currency": "GHS", "whatsappPhoneNumberId": waPhoneNumberID, "category": "beauty"}, r.token)
	businessID := jsonString(body, "id")
	r.check("POST /businesses creates the tenant", code == 201 && businessID != "", body)
	code, body = r.do("GET", "/orders", nil, r.token)
	r.check("the SAME pre-onboarding token now reaches business routes (businessId derived per request)", code == 200, body)
	code, body = r.do("GET", "/auth/me", nil, r.token)
	r.check("GET /auth/me returns the business", code == 200 && strings.Contains(body, businessID))

	code, body = r.do("POST", "/locations", map[string]any{"name": "Osu Shop", "address": "12 Oxford St", "openingTime": "08:00", "closingTime": "18:00"}, r.token)
	r.check("POST /locations", code == 201 && jsonString(body, "id") != "", body)

	r.section("Catalog + embeddings")
	code, body = r.do("POST", "/products", map[string]any{"name": "Shea Butter", "description": "Raw unrefined shea", "price": 45, "stock": 10, "category": "skin care"}, r.token)
	productID := jsonString(body, "id")
	r.check("POST /products", code == 201 && productID != "", body)
	r.check("product text embedding is written by the worker", r.waitFor(30*time.Second, func() bool {
		return r.scalar(`SELECT count(*) FROM products WHERE id = $1 AND embedding IS NOT NULL`, productID) == "1"
	}))
	code, body = r.upload("/products/"+productID+"/images", "files", testJPEG(color.RGBA{R: 200, G: 160, B: 90, A: 255}))
	r.check("POST /products/:id/images uploads to storage", code == 201 && strings.Contains(body, "/img/e2e-"), body)
	r.check("product image embedding is written by the worker (Gemini)", r.waitFor(30*time.Second, func() bool {
		return r.scalar(`SELECT count(*) FROM product_images WHERE product_id = $1 AND embedding IS NOT NULL`, productID) == "1"
	}))
	code, body = r.do("GET", "/products/metrics", nil, r.token)
	r.check("GET /products/metrics", code == 200 && strings.Contains(body, `"totalInventoryValue":450`), body)

	r.section("WhatsApp webhook ingest")
	code, body = r.do("GET", "/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token="+waVerifyToken+"&hub.challenge=ch-42", nil, "")
	r.check("Meta verification handshake echoes the challenge", code == 200 && body == "ch-42", body)
	code, _ = r.webhook("wamid.bad", customerPhone, "text", map[string]any{"body": "forged"}, "sha256=deadbeef")
	r.check("unsigned/forged webhook is rejected (403)", code == 403)

	r.section("Conversation: product photo request")
	r.sendText("wamid.c1", customerPhone, "Hi! Do you have shea butter? Send a photo please")
	r.check("orchestrator replied with product image then text", r.waitFor(40*time.Second, func() bool {
		msgs := r.mock.messagesTo(customerPhone)
		return len(msgs) >= 2 && msgs[0].Type == "image" && msgs[1].Type == "text"
	}))
	msgs := r.mock.messagesTo(customerPhone)
	if len(msgs) >= 2 {
		r.check("image carries the product caption and stored image URL", strings.HasPrefix(msgs[0].Caption, "Shea Butter — GH₵45.00") && strings.Contains(msgs[0].ImageLink, "/img/e2e-"), msgs[0])
		r.check("messages were sent from the tenant's phone number id", msgs[0].PhoneNumberID == waPhoneNumberID && msgs[1].PhoneNumberID == waPhoneNumberID)
	}
	calls := r.mock.llmCalls()
	r.check("LLM saw the catalog with the product and the customer's words",
		len(calls) == 1 && strings.Contains(calls[0].System, "Shea Butter") && strings.Contains(calls[0].LastUser, "Send a photo"))
	r.check("customer + profile + conversation captured (stealth CRM)",
		r.scalar(`SELECT count(*) FROM customers c JOIN customer_profiles p ON p.customer_id = c.id WHERE c.business_id = $1 AND c.phone = $2`, businessID, customerPhone) == "1")
	r.check("conversation moved LEAD → BROWSING", r.waitFor(10*time.Second, func() bool {
		return r.scalar(`SELECT state FROM conversations WHERE business_id = $1 AND customer_phone = $2`, businessID, customerPhone) == "BROWSING"
	}))
	r.check("outbound rows are marked sent with Meta ids",
		r.scalar(`SELECT count(*) FROM outbound_messages WHERE recipient_phone = $1 AND status = 'sent' AND whatsapp_message_id LIKE 'wamid.e2e.%'`, customerPhone) == "2")
	r.check("preferences accumulated by the CRM materialiser", r.waitFor(10*time.Second, func() bool {
		return strings.Contains(r.scalar(`SELECT p.preferences::text FROM customer_profiles p JOIN customers c ON c.id = p.customer_id WHERE c.phone = $1`, customerPhone), "skin care")
	}))

	llmBefore := r.mock.callCount("llm")
	r.sendText("wamid.c1", customerPhone, "Hi! Do you have shea butter? Send a photo please")
	time.Sleep(6 * time.Second)
	r.check("a redelivered webhook (same message id) is ignored", r.mock.callCount("llm") == llmBefore && r.scalar(`SELECT count(*) FROM inbound_messages WHERE whatsapp_message_id = 'wamid.c1'`) == "1")

	r.section("Conversation: burst debounce")
	llmBefore = r.mock.callCount("llm")
	r.sendText("wamid.b1", customerPhone, "hello")
	r.sendText("wamid.b2", customerPhone, "hello again")
	r.sendText("wamid.b3", customerPhone, "anyone there?")
	time.Sleep(9 * time.Second)
	r.check("three rapid messages collapse into one LLM run", r.mock.callCount("llm") == llmBefore+1, r.mock.callCount("llm")-llmBefore)

	r.section("Conversation: order confirmation → invoice")
	sentBefore := len(r.mock.messagesTo(customerPhone))
	r.sendText("wamid.c2", customerPhone, "Yes, confirm 2 please. I'm Ama, deliver to East Legon")
	var orderID string
	r.check("order created from the confirmed turn", r.waitFor(40*time.Second, func() bool {
		orderID = r.scalar(`SELECT id FROM orders WHERE business_id = $1 AND idempotency_key = 'wamid.c2'`, businessID)
		return orderID != ""
	}))
	r.check("order total, status and currency", r.scalar(`SELECT status || ' ' || total_amount || ' ' || currency FROM orders WHERE id = $1`, orderID) == "CONFIRMED 90.00 GHS")
	r.check("stock decremented 10 → 8", r.scalar(`SELECT stock FROM products WHERE id = $1`, productID) == "8")
	r.check("payment link delivered after the confirmation reply", r.waitFor(20*time.Second, func() bool {
		replyAt, linkAt := -1, -1
		for i, m := range r.mock.messagesTo(customerPhone)[sentBefore:] {
			if strings.HasPrefix(m.Text, "Your order is confirmed!") {
				replyAt = i
			}
			if strings.Contains(m.Text, "https://checkout.paystack.mock/"+orderID) && strings.Contains(m.Text, "GHS 90.00") {
				linkAt = i
			}
		}
		return replyAt >= 0 && linkAt > replyAt
	}))
	r.check("conversation moved to INVOICING", r.scalar(`SELECT state FROM conversations WHERE business_id = $1 AND customer_phone = $2`, businessID, customerPhone) == "INVOICING")
	r.check("three follow-ups scheduled (2h/24h/48h)", r.scalar(`SELECT count(*) FROM scheduled_follow_ups WHERE order_id = $1 AND executed_at IS NULL AND cancelled_at IS NULL`, orderID) == "3")
	r.check("customer name, delivery area, opt-in and order stats recorded", r.waitFor(10*time.Second, func() bool {
		return r.scalar(`SELECT c.name || '|' || p.delivery_area || '|' || c.marketing_opt_in || '|' || p.total_orders || '|' || p.total_spent
			FROM customers c JOIN customer_profiles p ON p.customer_id = c.id WHERE c.phone = $1`, customerPhone) == "Ama Mensah|East Legon|true|1|90.00"
	}))

	r.section("Payments")
	sentBefore = len(r.mock.messagesTo(customerPhone))
	payload := map[string]any{"event": "charge.success", "data": map[string]any{
		"reference": orderID, "amount": 9000, "currency": "GHS", "paid_at": time.Now().UTC().Format(time.RFC3339),
		"customer":      map[string]any{"phone": customerPhone},
		"authorization": map[string]any{"bank": "MTN"},
		"metadata":      map[string]any{"businessId": businessID},
	}}
	raw, _ := json.Marshal(payload)
	code, _ = r.rawPost("/webhooks/payments/paystack", raw, map[string]string{"X-Paystack-Signature": "bad"})
	r.check("payment webhook with a bad signature is rejected (400)", code == 400)
	code, body = r.rawPost("/webhooks/payments/paystack", raw, map[string]string{"X-Paystack-Signature": hmacHex(sha512.New, paystackSecret, raw)})
	r.check("signed Paystack charge.success accepted", code == 200, body)
	r.check("order reconciled to PAID with a SUCCESS payment", r.waitFor(30*time.Second, func() bool {
		return r.scalar(`SELECT o.status || ' ' || p.status || ' ' || p.amount || ' ' || p.network FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1`, orderID) == "PAID SUCCESS 90.00 MTN"
	}))
	r.check("pending follow-ups cancelled after payment", r.scalar(`SELECT count(*) FROM scheduled_follow_ups WHERE order_id = $1 AND cancelled_at IS NOT NULL`, orderID) == "3")
	r.check("payment confirmation delivered to the customer", r.waitFor(20*time.Second, func() bool {
		for _, m := range r.mock.messagesTo(customerPhone)[sentBefore:] {
			if strings.Contains(m.Text, "Payment of *GHS 90.00* received") {
				return true
			}
		}
		return false
	}))
	r.check("preferred payment network recorded", r.scalar(`SELECT p.preferred_payment_network FROM customer_profiles p JOIN customers c ON c.id = p.customer_id WHERE c.phone = $1`, customerPhone) == "MTN")

	r.section("Vendor app reads")
	for _, path := range []string{"/orders", "/orders/" + orderID, "/orders/summary", "/customers", "/customers/summary", "/inbox/summary", "/inbox/handoffs",
		"/dashboard/summary", "/analytics/overview", "/analytics/handoff-reasons", "/analytics/demand", "/products", "/products/" + productID, "/locations", "/payouts/history", "/"} {
		code, body = r.do("GET", path, nil, r.token)
		r.check("GET "+path, code == 200, body)
	}
	_, body = r.do("GET", "/orders/summary", nil, r.token)
	r.check("orders summary counts the paid order", strings.Contains(body, `"paidOrders":1`) && strings.Contains(body, `"totalRevenue":90`), body)
	convID := r.scalar(`SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2`, businessID, customerPhone)
	code, body = r.do("GET", "/conversations/"+convID+"/messages", nil, r.token)
	r.check("conversation thread merges inbound + outbound", code == 200 && strings.Contains(body, `"type":"inbound"`) && strings.Contains(body, `"type":"outbound"`))
	customerID := r.scalar(`SELECT id FROM customers WHERE business_id = $1 AND phone = $2`, businessID, customerPhone)
	code, body = r.do("GET", "/customers/"+customerID, nil, r.token)
	r.check("customer detail includes the profile", code == 200 && strings.Contains(body, `"deliveryArea":"East Legon"`), body)

	r.section("Fulfillment follow-up (cron)")
	code, body = r.do("PATCH", "/orders/"+orderID+"/fulfillment", map[string]any{"status": "DELIVERED"}, r.token)
	r.check("PATCH fulfillment → DELIVERED", code == 200, body)
	r.check("delivery-confirmation follow-up scheduled", r.scalar(`SELECT count(*) FROM scheduled_follow_ups WHERE order_id = $1 AND job_type = 'delivery-confirmation'`, orderID) == "1")
	r.exec(`UPDATE scheduled_follow_ups SET scheduled_at = now() - interval '1 minute' WHERE order_id = $1 AND job_type = 'delivery-confirmation'`, orderID)
	sentBefore = len(r.mock.messagesTo(customerPhone))
	r.triggerCron("cron:follow-up-scanner")
	r.check("follow-up scanner + follow-up worker deliver the check-in", r.waitFor(30*time.Second, func() bool {
		for _, m := range r.mock.messagesTo(customerPhone)[sentBefore:] {
			if strings.Contains(m.Text, "marked as delivered") {
				return true
			}
		}
		return false
	}))

	r.section("Human handoff")
	sentBefore = len(r.mock.messagesTo(customerPhone))
	code, body = r.do("POST", "/conversations/"+convID+"/reply", map[string]any{"text": "Hi Ama, this is the owner. Thanks!"}, r.token)
	r.check("vendor reply is sent synchronously (201, status sent, Meta id)", code == 201 && strings.Contains(body, `"status":"sent"`) && strings.Contains(body, "wamid.e2e."), body)
	delivered := false
	for _, m := range r.mock.messagesTo(customerPhone)[sentBefore:] {
		if m.Text == "Hi Ama, this is the owner. Thanks!" {
			delivered = true
		}
	}
	r.check("vendor reply reached Meta before the response returned", delivered)
	code, _ = r.do("POST", "/conversations/"+convID+"/takeover", nil, r.token)
	r.check("takeover", code == 201)
	llmBefore = r.mock.callCount("llm")
	sentBefore = len(r.mock.messagesTo(customerPhone))
	r.sendText("wamid.c3", customerPhone, "hello? anyone?")
	// The orchestrator links the inbound row to its conversation as its first
	// step, so a linked row proves the debounced run actually happened.
	ran := r.waitFor(30*time.Second, func() bool {
		return r.scalar(`SELECT count(*) FROM inbound_messages WHERE whatsapp_message_id = 'wamid.c3' AND conversation_id IS NOT NULL`) == "1"
	})
	time.Sleep(2 * time.Second)
	r.check("automation pauses while a human has the conversation (run happened, no LLM call, no reply)",
		ran && r.mock.callCount("llm") == llmBefore && len(r.mock.messagesTo(customerPhone)) == sentBefore)
	code, _ = r.do("POST", "/conversations/"+convID+"/release", nil, r.token)
	r.check("release", code == 201)

	r.section("Second customer: voice note, safety, failure recovery")
	r.sendMedia("wamid.v1", customer2Phone, "audio", "media-audio-1")
	r.check("voice note downloaded, transcribed and answered", r.waitFor(40*time.Second, func() bool {
		for _, m := range r.mock.messagesTo(customer2Phone) {
			if m.Text == "Shea Butter is GH₵45." {
				return true
			}
		}
		return false
	}))
	lastLLM := r.mock.llmCalls()
	r.check("the LLM received the transcript as the customer's turn",
		len(lastLLM) > 0 && strings.Contains(lastLLM[len(lastLLM)-1].LastUser, "how much is the shea butter"))
	r.check("transcript persisted on the inbound message",
		strings.Contains(r.scalar(`SELECT text_content FROM inbound_messages WHERE whatsapp_message_id = 'wamid.v1'`), "how much"))

	sentBefore = len(r.mock.messagesTo(customer2Phone))
	r.sendText("wamid.v2", customer2Phone, "please llm-error now")
	hitOutage := r.waitFor(30*time.Second, func() bool {
		for _, c := range r.mock.llmCalls() {
			if strings.Contains(c.LastUser, "llm-error") {
				return true
			}
		}
		return false
	})
	time.Sleep(2 * time.Second)
	r.check("an LLM outage fails the run without replying",
		hitOutage && len(r.mock.messagesTo(customer2Phone)) == sentBefore)
	llmBefore = r.mock.callCount("llm")
	r.sendText("wamid.v3", customer2Phone, "how much again?")
	r.check("the next message after a failed run is still processed (debounce id freed)", r.waitFor(30*time.Second, func() bool {
		return r.mock.callCount("llm") > llmBefore
	}))

	sentBefore = len(r.mock.messagesTo(customer2Phone))
	llmBefore = r.mock.callCount("llm")
	r.sendText("wamid.v4", customer2Phone, "I want a refund, this is a scam")
	r.check("escalation keyword hands off to a human without calling the LLM", r.waitFor(20*time.Second, func() bool {
		for _, m := range r.mock.messagesTo(customer2Phone)[sentBefore:] {
			if m.Text == "You are being connected to a human agent. Please wait." {
				return true
			}
		}
		return false
	}) && r.mock.callCount("llm") == llmBefore)
	r.check("escalation reason recorded", r.scalar(`SELECT is_escalated_to_human || ' ' || escalation_reason FROM conversations WHERE customer_phone = $1`, customer2Phone) == "true KEYWORD_MATCH")

	r.section("Outbox recovery, retention, new arrivals (crons)")
	// A quiet conversation (no newer sends) gets its stranded row delivered;
	// a busy one (newer message already sent) gets it marked stale instead of
	// delivering out of order.
	const quietPhone = "233244555666"
	quietConv := "conv-quiet-e2e"
	r.exec(`INSERT INTO conversations (id, business_id, customer_phone, updated_at) VALUES ($1, $2, $3, now())`, quietConv, businessID, quietPhone)
	r.exec(`INSERT INTO outbound_messages (id, business_id, conversation_id, recipient_phone, message_type, text_content, raw_payload, status, created_at)
		VALUES ('om-stranded-quiet', $1, $2, $3, 'text', 'recovered from the outbox', '{}', 'pending', now() - interval '5 minutes')`, businessID, quietConv, quietPhone)
	r.exec(`INSERT INTO outbound_messages (id, business_id, conversation_id, recipient_phone, message_type, text_content, raw_payload, status, created_at)
		VALUES ('om-stranded-busy', $1, $2, $3, 'text', 'superseded', '{}', 'pending', now() - interval '5 minutes')`, businessID, convID, customerPhone)
	r.triggerCron("cron:outbox-sweep")
	r.check("stranded pending outbound is re-enqueued and delivered", r.waitFor(30*time.Second, func() bool {
		return r.scalar(`SELECT status FROM outbound_messages WHERE id = 'om-stranded-quiet'`) == "sent" && len(r.mock.messagesTo(quietPhone)) == 1
	}))
	r.check("stranded outbound superseded by a newer send is marked failed_stale, not sent",
		r.scalar(`SELECT status FROM outbound_messages WHERE id = 'om-stranded-busy'`) == "failed_stale")

	r.exec(`UPDATE customer_profiles SET last_order_at = now() - interval '90 days', order_frequency_days = 10, last_reengagement_at = NULL WHERE customer_id = $1`, customerID)
	sentBefore = len(r.mock.messagesTo(customerPhone))
	r.triggerCron("cron:retention-scanner")
	r.check("retention scanner re-engages a lapsed customer", r.waitFor(30*time.Second, func() bool {
		for _, m := range r.mock.messagesTo(customerPhone)[sentBefore:] {
			if strings.Contains(m.Text, "it's been a while") {
				return true
			}
		}
		return false
	}))

	r.exec(`UPDATE businesses SET new_arrivals_template_name = 'new_arrivals_v1', last_new_arrivals_notified_at = NULL WHERE id = $1`, businessID)
	r.triggerCron("cron:new-arrivals-scanner")
	r.check("new-arrivals digest sent as a template to the opted-in customer", r.waitFor(30*time.Second, func() bool {
		for _, m := range r.mock.messagesTo(customerPhone) {
			if m.Type == "template" && m.Template == "new_arrivals_v1" {
				return true
			}
		}
		return false
	}))

	r.section("Delivery failure handling")
	failID := "om-meta-reject-e2e"
	r.exec(`INSERT INTO outbound_messages (id, business_id, conversation_id, recipient_phone, message_type, text_content, raw_payload, status)
		VALUES ($1, $2, $3, $4, 'text', 'meta-reject this one', '{}', 'pending')`, failID, businessID, convID, customerPhone)
	r.enqueueOutbound(failID)
	r.check("Meta rejection is retried then marked failed with the error", r.waitFor(60*time.Second, func() bool {
		return r.scalar(`SELECT status FROM outbound_messages WHERE id = $1`, failID) == "failed" &&
			strings.Contains(r.scalar(`SELECT meta_response->>'error' FROM outbound_messages WHERE id = $1`, failID), "131047")
	}))

	r.section("Payouts")
	code, body = r.do("GET", "/payouts/balance", nil, r.token)
	r.check("balance reflects the settled payment", code == 200 && strings.Contains(body, `"availableBalance":90`), body)
	code, body = r.do("POST", "/payouts/request", map[string]any{"amount": 500, "accountNumber": "0244000000", "bankCode": "MTN", "accountName": "Ama Mensah"}, r.token)
	r.check("over-balance payout rejected (400)", code == 400, body)
	code, body = r.do("POST", "/payouts/request", map[string]any{"amount": 50, "accountNumber": "0244000000", "bankCode": "MTN", "accountName": "Ama Mensah"}, r.token)
	r.check("payout executed through Paystack transfer", (code == 200 || code == 201) && strings.Contains(body, `"status":"SUCCESS"`) && strings.Contains(body, "TRF_e2e"), body)
	_, body = r.do("GET", "/payouts/balance", nil, r.token)
	r.check("balance reduced by the payout", strings.Contains(body, `"availableBalance":40`), body)
	code, body = r.do("POST", "/messages/send/text", map[string]any{"to": "233201234567", "text": "manual test"}, r.token)
	r.check("authenticated /messages/send/text works", code == 201, body)

	r.sentryChecks()

	r.section("Standalone mobile API (-mobile)")
	mobilePort := freePort()
	mobile := startServiceArgs(r.bin["api"], []string{"-mobile"}, append(r.env, "MOBILE_API_PORT="+strconv.Itoa(mobilePort)), filepath.Join(r.logDir, "mobile-api.log"))
	defer func() { _ = mobile.Process.Kill() }()
	mobileURL := "http://127.0.0.1:" + strconv.Itoa(mobilePort)
	mobileGet := func(path, token string) (int, string) {
		req, _ := http.NewRequestWithContext(r.ctx, http.MethodGet, mobileURL+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		return send(req)
	}
	r.check("mobile API listens on MOBILE_API_PORT", r.waitFor(30*time.Second, func() bool {
		code, _ := mobileGet("/health/live", "")
		return code == 200
	}))
	code, body = mobileGet("/orders/summary", r.token)
	r.check("mobile API serves vendor routes with the same session", code == 200 && strings.Contains(body, `"totalOrders":1`), body)
	code, _ = mobileGet("/webhooks/whatsapp?hub.mode=subscribe", "")
	r.check("mobile API does not serve webhooks", code == 404)
	code, _ = mobileGet("/api-docs-json", "")
	r.check("mobile API does not serve API docs", code == 404)
	r.check("mobile API exits cleanly on SIGTERM", stopService(mobile, 15*time.Second))
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (r *runner) section(name string) { fmt.Printf("\n== %s\n", name) }

func (r *runner) check(name string, ok bool, detail ...any) {
	r.steps++
	if ok {
		fmt.Printf("  PASS  %s\n", name)
		return
	}
	r.failures++
	msg := ""
	if len(detail) > 0 {
		msg = fmt.Sprintf("  → %.400v", detail[0])
	}
	fmt.Printf("  FAIL  %s%s\n", name, msg)
}

func (r *runner) waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return cond()
}

func (r *runner) do(method, path string, body any, token string) (int, string) {
	var rdr io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	}
	req, _ := http.NewRequestWithContext(r.ctx, method, r.api+path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return send(req)
}

func (r *runner) rawPost(path string, raw []byte, headers map[string]string) (int, string) {
	req, _ := http.NewRequestWithContext(r.ctx, http.MethodPost, r.api+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return send(req)
}

func (r *runner) upload(path, field string, data []byte) (int, string) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename="shea.jpg"`, field))
	h.Set("Content-Type", "image/jpeg")
	part, _ := w.CreatePart(h)
	_, _ = part.Write(data)
	_ = w.Close()
	req, _ := http.NewRequestWithContext(r.ctx, http.MethodPost, r.api+path, &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+r.token)
	return send(req)
}

func send(req *http.Request) (int, string) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func (r *runner) webhook(id, from, msgType string, content map[string]any, signature string) (int, string) {
	msg := map[string]any{"from": from, "id": id, "timestamp": strconv.FormatInt(time.Now().Unix(), 10), "type": msgType, msgType: content}
	body := map[string]any{"object": "whatsapp_business_account", "entry": []any{map[string]any{
		"id": "WABA-E2E", "changes": []any{map[string]any{"field": "messages", "value": map[string]any{
			"messaging_product": "whatsapp",
			"metadata":          map[string]any{"display_phone_number": "233300000000", "phone_number_id": waPhoneNumberID},
			"contacts":          []any{map[string]any{"wa_id": from, "profile": map[string]any{"name": "Customer"}}},
			"messages":          []any{msg},
		}}},
	}}}
	raw, _ := json.Marshal(body)
	if signature == "" {
		signature = "sha256=" + hmacHex(sha256.New, waAppSecret, raw)
	}
	return r.rawPost("/webhooks/whatsapp", raw, map[string]string{"X-Hub-Signature-256": signature})
}

func (r *runner) sendText(id, from, text string) {
	if code, body := r.webhook(id, from, "text", map[string]any{"body": text}, ""); code != 200 {
		r.check("webhook "+id+" accepted", false, body)
	}
}

func (r *runner) sendMedia(id, from, kind, mediaID string) {
	if code, body := r.webhook(id, from, kind, map[string]any{"id": mediaID, "mime_type": "audio/ogg; codecs=opus"}, ""); code != 200 {
		r.check("webhook "+id+" accepted", false, body)
	}
}

// scalar returns a single-value query as text ("" for no row or NULL).
func (r *runner) scalar(q string, args ...any) string {
	var v *string
	if err := r.db.QueryRow(r.ctx, `SELECT (`+q+`)::text`, args...).Scan(&v); err != nil || v == nil {
		return ""
	}
	return *v
}

func (r *runner) exec(q string, args ...any) {
	if _, err := r.db.Exec(r.ctx, q, args...); err != nil {
		r.check("setup SQL: "+q, false, err)
	}
}

// triggerCron enqueues a cron tick now instead of waiting for its schedule.
func (r *runner) triggerCron(taskType string) {
	c := asynq.NewClient(r.redis)
	defer func() { _ = c.Close() }()
	if _, err := c.EnqueueContext(r.ctx, asynq.NewTask(taskType, nil), asynq.MaxRetry(0)); err != nil {
		r.check("trigger "+taskType, false, err)
	}
}

func (r *runner) enqueueExample(id, name string, data any) {
	c := asynq.NewClient(r.redis)
	defer func() { _ = c.Close() }()
	payload, _ := json.Marshal(data)
	_, err := c.EnqueueContext(r.ctx, asynq.NewTask("example-queue:"+name, payload),
		asynq.Queue("example-queue"), asynq.MaxRetry(0), asynq.TaskID(id))
	if err != nil {
		r.check("enqueue example "+id, false, err)
	}
}

func (r *runner) enqueueOutbound(id string) {
	c := asynq.NewClient(r.redis)
	defer func() { _ = c.Close() }()
	payload, _ := json.Marshal(map[string]string{"outboundMessageId": id})
	_, err := c.EnqueueContext(r.ctx, asynq.NewTask("outbound-queue:send-message", payload),
		asynq.Queue("outbound-queue"), asynq.MaxRetry(2), asynq.TaskID("outbound:"+id))
	if err != nil {
		r.check("enqueue outbound "+id, false, err)
	}
}

func hmacHex(h func() hash.Hash, secret string, body []byte) string {
	mac := hmac.New(h, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func jsonString(body, key string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return ""
	}
	s, _ := m[key].(string)
	return s
}

// ---------------------------------------------------------------------------
// process + infra plumbing
// ---------------------------------------------------------------------------

func resetDatabase(ctx context.Context, adminDSN, name string) string {
	admin, err := pgx.Connect(ctx, adminDSN)
	must(err)
	defer func() { _ = admin.Close(ctx) }()
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, name)
	_, err = admin.Exec(ctx, `DROP DATABASE IF EXISTS `+name)
	must(err)
	_, err = admin.Exec(ctx, `CREATE DATABASE `+name)
	must(err)
	u, err := url.Parse(adminDSN)
	must(err)
	u.Path = "/" + name
	return u.String()
}

// flushRedis removes every queue left over from a previous run.
func flushRedis(opt asynq.RedisClientOpt) {
	inspector := asynq.NewInspector(opt)
	defer func() { _ = inspector.Close() }()
	queues, _ := inspector.Queues()
	for _, q := range queues {
		_ = inspector.DeleteQueue(q, true)
	}
}

func buildBinaries(dir string) map[string]string {
	out := map[string]string{}
	for _, name := range []string{"migrate", "api", "worker"} {
		path := filepath.Join(dir, name)
		cmd := exec.Command("go", "build", "-o", path, "./cmd/"+name)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		must(cmd.Run())
		out[name] = path
	}
	return out
}

func serviceEnv(dsn, redisAddr, redisPass, mockURL string) []string {
	host, port, _ := net.SplitHostPort(redisAddr)
	return append(os.Environ(),
		"NODE_ENV=production",
		"LOG_LEVEL=debug",
		"DATABASE_URL="+dsn,
		"REDIS_HOST="+host, "REDIS_PORT="+port, "REDIS_PASSWORD="+redisPass,
		"JWT_SECRET="+jwtSecret,
		"WHATSAPP_VERIFY_TOKEN="+waVerifyToken,
		"WHATSAPP_APP_SECRET="+waAppSecret,
		"WHATSAPP_ACCESS_TOKEN=wa-token-e2e",
		"WHATSAPP_PHONE_NUMBER_ID=PNID-PLATFORM",
		"WHATSAPP_BASE_URL="+mockURL+"/graph",
		"OPENAI_API_KEY=sk-openai-e2e",
		"OPENAI_BASE_URL="+mockURL+"/openai",
		"GOOGLE_GENERATIVE_AI_API_KEY=g-e2e",
		"GOOGLE_GENERATIVE_AI_BASE_URL="+mockURL+"/google",
		"PAYSTACK_SECRET_KEY="+paystackSecret,
		"PAYSTACK_BASE_URL="+mockURL+"/paystack",
		"CLOUDINARY_CLOUD_NAME=e2e-cloud", "CLOUDINARY_API_KEY=key", "CLOUDINARY_API_SECRET=secret",
		"CLOUDINARY_BASE_URL="+mockURL+"/cloudinary",
		"ENABLE_SWAGGER=true",
		"BULL_BOARD_USER="+dashboardUser,
		"BULL_BOARD_PASSWORD="+dashboardPass,
		"SENTRY_DSN="+strings.Replace(mockURL, "http://", "http://e2epublickey@", 1)+"/sentry/1",
		"SENTRY_ENVIRONMENT=e2e",
		"SENTRY_TRACES_SAMPLE_RATE=1.0",
		"SENTRY_PROFILES_SAMPLE_RATE=1.0",
	)
}

func runOnce(bin string, env []string, logPath string) error {
	f, err := os.Create(logPath)
	must(err)
	defer func() { _ = f.Close() }()
	cmd := exec.Command(bin)
	cmd.Env, cmd.Stdout, cmd.Stderr = env, f, f
	return cmd.Run()
}

func startService(bin string, env []string, logPath string) *exec.Cmd {
	return startServiceArgs(bin, nil, env, logPath)
}

func startServiceArgs(bin string, args, env []string, logPath string) *exec.Cmd {
	f, err := os.Create(logPath)
	must(err)
	cmd := exec.Command(bin, args...)
	cmd.Env, cmd.Stdout, cmd.Stderr = env, f, f
	must(cmd.Start())
	return cmd
}

func stopService(cmd *exec.Cmd, timeout time.Duration) bool {
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err == nil
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		return false
	}
}

func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		os.Exit(2)
	}
}
