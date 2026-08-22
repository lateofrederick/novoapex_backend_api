package harness

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

type waTextMessage struct {
	ID        string `json:"id"`
	From      string `json:"from"`
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Text      struct {
		Body string `json:"body"`
	} `json:"text"`
}

type waPayload struct {
	Object string    `json:"object"`
	Entry  []waEntry `json:"entry"`
}

type waEntry struct {
	ID      string     `json:"id"`
	Changes []waChange `json:"changes"`
}

type waChange struct {
	Value waValue `json:"value"`
	Field string  `json:"field"`
}

type waValue struct {
	MessagingProduct string          `json:"messaging_product"`
	Metadata         waMetadata      `json:"metadata"`
	Contacts         []any           `json:"contacts,omitempty"`
	Messages         []waTextMessage `json:"messages,omitempty"`
}

type waMetadata struct {
	DisplayPhoneNumber string `json:"display_phone_number"`
	PhoneNumberID      string `json:"phone_number_id"`
}

func buildWhatsAppPayload(wabaID, phoneNumberID, messageID, fromPhone, body string) []byte {
	p := waPayload{
		Object: "whatsapp_business_account",
		Entry: []waEntry{{
			ID: wabaID,
			Changes: []waChange{{
				Field: "messages",
				Value: waValue{
					MessagingProduct: "whatsapp",
					Metadata: waMetadata{
						DisplayPhoneNumber: "233200000000",
						PhoneNumberID:      phoneNumberID,
					},
					Messages: []waTextMessage{newTextMessage(messageID, fromPhone, body)},
				},
			}},
		}},
	}
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return b
}

func newTextMessage(id, from, body string) waTextMessage {
	m := waTextMessage{ID: id, From: from, Timestamp: fmt.Sprint(time.Now().Unix()), Type: "text"}
	m.Text.Body = body
	return m
}

func signWhatsAppBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func postWebhook(t *testing.T, stack *NodeStack, payload []byte) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, stack.BaseURL+"/webhooks/whatsapp", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hub-Signature-256", signWhatsAppBody(testWhatsAppAppSecret, payload))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /webhooks/whatsapp: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	drainAndClose(resp)
	return resp.StatusCode
}

func drainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func TestT009_WebhookIdempotencySingleInboundMessage(t *testing.T) {
	h := startHarness(t)
	factory, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	biz := factory.Business()

	messageID := fmt.Sprintf("wamid.T0.9-%d", time.Now().UnixNano())
	payload := buildWhatsAppPayload(
		"waba-test-1",
		biz.WhatsAppPhoneNumberID,
		messageID,
		biz.OwnerPhone,
		"characterization idempotency probe",
	)

	code := postWebhook(t, stack, payload)
	if code != http.StatusOK {
		t.Fatalf("first POST status = %d\n--- logs ---\n%s", code, stack.DumpLogs())
	}
	code = postWebhook(t, stack, payload)
	if code != http.StatusOK {
		t.Fatalf("second POST status = %d\n--- logs ---\n%s", code, stack.DumpLogs())
	}

	waitForCount(t, db,
		`SELECT COUNT(*) FROM inbound_messages WHERE whatsapp_message_id = $1`, messageID, 1, 20*time.Second)
	waitForCount(t, db,
		`SELECT COUNT(*) FROM webhook_events WHERE idempotency_key LIKE '%' || $1`, messageID, 1, 10*time.Second)
}

func newFactoryOn(t *testing.T, h *Harness) (*Factory, *sql.DB) {
	t.Helper()
	repoDir := harnessRepoDir(t)
	applyMigrations(t, h.PostgresDSN, repoDir)

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewFactory(t, db), db
}

func waitForCount(t *testing.T, db *sql.DB, query string, arg string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int
	for time.Now().Before(deadline) {
		if err := db.QueryRow(query, arg).Scan(&got); err != nil {
			t.Fatalf("count query: %v", err)
		}
		if got == want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Errorf("count for %q never reached %d (last=%d) within %s", query, want, got, timeout)
}
