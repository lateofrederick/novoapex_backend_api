package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// T5.18-style replay: ONE captured WhatsApp webhook payload posted to
//
//	a) the LIVE NODE stack (api + BullMQ worker, own Postgres+Redis), and
//	b) the Go ingest handler + asynq pipeline (own Postgres+Redis),
//
// then the resulting webhook_events / inbound_messages decoded rows are
// diffed after stripping ids/timestamps. Both databases start from the SAME
// empty migrated schema, so any behavioural divergence shows up here.
//
// Requires the Node dist build (skipped when absent — CI builds it).
func TestS5_T5_18_WebhookIngestReplayVsNodeStack(t *testing.T) {
	repoDir, err := harness.NovoApexRepoDir()
	if err != nil {
		t.Skipf("novoapex repo not reachable: %v", err)
	}
	for _, bin := range []string{
		filepath.Join(repoDir, "dist/apps/api/apps/api/src/main.js"),
		filepath.Join(repoDir, "dist/apps/worker/apps/worker/src/main.js"),
	} {
		if _, err := os.Stat(bin); err != nil {
			t.Skipf("node dist missing (%s) — run npm run build:all first", bin)
		}
	}

	ctx := context.Background()

	// --- stack A: live Node over its own containers -------------------------
	hn, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start node harness: %v", err)
	}
	t.Cleanup(func() { hn.Terminate(context.Background()) })
	if err := harness.ApplyPrismaMigrations(ctx, repoDir, hn.PostgresDSN); err != nil {
		t.Fatalf("migrate node db: %v", err)
	}
	node := harness.StartNodeStack(t, hn)

	// --- stack B: Go ingest + asynq worker over its own containers ----------
	hg, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start go harness: %v", err)
	}
	t.Cleanup(func() { hg.Terminate(context.Background()) })
	if err := harness.ApplyPrismaMigrations(ctx, repoDir, hg.PostgresDSN); err != nil {
		t.Fatalf("migrate go db: %v", err)
	}
	goPool, err := pgxpool.New(ctx, hg.PostgresDSN)
	if err != nil {
		t.Fatalf("open go pool: %v", err)
	}
	t.Cleanup(goPool.Close)

	publisher := queue.NewClient(hg.RedisAddr, harness.RedisPassword)
	t.Cleanup(func() { _ = publisher.Close() })

	server := queue.NewServer(queue.ServerConfig{
		RedisAddr: hg.RedisAddr,
		RedisPass: harness.RedisPassword,
	})
	server.Register(queue.TaskWebhookProcess, s5_replayInboxWorker(goPool))
	if err := server.Start(); err != nil {
		t.Fatalf("start asynq test server: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Stop()
		_ = server.Close()
	})

	goTree := httptest.NewServer(s5_mountWebhooks(t, &epEnv{Pool: goPool}, publisher,
		s5_nodeStackAppSecret, "unused-verify-token"))
	t.Cleanup(goTree.Close)

	// --- the captured payload ------------------------------------------------
	payload := []byte(`{"object":"whatsapp_business_account","entry":[{"id":"WABA-REPLAY","changes":[{"field":"messages","value":{"messaging_product":"whatsapp","metadata":{"display_phone_number":"233201234567","phone_number_id":"wni_replay"},"contacts":[{"profile":{"name":"Ama"},"wa_id":"+233501110000"}],"messages":[{"from":"+233501110000","id":"wamid.replay.1","timestamp":"1755854400","text":{"body":"How much is milo?"},"type":"text"}]}}]}]}`)
	signature := s5_hubSign(s5_nodeStackAppSecret, payload)

	// a) POST to the live Node stack.
	nodeStatus, nodeBody, err := s5_postRaw(node.BaseURL+"/webhooks/whatsapp", payload, signature)
	if err != nil || nodeStatus != http.StatusOK || !bytes.Contains(nodeBody, []byte(`"ok"`)) {
		t.Fatalf("node status=%d body=%s err=%v\nlogs:\n%s", nodeStatus, nodeBody, err, node.DumpLogs())
	}
	s5_waitFor(t, 90*time.Second, "node inbound_messages row",
		func() bool { return s5_inboundCount(ctx, t, hn.PostgresDSN) == 1 })

	// b) POST the SAME bytes to the Go handler.
	goTreeClient := goTree.Client()
	req, _ := http.NewRequest(http.MethodPost, goTree.URL+"/webhooks/whatsapp", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hub-Signature-256", signature)
	resp, err := goTreeClient.Do(req)
	if err != nil {
		t.Fatalf("go post: %v", err)
	}
	rawBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Contains(rawBody, []byte(`"ok"`)) {
		t.Fatalf("go status=%d body=%s", resp.StatusCode, rawBody)
	}
	s5_waitFor(t, 60*time.Second, "go inbound_messages row",
		func() bool { return s5_inboundCount(ctx, t, hg.PostgresDSN) == 1 })

	// --- diff decoded rows (strip ids/timestamps) ---------------------------
	wantEvents := []string{"WABA-REPLAY:+233501110000:wamid.replay.1"}
	gotNodeEvents := s5_idempotencyKeys(ctx, t, hn.PostgresDSN)
	gotGoEvents := s5_idempotencyKeys(ctx, t, hg.PostgresDSN)
	if !reflect.DeepEqual(gotNodeEvents, wantEvents) || !reflect.DeepEqual(gotGoEvents, wantEvents) {
		t.Fatalf("webhook_events keys:\nnode=%v\ngo  =%v\nwant=%v", gotNodeEvents, gotGoEvents, wantEvents)
	}

	type replayRow struct {
		WhatsappMessageID string
		SenderPhone       string
		RecipientPhone    string
		MessageType       string
		TextContent       *string
		RawPayload        map[string]any
		TimestampUnix     int64
	}
	read := func(dsn string) replayRow {
		t.Helper()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		var row replayRow
		var raw []byte
		if err := pool.QueryRow(ctx, `
			SELECT whatsapp_message_id, sender_phone, recipient_phone, message_type,
			       text_content, raw_payload::text, EXTRACT(EPOCH FROM timestamp)::bigint
			  FROM inbound_messages LIMIT 1`).
			Scan(&row.WhatsappMessageID, &row.SenderPhone, &row.RecipientPhone,
				&row.MessageType, &row.TextContent, &raw, &row.TimestampUnix); err != nil {
			t.Fatalf("scan inbound row: %v", err)
		}
		if err := json.Unmarshal(raw, &row.RawPayload); err != nil {
			t.Fatalf("decode raw_payload: %v", err)
		}
		return row
	}

	nodeRow, goRow := read(hn.PostgresDSN), read(hg.PostgresDSN)
	if !reflect.DeepEqual(nodeRow, goRow) {
		nb, _ := json.MarshalIndent(nodeRow, "", "  ")
		gb, _ := json.MarshalIndent(goRow, "", "  ")
		t.Fatalf("inbound_messages diverge:\n--- node ---\n%s\n--- go ----\n%s", nb, gb)
	}
}

// s5_nodeStackAppSecret MUST byte-match harness.testWhatsAppAppSecret — the
// Node stack boots with it as WHATSAPP_APP_SECRET (harness/nodestack.go).
const s5_nodeStackAppSecret = "test-app-secret-0123456789abcdef"

// s5_replayInboxWorker is a test-local stand-in for the Stage 7 webhook
// processor port: the transactional-inbox half of webhook.processor.ts
// (derive fields -> insert InboundMessage). The WebhookEvent row already
// exists because THIS stage's producer-side gate wrote it; ON CONFLICT skip
// keeps the final state identical either way.
func s5_replayInboxWorker(pool *pgxpool.Pool) queue.Handler {
	return func(ctx context.Context, payload []byte) error {
		var job struct {
			IdempotencyKey string          `json:"idempotencyKey"`
			Payload        json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(payload, &job); err != nil {
			return fmt.Errorf("replay worker decode: %w", err)
		}
		var msg struct {
			ID             string      `json:"id"`
			From           string      `json:"from"`
			Type           string      `json:"type"`
			RecipientPhone string      `json:"recipientPhone"`
			Timestamp      json.Number `json:"timestamp"`
			Text           *struct {
				Body string `json:"body"`
			} `json:"text"`
			Image *struct {
				Caption string `json:"caption"`
			} `json:"image"`
		}
		if err := json.Unmarshal(job.Payload, &msg); err != nil {
			return fmt.Errorf("replay worker decode message: %w", err)
		}

		messageType := msg.Type
		if messageType == "" {
			messageType = "unknown"
		}
		var textContent *string
		switch messageType {
		case "text":
			if msg.Text != nil && msg.Text.Body != "" {
				v := msg.Text.Body
				textContent = &v
			}
		case "image":
			v := "[System: User uploaded an image without a text caption. Please analyze the image and ask how you can help.]"
			if msg.Image != nil && msg.Image.Caption != "" {
				v = msg.Image.Caption
			}
			textContent = &v
		case "audio":
			textContent = nil
		default:
			if msg.Text != nil && msg.Text.Body != "" {
				v := msg.Text.Body
				textContent = &v
			}
		}
		ts := time.Now().UTC()
		if n, err := msg.Timestamp.Int64(); err == nil && msg.Timestamp.String() != "" {
			ts = time.Unix(n, 0).UTC()
		}

		// raw_payload stores the ENRICHED message object (job.Payload).
		_, err := pool.Exec(ctx, `
			INSERT INTO inbound_messages
				(id, whatsapp_message_id, sender_phone, recipient_phone, message_type,
				 text_content, raw_payload, timestamp)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (whatsapp_message_id) DO NOTHING`,
			uuid.NewString(), msg.ID, msg.From, msg.RecipientPhone, messageType,
			textContent, job.Payload, ts)
		if err != nil {
			return fmt.Errorf("replay worker insert: %w", err)
		}
		return nil
	}
}

// ---- small helpers ----------------------------------------------------------

func s5_postRaw(url string, body []byte, signature string) (int, []byte, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	// Meta posts application/json; without it Express' json parser (and the
	// Nest rawBody capture) never runs.
	req.Header.Set("Content-Type", "application/json")
	if signature != "" {
		req.Header.Set("X-Hub-Signature-256", signature)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	return resp.StatusCode, raw, err
}

func s5_waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func s5_countQuery(ctx context.Context, t *testing.T, dsn, query string, args ...any) int {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func s5_inboundCount(ctx context.Context, t *testing.T, dsn string) int {
	t.Helper()
	return s5_countQuery(ctx, t, dsn, `SELECT COUNT(*) FROM inbound_messages`)
}

func s5_idempotencyKeys(ctx context.Context, t *testing.T, dsn string) []string {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, `SELECT idempotency_key FROM webhook_events ORDER BY idempotency_key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	return keys
}
