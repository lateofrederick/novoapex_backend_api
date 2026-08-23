// Package handlers — Stage 5 webhook ingest (T5.1–T5.10).
//
// webhooks_whatsapp.go ports WhatsAppWebhookController
// (apps/api/src/webhooks/whatsapp.controller.ts):
//
//	GET  /webhooks/whatsapp  hub.mode/hub.verify_token/hub.challenge handshake
//	POST /webhooks/whatsapp  X-Hub-Signature-256 verified ingest -> queue
//
// Source contracts preserved verbatim:
//   - GET verify answers the RAW challenge string (Nest res.send(string) =>
//     text/html; charset=utf-8, body exactly the challenge, no JSON quoting);
//     anything else is ForbiddenException('Verification failed') rendered
//     through the AllExceptionsFilter envelope.
//   - WHEN WHATSAPP_APP_SECRET is unset the signature check is SKIPPED with a
//     warn log ('whatsapp_webhook_signature_skipped') — the config-driven
//     fail-open behaviour; F.4f decides fail-closed separately, NOT here.
//   - POST always answers {"status":"ok"} (200) after the walk, even when a
//     single enqueue fails (logged 'webhook_enqueue_failed', never surfaced).
//
// @SkipThrottle() parity: apps/api registers no per-IP throttler on this
// surface (Meta bursts from few IPs), and the Go kernel mounts none either —
// there is nothing to skip, so skip semantics hold trivially.
//
// Ingest-side idempotency (T5.7): each message inserts its WebhookEvent row
// (idempotency_key unique) ON CONFLICT DO NOTHING inside one transaction that
// also scopes the publish attempt: duplicate => no job, still ok; inserted =>
// commit only when Enqueue succeeds, else roll back so a later redelivery can
// still produce the job. The downstream processor's own create must use the
// same conflict-skip semantics because the row pre-exists (Stage 7 note).
package handlers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// WebhookDeps carries the public-ingest collaborators.
type WebhookDeps struct {
	Pool        *pgxpool.Pool
	Publisher   queue.Publisher
	AppSecret   string // WHATSAPP_APP_SECRET; "" => warn-and-skip signature verification
	VerifyToken string // WHATSAPP_VERIFY_TOKEN for the GET hub handshake

	PaystackSecret string // PAYSTACK_SECRET_KEY for the payments provider registry
}

// MountWebhooks registers the public webhook surface on r:
//
//	GET+POST /webhooks/whatsapp
//	POST     /webhooks/payments/{provider}   (delegates to MountPaymentsIngest)
func MountWebhooks(r chi.Router, d WebhookDeps) {
	MountWhatsAppIngest(r, d)
	MountPaymentsIngest(r, PaymentsIngestDeps{Publisher: d.Publisher, PaystackSecret: d.PaystackSecret})
}

// MountWhatsAppIngest registers GET+POST /webhooks/whatsapp.
func MountWhatsAppIngest(r chi.Router, d WebhookDeps) {
	r.Get("/webhooks/whatsapp", ww_verify(d.VerifyToken))
	r.Post("/webhooks/whatsapp", ww_handle(d))
}

// ww_verify ports verifyWebhook (whatsapp.controller.ts:37-54). The challenge
// is echoed as a RAW string — Nest serialises returned strings via
// res.send(text/html), NOT JSON — so no envelope, no quotes, no newline.
func ww_verify(verifyToken string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mode := q.Get("hub.mode")
		token := q.Get("hub.verify_token")
		challenge := q.Get("hub.challenge")

		if mode == "subscribe" && token == verifyToken {
			slog.Info("Webhook verified successfully")
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, challenge)
			return
		}

		slog.Warn("Webhook verification failed")
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusForbidden, "Verification failed"))
	}
}

// ww_metaEnvelope mirrors the Meta Cloud API webhook body shape we actually
// walk: {object, entry:[{id, changes:[{value:{messages[], metadata{}}}]}]}.
type ww_metaEnvelope struct {
	Object string     `json:"object"`
	Entry  []ww_entry `json:"entry"`
}

type ww_entry struct {
	ID      string      `json:"id"` // WABA id
	Changes []ww_change `json:"changes"`
}

type ww_change struct {
	Value ww_value `json:"value"`
}

type ww_value struct {
	Messages []json.RawMessage `json:"messages"`
	Metadata struct {
		PhoneNumberID string `json:"phone_number_id"`
	} `json:"metadata"`
}

// ww_jobData mirrors WebhookProcessingJobData
// (libs/queue/src/schemas/webhook-processing.schema.ts):
// {idempotencyKey, payload:<enriched message>, receivedAt}.
type ww_jobData struct {
	IdempotencyKey string          `json:"idempotencyKey"`
	Payload        json.RawMessage `json:"payload"`
	ReceivedAt     string          `json:"receivedAt"`
}

// ww_handle ports handleWebhook (whatsapp.controller.ts:78-164).
func ww_handle(d WebhookDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// T5.2: read the raw body FULLY before any decoding — the HMAC is
		// computed over the exact bytes Meta signed; re-encoded JSON would
		// drift and fail verification.
		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			httpx.WriteError(w, r, fmt.Errorf("read webhook body: %w", err))
			return
		}

		// Signature gate (whatsapp.controller.ts:84-100).
		if d.AppSecret != "" {
			if !ww_verifySignature(rawBody, r.Header.Get("X-Hub-Signature-256"), d.AppSecret) {
				slog.Warn("Invalid WhatsApp webhook signature — rejecting",
					"event", "whatsapp_webhook_invalid_signature")
				httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusForbidden, "Invalid signature"))
				return
			}
		} else {
			slog.Warn("WHATSAPP_APP_SECRET not set — skipping webhook signature verification. Set it in production.",
				"event", "whatsapp_webhook_signature_skipped")
		}

		var body ww_metaEnvelope
		if err := json.Unmarshal(rawBody, &body); err != nil {
			// Unparseable payloads cannot be walked; the controller contract
			// is ALWAYS {status:'ok'} — answering non-2xx would make Meta
			// redeliver a permanently malformed event forever.
			slog.Warn(fmt.Sprintf("Dropping unparseable WhatsApp webhook body (%d bytes): %v", len(rawBody), err))
			_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}

		// Early return (whatsapp.controller.ts:104-106).
		if body.Object != "whatsapp_business_account" {
			_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}

		for _, entry := range body.Entry {
			wabaID := entry.ID // entry.id ?? ''
			for _, change := range entry.Changes {
				value := change.Value
				recipientPhone := value.Metadata.PhoneNumberID

				for _, rawMsg := range value.Messages {
					var message map[string]any
					if err := json.Unmarshal(rawMsg, &message); err != nil {
						continue
					}
					messageID := ww_string(message, "id")
					senderPhone := ww_string(message, "from")

					// Composite idempotency key:
					// {wabaId}:{phoneNumber}:{messageId}
					idempotencyKey := wabaID + ":" + senderPhone + ":" + messageID

					// Referral capture logging (click-to-WhatsApp ads / QR);
					// keys mirror the processor's referral_captured context.
					if referral, ok := message["referral"].(map[string]any); ok {
						slog.Info("WhatsApp referral data detected",
							"event", "referral_captured",
							"senderPhone", senderPhone,
							"sourceType", referral["source_type"],
							"sourceId", referral["source_id"],
							"sourceUrl", referral["source_url"],
							"headline", referral["headline"])
					}

					// Attach extracted metadata for the processor.
					message["recipientPhone"] = recipientPhone
					enriched, err := json.Marshal(message)
					if err != nil {
						continue
					}

					job := ww_jobData{
						IdempotencyKey: idempotencyKey,
						Payload:        enriched,
						ReceivedAt:     time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"),
					}

					ww_ingestMessage(ctx, d, senderPhone, idempotencyKey, job)
				}
			}
		}

		// Always return 200 immediately after enqueuing.
		_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// ww_ingestMessage runs the per-message transactional gate: INSERT
// webhook_events ON CONFLICT DO NOTHING, scoped around the publish attempt
// (see the file comment for why the row lives on the producer side).
//
//	rows==0  -> duplicate delivery: skip publishing, still ok
//	rows==1  -> publish; success commits the row, failure rolls it back and
//	            logs 'webhook_enqueue_failed' (never surfaced to Meta)
func ww_ingestMessage(ctx context.Context, d WebhookDeps, senderPhone, idempotencyKey string, job ww_jobData) {
	if d.Pool == nil {
		slog.Error("Failed to enqueue webhook job",
			"event", "webhook_enqueue_failed",
			"idempotencyKey", idempotencyKey,
			"senderPhone", senderPhone,
			"error", "no database pool wired")
		return
	}

	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		slog.Error("Failed to enqueue webhook job",
			"event", "webhook_enqueue_failed",
			"idempotencyKey", idempotencyKey,
			"senderPhone", senderPhone,
			"error", err.Error())
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		`INSERT INTO webhook_events (id, idempotency_key, processed_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (idempotency_key) DO NOTHING`,
		uuid.NewString(), idempotencyKey)
	if err != nil {
		slog.Error("Failed to enqueue webhook job",
			"event", "webhook_enqueue_failed",
			"idempotencyKey", idempotencyKey,
			"senderPhone", senderPhone,
			"error", err.Error())
		return
	}

	if tag.RowsAffected() == 0 {
		slog.Warn("Duplicate webhook event — skipping",
			"event", "duplicate_webhook_rejected",
			"idempotencyKey", idempotencyKey)
		return
	}

	// BullMQ add() options -> EnqueueOpts: jobId + deduplication ttl 60s
	// (whatsapp.controller.ts:140-148); queue policy carries MaxRetry 2.
	err = d.Publisher.Enqueue(ctx, queue.QWebhookProcessing, queue.TaskWebhookProcess, job,
		&queue.EnqueueOpts{TaskID: idempotencyKey, MaxRetry: 2, UniqueTTL: 60 * time.Second})
	if err != nil {
		slog.Error("Failed to enqueue webhook job",
			"event", "webhook_enqueue_failed",
			"idempotencyKey", idempotencyKey,
			"senderPhone", senderPhone,
			"error", err.Error())
		return // defer rolls the idempotency row back
	}

	if err := tx.Commit(ctx); err != nil {
		slog.Error("Failed to commit webhook event",
			"event", "webhook_enqueue_failed",
			"idempotencyKey", idempotencyKey,
			"error", err.Error())
		return
	}
}

// ww_verifySignature ports verifySignature (whatsapp.controller.ts:60-76):
// sha256=<hex hmac> over the raw body, constant-time compare after a length
// pre-check. Missing body or header fails.
func ww_verifySignature(rawBody []byte, signatureHeader, appSecret string) bool {
	if len(rawBody) == 0 || signatureHeader == "" {
		return false
	}
	mac := hmac.New(sha256.New, []byte(appSecret))
	mac.Write(rawBody)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	a := []byte(expected)
	b := []byte(signatureHeader)
	if len(a) != len(b) {
		return false
	}
	return hmac.Equal(a, b)
}

// ww_string reads a string field off a decoded message object; absent or
// non-string values yield "" (`x ?? ”` semantics).
func ww_string(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}
