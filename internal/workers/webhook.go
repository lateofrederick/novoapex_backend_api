// webhook.go ports libs/queue/src/processors/webhook.processor.ts: the
// webhook-processing consumer. It persists one inbound WhatsApp message and
// schedules the debounced orchestrator run for that conversation.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/idempotency"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// imageWithoutCaptionText is the placeholder the source stores for a captionless
// image so the LLM still gets a user turn to answer.
const imageWithoutCaptionText = "[System: User uploaded an image without a text caption. Please analyze the image and ask how you can help.]"

// WebhookDeps carries the webhook-processing collaborators.
type WebhookDeps struct {
	Pool      *pgxpool.Pool
	Publisher queue.Publisher
}

// WebhookJob mirrors WebhookProcessingJobData
// (libs/queue/src/schemas/webhook-processing.schema.ts).
type WebhookJob struct {
	IdempotencyKey string          `json:"idempotencyKey"`
	Payload        json.RawMessage `json:"payload"`
	ReceivedAt     string          `json:"receivedAt"`
}

// RegisterWebhookProcessing attaches the webhook-processing handler.
func RegisterWebhookProcessing(reg queue.Registrar, deps WebhookDeps) {
	reg.Register(queue.TaskWebhookProcess, func(ctx context.Context, payload []byte) error {
		var job WebhookJob
		if err := json.Unmarshal(payload, &job); err != nil {
			return fmt.Errorf("webhook-processing: decode job payload: %w", errors.Join(err, asynq.SkipRetry))
		}
		retried, _ := asynq.GetRetryCount(ctx)
		return HandleWebhookJob(ctx, deps, job, retried > 0)
	})
}

// HandleWebhookJob ports WebhookProcessor.process. isRetry marks a re-run of
// a job whose previous attempt failed.
//
// Idempotency differs from the source in one deliberate way: the ingest
// handler already claimed webhook_events.idempotency_key, so the inbound row
// itself (unique whatsapp_message_id) is the processor-side guard. When a
// RETRY finds the row already persisted, the previous attempt died between
// commit and the orchestrator enqueue — the source silently skipped that case
// (dropping the reply); here the enqueue is re-attempted, which the fixed
// debounce TaskID keeps idempotent.
func HandleWebhookJob(ctx context.Context, deps WebhookDeps, job WebhookJob, isRetry bool) error {
	started := time.Now()
	slog.InfoContext(ctx, "Processing webhook job",
		"event", "webhook_job_started",
		"idempotencyKey", job.IdempotencyKey)

	var message map[string]any
	if err := json.Unmarshal(job.Payload, &message); err != nil || message == nil {
		return fmt.Errorf("webhook-processing: payload is not a JSON object: %w", errors.Join(err, asynq.SkipRetry))
	}

	whatsappMessageID := stringField(message, "id")
	senderPhone := stringField(message, "from")
	recipientPhone := stringField(message, "recipientPhone")
	messageType := stringField(message, "type")
	if messageType == "" {
		messageType = "unknown"
	}

	textContent := inboundTextContent(message, messageType)
	timestamp := inboundTimestamp(message)

	if referral, ok := message["referral"].(map[string]any); ok {
		slog.InfoContext(ctx, "WhatsApp referral data detected",
			"event", "referral_captured",
			"senderPhone", senderPhone,
			"sourceType", referral["source_type"],
			"sourceId", referral["source_id"],
			"sourceUrl", referral["source_url"],
			"headline", referral["headline"])
	}

	var textArg any
	if textContent != nil {
		textArg = *textContent
	}

	// Transactional inbox: the idempotency claim and the inbound row commit
	// together. ON CONFLICT keeps both idempotent when ingest already claimed
	// the key (the normal path) or Meta redelivered the message id.
	inserted := false
	err := pgx.BeginFunc(ctx, deps.Pool, func(tx pgx.Tx) error {
		if job.IdempotencyKey != "" {
			// Normally already claimed by ingest; recording again covers jobs
			// that reached the queue by another path.
			if _, err := idempotency.Record(ctx, tx, job.IdempotencyKey); err != nil {
				return err
			}
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO inbound_messages
				(id, whatsapp_message_id, sender_phone, recipient_phone, message_type,
				 text_content, raw_payload, timestamp)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (whatsapp_message_id) DO NOTHING`,
			uuid.NewString(), whatsappMessageID, senderPhone, recipientPhone, messageType,
			textArg, []byte(job.Payload), timestamp)
		if err != nil {
			return err
		}
		inserted = tag.RowsAffected() == 1
		return nil
	})
	if err != nil {
		return fmt.Errorf("webhook-processing: persist inbound message: %w", err)
	}

	if !inserted && !isRetry {
		slog.WarnContext(ctx, "Duplicate webhook event in processor, skipping",
			"event", "duplicate_webhook_rejected",
			"idempotencyKey", job.IdempotencyKey)
		return nil
	}

	slog.InfoContext(ctx, "Webhook job completed",
		"event", "webhook_job_completed",
		"idempotencyKey", job.IdempotencyKey,
		"durationMs", time.Since(started).Milliseconds(),
		"conversationId", senderPhone,
		"direction", "inbound",
		"messageType", messageType,
		"whatsappMessageId", whatsappMessageID,
		"senderPhone", senderPhone)

	// Outside the transaction on purpose: rapid messages in one conversation
	// collapse into ONE orchestrator run via the fixed debounce TaskID; a
	// failure here retries the job, and the retry branch above re-enqueues.
	if err := PublishOrchestratorDebounce(ctx, deps.Publisher, OrchestratorJob{
		RecipientPhone: recipientPhone,
		SenderPhone:    senderPhone,
	}); err != nil {
		slog.ErrorContext(ctx, "Failed to enqueue debounced orchestrator job",
			"event", "orchestrator_enqueue_failed",
			"errorMessage", err.Error(),
			"whatsappMessageId", whatsappMessageID)
		return fmt.Errorf("webhook-processing: enqueue orchestrator: %w", err)
	}
	return nil
}

// inboundTextContent ports the source's per-type text extraction: text body,
// image caption (or the captionless placeholder), nothing for audio (the
// orchestrator transcribes it), and text.body for anything else.
func inboundTextContent(message map[string]any, messageType string) *string {
	nested := func(key, field string) *string {
		obj, ok := message[key].(map[string]any)
		if !ok {
			return nil
		}
		s, ok := obj[field].(string)
		if !ok {
			return nil
		}
		return &s
	}
	switch messageType {
	case "image":
		if caption := nested("image", "caption"); caption != nil {
			return caption
		}
		s := imageWithoutCaptionText
		return &s
	case "audio":
		return nil
	default:
		return nested("text", "body")
	}
}

// inboundTimestamp converts Meta's unix-seconds timestamp (sent as a string)
// to UTC, falling back to now like the source.
func inboundTimestamp(message map[string]any) time.Time {
	switch v := message["timestamp"].(type) {
	case string:
		if secs, err := strconv.ParseInt(v, 10, 64); err == nil && secs > 0 {
			return time.Unix(secs, 0).UTC()
		}
	case float64:
		if v > 0 {
			return time.Unix(int64(v), 0).UTC()
		}
	}
	return time.Now().UTC()
}

func stringField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}
