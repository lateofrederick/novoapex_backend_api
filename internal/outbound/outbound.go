// Package outbound ports libs/queue/src/processors/outbound.processor.ts: the
// delivery of outbound_messages rows. Every sender (orchestrator replies, product
// images, invoices, payment confirmations, follow-ups, new-arrivals digests,
// vendor replies) persists an outbound_messages row first and publishes only
// its id; this handler loads the row, delivers it through the WhatsApp Cloud
// API and records the outcome.
//
// Ordering: BullMQ consumed this queue one job at a time, so a conversation's
// messages reached the customer in the order they were written (product image
// before its reply, reply before the payment link). asynq consumes
// concurrently, so the handler restores that guarantee explicitly: sends are
// serialised per conversation with a Postgres advisory lock — which also holds
// across worker replicas — and every pending row of the conversation up to and
// including this one is delivered in outbound_messages.seq order.
package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sender is the WhatsApp surface delivery needs; *whatsapp.Client satisfies it.
type Sender interface {
	SendTextMessageData(ctx context.Context, phoneNumberID, to, text string) (map[string]any, error)
	SendImageMessage(ctx context.Context, phoneNumberID, to, imageURL, caption string) (map[string]any, error)
	SendTemplateMessage(ctx context.Context, phoneNumberID, to, templateName, languageCode string) (map[string]any, error)
}

// Deps carries the delivery collaborators.
type Deps struct {
	Pool *pgxpool.Pool
	WA   Sender
}

type outboundMessage struct {
	id            string
	seq           int64
	messageType   string
	recipient     string
	status        string
	textContent   *string
	imageURL      *string
	templateName  *string
	phoneNumberID string
	conversation  *string
}

// dbtx is the query surface shared by the pool and a single acquired conn.
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const outboundColumns = `o.id, o.seq, o.message_type, o.recipient_phone, o.status, o.text_content,
	o.image_url, o.template_name, b.whatsapp_phone_number_id, o.conversation_id`

func scanOutbound(row pgx.Row) (outboundMessage, error) {
	var m outboundMessage
	err := row.Scan(&m.id, &m.seq, &m.messageType, &m.recipient, &m.status, &m.textContent,
		&m.imageURL, &m.templateName, &m.phoneNumberID, &m.conversation)
	return m, err
}

// Deliver sends one outbound row (outbound.processor.ts process), first
// flushing any older pending rows of the same conversation so the customer
// receives them in write order. finalAttempt controls whether a delivery
// failure marks the row 'failed' (no retries left) or leaves it 'pending' for
// the next attempt. The returned error is the failure of this row only.
func Deliver(ctx context.Context, deps Deps, outboundMessageID string, finalAttempt bool) error {
	m, err := scanOutbound(deps.Pool.QueryRow(ctx, `
		SELECT `+outboundColumns+`
		  FROM outbound_messages o
		  JOIN businesses b ON b.id = o.business_id
		 WHERE o.id = $1`, outboundMessageID))
	if errors.Is(err, pgx.ErrNoRows) {
		slog.ErrorContext(ctx, fmt.Sprintf("OutboundMessage %s or associated Business not found.", outboundMessageID))
		return nil
	}
	if err != nil {
		return fmt.Errorf("outbound: load message %s: %w", outboundMessageID, err)
	}
	if skipOutbound(ctx, m) {
		return nil
	}
	if m.conversation == nil {
		return deliverOutbound(ctx, deps.Pool, deps.WA, m, true, finalAttempt)
	}

	conn, err := deps.Pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("outbound: acquire connection: %w", err)
	}
	lockKey := "outbound-conversation:" + *m.conversation
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		conn.Release()
		return fmt.Errorf("outbound: lock conversation %s: %w", *m.conversation, err)
	}
	defer func() {
		if _, uerr := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey); uerr != nil {
			// A connection that may still hold the lock must not return to the
			// pool; closing it makes Postgres release the lock.
			_ = conn.Hijack().Close(context.WithoutCancel(ctx))
			return
		}
		conn.Release()
	}()

	rows, err := conn.Query(ctx, `
		SELECT `+outboundColumns+`
		  FROM outbound_messages o
		  JOIN businesses b ON b.id = o.business_id
		 WHERE o.conversation_id = $1 AND o.status = 'pending' AND o.seq <= $2
		 ORDER BY o.seq`, *m.conversation, m.seq)
	if err != nil {
		return fmt.Errorf("outbound: load conversation backlog: %w", err)
	}
	backlog, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outboundMessage, error) { return scanOutbound(row) })
	if err != nil {
		return fmt.Errorf("outbound: load conversation backlog: %w", err)
	}

	var ownErr error
	for _, msg := range backlog {
		own := msg.id == outboundMessageID
		if err := deliverOutbound(ctx, conn, deps.WA, msg, own, own && finalAttempt); err != nil {
			if own {
				ownErr = err
			}
			// An earlier row that fails stays pending for its own task (which
			// owns its retries and failure accounting); later rows still go out
			// rather than stalling the whole conversation behind it.
		}
	}
	// Our row absent from the backlog means another task already delivered
	// it while we waited for the lock.
	return ownErr
}

func skipOutbound(ctx context.Context, m outboundMessage) bool {
	if m.status == "sent" {
		slog.WarnContext(ctx, fmt.Sprintf("Message %s is already sent, skipping.", m.id))
		return true
	}
	// failed_24h_window_closed / failed_stale are compliance verdicts, not
	// delivery failures — they must never go out, even if re-enqueued by hand.
	if strings.HasPrefix(m.status, "failed_") {
		slog.WarnContext(ctx, fmt.Sprintf("Message %s has terminal status %s, skipping.", m.id, m.status))
		return true
	}
	return false
}

// deliverOutbound sends one row and records the result. own marks the row
// the running task was enqueued for (only its failures are returned and, on
// the final attempt, recorded as 'failed').
func deliverOutbound(ctx context.Context, db dbtx, wa Sender, m outboundMessage, own, markFailed bool) error {
	slog.InfoContext(ctx, "Processing outbound message job", "outboundMessageId", m.id, "seq", m.seq)

	metaResponse, sendErr := sendOutbound(ctx, wa, m)
	if sendErr != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("Failed to send message %s: %v", m.id, sendErr))
		if markFailed {
			errBody, _ := json.Marshal(map[string]string{"error": sendErr.Error()})
			if _, uerr := db.Exec(ctx,
				`UPDATE outbound_messages SET status = 'failed', meta_response = $2 WHERE id = $1`,
				m.id, errBody); uerr != nil {
				slog.ErrorContext(ctx, "Failed to mark outbound message failed",
					"outboundMessageId", m.id, "error", uerr.Error())
			}
		}
		return fmt.Errorf("outbound: send %s: %w", m.id, sendErr)
	}

	whatsappMessageID := extractWhatsAppMessageID(metaResponse)
	metaJSON, err := json.Marshal(metaResponse)
	if err != nil {
		metaJSON = []byte("{}")
	}
	var waID any
	if whatsappMessageID != "" {
		waID = whatsappMessageID
	}
	// The message is already on the customer's phone: a failed status write
	// must not trigger a retry (that would double-send), so it is logged only.
	if _, err := db.Exec(ctx,
		`UPDATE outbound_messages SET status = 'sent', whatsapp_message_id = $2, meta_response = $3 WHERE id = $1`,
		m.id, waID, metaJSON); err != nil {
		slog.ErrorContext(ctx, "Message sent but status update failed",
			"outboundMessageId", m.id, "error", err.Error())
		return nil
	}

	conversationID := m.recipient
	if m.conversation != nil {
		conversationID = *m.conversation
	}
	slog.InfoContext(ctx, "Conversation outbound turn",
		"event", "conversation.outbound",
		"conversationId", conversationID,
		"senderPhone", m.recipient,
		"direction", "outbound",
		"messageType", m.messageType,
		"timestamp", time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		"whatsappMessageId", whatsappMessageID)
	slog.InfoContext(ctx, fmt.Sprintf("Message %s successfully sent and marked as sent.", m.id),
		"sentByOwnTask", own)
	return nil
}

func sendOutbound(ctx context.Context, wa Sender, m outboundMessage) (map[string]any, error) {
	if wa == nil {
		return nil, errors.New("whatsapp sender not configured")
	}
	switch {
	case m.messageType == "text" && m.textContent != nil && *m.textContent != "":
		return wa.SendTextMessageData(ctx, m.phoneNumberID, m.recipient, *m.textContent)
	case m.messageType == "image" && m.imageURL != nil && *m.imageURL != "":
		caption := ""
		if m.textContent != nil {
			caption = *m.textContent
		}
		return wa.SendImageMessage(ctx, m.phoneNumberID, m.recipient, *m.imageURL, caption)
	case m.messageType == "template" && m.templateName != nil && *m.templateName != "":
		// Language code isn't stored per message yet — source falls back to en_US.
		return wa.SendTemplateMessage(ctx, m.phoneNumberID, m.recipient, *m.templateName, "en_US")
	default:
		return nil, fmt.Errorf("unsupported messageType or missing content: %s", m.messageType)
	}
}

// extractWhatsAppMessageID ports WhatsAppService.extractMessageId:
// metaResponse?.messages?.[0]?.id.
func extractWhatsAppMessageID(resp map[string]any) string {
	msgs, ok := resp["messages"].([]any)
	if !ok || len(msgs) == 0 {
		return ""
	}
	first, ok := msgs[0].(map[string]any)
	if !ok {
		return ""
	}
	id, _ := first["id"].(string)
	return id
}
