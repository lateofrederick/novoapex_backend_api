package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// ms_persistOutbound ports MessagesService.persistOutboundMessage
// (apps/api/src/messages/messages.service.ts): every manual send attempt —
// success or failure — is recorded as an outbound_messages row for a complete
// audit trail, then logged as a structured "Conversation outbound turn"
// (buildOutboundConversationLogContext).
//
// The source keyed conversationId to the recipient phone, which the
// conversations foreign key rejects; here the row links to the calling
// vendor's business and, when one exists, their conversation with that
// recipient. A failed audit write is logged, never surfaced: by then the
// message outcome is already decided.
func ms_persistOutbound(r *http.Request, pool *pgxpool.Pool, rec ms_outboundRecord) {
	if pool == nil {
		return
	}
	ctx := context.WithoutCancel(r.Context())

	var businessID, conversationID *string
	if claims, ok := auth.FromContext(r.Context()); ok && claims.BusinessID != "" {
		id := claims.BusinessID
		businessID = &id
		var convID string
		if err := pool.QueryRow(ctx,
			`SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
			id, rec.recipientPhone).Scan(&convID); err == nil {
			conversationID = &convID
		}
	}

	status := "sent"
	metaResponse := any(rec.metaResponse)
	if rec.sendErr != nil {
		status = "failed"
		metaResponse = map[string]string{"error": rec.sendErr.Error()}
	}
	whatsappMessageID := ""
	if msgs, ok := rec.metaResponse["messages"].([]any); ok && len(msgs) > 0 {
		if first, ok := msgs[0].(map[string]any); ok {
			whatsappMessageID, _ = first["id"].(string)
		}
	}

	raw, _ := json.Marshal(rec.rawPayload)
	var meta []byte
	if metaResponse != nil {
		meta, _ = json.Marshal(metaResponse)
	}
	nullable := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}

	id := uuid.NewString()
	var createdAt time.Time
	err := pool.QueryRow(ctx, `
		INSERT INTO outbound_messages
			(id, whatsapp_message_id, recipient_phone, message_type, text_content, template_name,
			 raw_payload, meta_response, status, business_id, conversation_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING created_at`,
		id, nullable(whatsappMessageID), rec.recipientPhone, rec.messageType, nullable(rec.textContent),
		nullable(rec.templateName), raw, meta, status, businessID, conversationID).Scan(&createdAt)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to persist outbound message",
			"event", "outbound_persist_failed", "recipientPhone", rec.recipientPhone, "error", err.Error())
		return
	}

	logConversationID := rec.recipientPhone // 1:1 chats: conversation keyed by the phone
	if conversationID != nil {
		logConversationID = *conversationID
	}
	slog.InfoContext(ctx, "Conversation outbound turn",
		"event", "conversation.outbound",
		"conversationId", logConversationID,
		"senderPhone", rec.recipientPhone,
		"direction", "outbound",
		"messageType", rec.messageType,
		"turnNumber", nil,
		"timestamp", createdAt.UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		"whatsappMessageId", whatsappMessageID,
		"outboundMessageId", id,
		"status", status)
}
