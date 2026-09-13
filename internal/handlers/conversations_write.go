package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/outbound"
)

// Conversation writes port apps/mobile-api/src/conversations (T4.18-T4.20):
// POST /conversations/:id/takeover, /release and /reply
// (conversations.controller.ts:36-53). Reads stay in inbox.go.
//
// Mount: chi r.Route("/conversations", func(r chi.Router) {
// MountConversationsWrite(r, deps); r.Mount("/", handlers.NewConversations(pool)) }).

// ConversationsWriteDeps carries the write subtree's collaborators.
type ConversationsWriteDeps struct {
	Pool *pgxpool.Pool
	WA   outbound.Sender // WhatsApp Cloud API client used by /reply
}

// MountConversationsWrite registers takeover/release/reply on r.
func MountConversationsWrite(r chi.Router, d ConversationsWriteDeps) {
	q := gen.New(d.Pool)
	r.Post("/{id}/takeover", conversationTakeover(q))
	r.Post("/{id}/release", conversationRelease(q))
	r.Post("/{id}/reply", conversationReply(q, outbound.Deps{Pool: d.Pool, WA: d.WA}))
}

// conversationTakeover ports ConversationsService.takeover
// (conversations.service.ts:90-101): updateMany {id, businessId} ->
// isEscalatedToHuman=true, state='ESCALATED'; count===0 -> NotFoundException(
// 'Conversation not found'). No escalation_reason is set by the operator path.
func conversationTakeover(q *gen.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		affected, err := q.TakeoverConversation(r.Context(), gen.TakeoverConversationParams{
			ID:         chi.URLParam(r, "id"),
			BusinessID: bizID,
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		if affected == 0 {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Conversation not found"))
			return
		}

		_ = httpx.WriteJSON(w, http.StatusCreated, struct {
			Success bool `json:"success"`
		}{Success: true})
	}
}

// conversationRelease ports ConversationsService.release
// (conversations.service.ts:103-114): same scoping as takeover with
// isEscalatedToHuman=false, state='LEAD'.
func conversationRelease(q *gen.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		affected, err := q.ReleaseConversation(r.Context(), gen.ReleaseConversationParams{
			ID:         chi.URLParam(r, "id"),
			BusinessID: bizID,
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		if affected == 0 {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Conversation not found"))
			return
		}

		_ = httpx.WriteJSON(w, http.StatusCreated, struct {
			Success bool `json:"success"`
		}{Success: true})
	}
}

// conversationReply ports ConversationsService.reply
// (conversations.service.ts): the vendor's message is sent to the customer
// synchronously and the response is the persisted OutboundMessage — status
// 'sent' with Meta's whatsappMessageId and metaResponse (201). When Meta
// rejects the send (e.g. outside the 24-hour customer-service window) the
// request fails with the 500 envelope and no message record remains, exactly
// as the source, which only persisted after a successful send.
//
// Delivery goes through internal/outbound so a reply can never overtake an
// assistant message of the same conversation that is still queued: the row is
// written first (taking its place in the conversation's send order) and
// delivered inline under the conversation's send lock.
func conversationReply(q *gen.Queries, deps outbound.Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		var body struct {
			Text *string `json:"text"`
		}
		if !wo_decodeBody(w, r, &body) {
			return
		}
		switch {
		case body.Text == nil:
			httpx.WriteZodValidationError(w, []httpx.FieldIssue{{
				Code: "invalid_type", Path: "text", Message: "Required",
			}})
			return
		case len(*body.Text) < 1:
			httpx.WriteZodValidationError(w, []httpx.FieldIssue{{
				Code: "too_small", Path: "text",
				Message: "String must contain at least 1 character(s)",
			}})
			return
		}

		ctx := r.Context()
		conv, err := q.GetConversationWithBusiness(ctx, gen.GetConversationWithBusinessParams{
			ID:         chi.URLParam(r, "id"),
			BusinessID: bizID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Conversation not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		// rawPayload: { type: 'text', text: dto.text } (conversations.service.ts).
		rawPayload, err := json.Marshal(map[string]string{"type": "text", "text": *body.Text})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		row, err := q.InsertOutboundMessage(ctx, gen.InsertOutboundMessageParams{
			ID:             wo_newID(),
			RecipientPhone: conv.CustomerPhone,
			MessageType:    "text",
			TextContent:    pgtype.Text{String: *body.Text, Valid: true},
			RawPayload:     rawPayload,
			Status:         "pending",
			BusinessID:     pgtype.Text{String: bizID, Valid: true},
			ConversationID: pgtype.Text{String: conv.ID, Valid: true},
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		if sendErr := outbound.Deliver(ctx, deps, row.ID, true); sendErr != nil {
			// The source threw before persisting anything; drop the record so
			// an undelivered reply never shows in the thread or in the
			// assistant's conversation history.
			if _, derr := deps.Pool.Exec(context.WithoutCancel(ctx),
				`DELETE FROM outbound_messages WHERE id = $1`, row.ID); derr != nil {
				slog.ErrorContext(ctx, "Failed to remove undelivered vendor reply",
					"outboundMessageId", row.ID, "error", derr.Error())
			}
			httpx.WriteError(w, r, sendErr)
			return
		}

		sent, err := wo_loadOutbound(ctx, deps.Pool, row.ID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusCreated, wo_outboundJSON(sent))
	}
}

// wo_loadOutbound re-reads an outbound row after delivery recorded its
// whatsappMessageId, metaResponse and status.
func wo_loadOutbound(ctx context.Context, pool *pgxpool.Pool, id string) (gen.InsertOutboundMessageRow, error) {
	var row gen.InsertOutboundMessageRow
	err := pool.QueryRow(ctx, `
		SELECT id, whatsapp_message_id, recipient_phone, message_type, text_content,
		       template_name, image_url, product_id, raw_payload, meta_response,
		       status, business_id, conversation_id, created_at
		  FROM outbound_messages WHERE id = $1`, id).Scan(
		&row.ID, &row.WhatsappMessageID, &row.RecipientPhone, &row.MessageType, &row.TextContent,
		&row.TemplateName, &row.ImageUrl, &row.ProductID, &row.RawPayload, &row.MetaResponse,
		&row.Status, &row.BusinessID, &row.ConversationID, &row.CreatedAt)
	return row, err
}

// wo_outboundJSON renders the Prisma OutboundMessage payload (same key set as
// the merged message objects in inbox.go, minus the type/time decorations).
func wo_outboundJSON(row gen.InsertOutboundMessageRow) map[string]any {
	return map[string]any{
		"id":                row.ID,
		"whatsappMessageId": ep_text(row.WhatsappMessageID),
		"recipientPhone":    row.RecipientPhone,
		"messageType":       row.MessageType,
		"textContent":       ep_text(row.TextContent),
		"templateName":      ep_text(row.TemplateName),
		"imageUrl":          ep_text(row.ImageUrl),
		"productId":         ep_text(row.ProductID),
		"rawPayload":        ep_raw(row.RawPayload),
		"metaResponse":      ep_raw(row.MetaResponse),
		"status":            row.Status,
		"businessId":        ep_text(row.BusinessID),
		"conversationId":    ep_text(row.ConversationID),
		"createdAt":         epISO(row.CreatedAt.Time),
	}
}
