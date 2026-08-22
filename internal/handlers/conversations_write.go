package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// Conversation writes port apps/mobile-api/src/conversations (T4.18-T4.20):
// POST /conversations/:id/takeover, /release and /reply
// (conversations.controller.ts:36-53). Reads stay in inbox.go.
//
// Mount: chi r.Route("/conversations", func(r chi.Router) {
// MountConversationsWrite(r, deps); r.Mount("/", handlers.NewConversations(pool)) }).

// OutboundPublisher is the queue seam behind T4.20/T4.1 + ADR 0001
// (docs/adr/0001-queue-substrate.md): the handler ALWAYS persists the
// outbound_messages row first, then hands the row's id to the publisher —
// the exact ordering of enqueueOutboundMessage
// (conversation-orchestrator.service.ts:59-78: outboundMessage.create ->
// outboundQueue.add('send-message', {outboundMessageId})).
//
// HOOK POINT for the real bridge: wire an asynq/BullMQ-shim implementation of
// this interface into ConversationsWriteDeps.Publisher at integration time.
// Nothing else changes — the persistence contract is already the durable
// source of truth.
type OutboundPublisher interface {
	PublishOutbound(ctx context.Context, outboundMessageID string) error
}

// NullPublisher is the default no-op publisher used until the queue bridge
// ships (ADR 0001 defers it): it logs so a dropped send is visible in ops,
// but never fails the request — the row is committed either way.
type NullPublisher struct{}

func (NullPublisher) PublishOutbound(ctx context.Context, outboundMessageID string) error {
	slog.Warn("outbound publish skipped: no queue bridge wired (ADR 0001)",
		"outboundMessageId", outboundMessageID)
	return nil
}

// ConversationsWriteDeps carries the write subtree's collaborators.
type ConversationsWriteDeps struct {
	Pool      *pgxpool.Pool
	Publisher OutboundPublisher // nil -> NullPublisher
}

// MountConversationsWrite registers takeover/release/reply on r.
func MountConversationsWrite(r chi.Router, d ConversationsWriteDeps) {
	q := gen.New(d.Pool)
	var pub OutboundPublisher = NullPublisher{}
	if d.Publisher != nil {
		pub = d.Publisher
	}
	r.Post("/{id}/takeover", conversationTakeover(q))
	r.Post("/{id}/release", conversationRelease(q))
	r.Post("/{id}/reply", conversationReply(q, pub))
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

// wo_window is the WhatsApp free-form window: 24h after the customer's newest
// inbound message. Source constant TWENTY_FOUR_HOURS
// (conversation-orchestrator.service.ts:40).
const wo_window = 24 * time.Hour

// conversationReply ports the vendor reply through the orchestrator's
// characterized flow (the mobile-api reply predates the queue substrate; T4.20
// pins the endpoint to enqueueOutboundMessage semantics,
// conversation-orchestrator.service.ts:28-81):
//
//  1. ownership probe findFirst({id, businessId}) include business -> 404
//     'Conversation not found' on miss;
//  2. window gate on the NEWEST inbound_messages.timestamp:
//     within iff a row exists AND now - timestamp < 24h;
//  3. blocked  -> persist status 'failed_24h_window_closed', raw_payload {},
//     do NOT enqueue, still return the persisted row (201);
//  4. allowed  -> persist status 'pending' with the WhatsApp text payload
//     shape, then PublishOutbound(row id).
//
// Response bodies mirror Prisma's OutboundMessage payload key-for-key.
func conversationReply(q *gen.Queries, pub OutboundPublisher) http.HandlerFunc {
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

		conv, err := q.GetConversationWithBusiness(r.Context(), gen.GetConversationWithBusinessParams{
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

		withinWindow := false
		if latest, err := q.GetLatestInboundTimestamp(r.Context(),
			pgtype.Text{String: conv.ID, Valid: true}); err == nil && latest.Valid {
			withinWindow = time.Since(latest.Time) < wo_window // strict <
		} else if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		status := "pending"
		rawPayload := []byte(fmt.Sprintf(
			`{"messaging_product":"whatsapp","to":%q,"type":"text","text":{"body":%q}}`,
			conv.CustomerPhone, *body.Text))
		if !withinWindow {
			// Compliance block (orchestrator lines 43-57): rawPayload {}.
			status = "failed_24h_window_closed"
			rawPayload = []byte(`{}`)
		}

		row, err := q.InsertOutboundMessage(r.Context(), gen.InsertOutboundMessageParams{
			ID:             wo_newID(),
			RecipientPhone: conv.CustomerPhone,
			MessageType:    "text",
			TextContent:    pgtype.Text{String: *body.Text, Valid: true},
			RawPayload:     rawPayload,
			Status:         status,
			BusinessID:     pgtype.Text{String: bizID, Valid: true},
			ConversationID: pgtype.Text{String: conv.ID, Valid: true},
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		if withinWindow {
			// Persist-then-publish: the row id is what travels on the queue.
			if err := pub.PublishOutbound(r.Context(), row.ID); err != nil {
				httpx.WriteError(w, r, err)
				return
			}
		}

		_ = httpx.WriteJSON(w, http.StatusCreated, wo_outboundJSON(row))
	}
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
