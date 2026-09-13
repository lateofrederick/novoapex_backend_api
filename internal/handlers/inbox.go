package handlers

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// Inbox ports the conversations module read paths: InboxController
// (GET /inbox/summary, GET /inbox/handoffs) and ConversationsController
// (GET /conversations/:id/messages).
// Mount: chi r.Mount("/inbox", handlers.NewInbox(pool)) and
// chi r.Mount("/conversations", handlers.NewConversations(pool)).

// NewInbox returns the /inbox subtree.
func NewInbox(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/summary", inboxSummary(pool))
	r.Get("/handoffs", inboxHandoffs(pool))
	return r
}

// NewConversations returns the /conversations subtree (read path only; the
// takeover/release/reply writes belong to Stage 4).
func NewConversations(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/{id}/messages", conversationMessages(pool))
	return r
}

// conversationJSON mirrors the Prisma Conversation payload plus the optional
// included customer.
type conversationJSON struct {
	ID                 string        `json:"id"`
	BusinessID         string        `json:"businessId"`
	CustomerID         *string       `json:"customerId"`
	CustomerPhone      string        `json:"customerPhone"`
	State              string        `json:"state"`
	IsEscalatedToHuman bool          `json:"isEscalatedToHuman"`
	Language           string        `json:"language"`
	EscalationReason   *string       `json:"escalationReason"`
	CreatedAt          isoTime       `json:"createdAt"`
	UpdatedAt          isoTime       `json:"updatedAt"`
	Customer           *customerJSON `json:"customer"`
}

// inboxSummary ports ConversationsService.getSummary
// (conversations.service.ts:15-25).
func inboxSummary(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		total, err := q.CountConversationsByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		handoffs, err := q.CountEscalatedConversations(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			TotalConversations int `json:"totalConversations"`
			Handoffs           int `json:"handoffs"`
		}{
			TotalConversations: int(total),
			Handoffs:           int(handoffs),
		})
	}
}

// inboxHandoffs ports ConversationsService.getHandoffs
// (conversations.service.ts:27-51): findMany {businessId,
// isEscalatedToHuman:true, include customer, orderBy updatedAt desc} + count.
// customerId is nullable so customer serialises as null when unlinked.
func inboxHandoffs(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		pq, ok := ep_page(w, r)
		if !ok {
			return
		}

		rows, err := q.ListEscalatedConversationsWithCustomer(r.Context(), gen.ListEscalatedConversationsWithCustomerParams{
			BusinessID: bizID,
			Limit:      int32(pq.Limit),
			Offset:     int32((pq.Page - 1) * pq.Limit),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		total, err := q.CountEscalatedConversations(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		data := make([]conversationJSON, 0, len(rows))
		for _, row := range rows {
			cj := ep_customerJoined(row.CID, row.CBusinessID, row.CPhone, row.CName, row.CAcquisitionChannel, row.CMarketingOptIn,
				row.CFirstContactAt, row.CLastContactAt, row.CCreatedAt, row.CUpdatedAt)
			data = append(data, conversationJSON{
				ID:                 row.ID,
				BusinessID:         row.BusinessID,
				CustomerID:         ep_text(row.CustomerID),
				CustomerPhone:      row.CustomerPhone,
				State:              string(row.State),
				IsEscalatedToHuman: row.IsEscalatedToHuman,
				Language:           row.Language,
				EscalationReason:   ep_text(row.EscalationReason),
				CreatedAt:          epISO(row.CreatedAt.Time),
				UpdatedAt:          epISO(row.UpdatedAt.Time),
				Customer:           cj,
			})
		}
		_ = httpx.WritePaginated(w, http.StatusOK, data, int(total), pq.Page, pq.Limit)
	}
}

type inboundMessageJSON struct {
	ID                string  `json:"id"`
	WhatsappMessageID string  `json:"whatsappMessageId"`
	SenderPhone       string  `json:"senderPhone"`
	RecipientPhone    string  `json:"recipientPhone"`
	MessageType       string  `json:"messageType"`
	TextContent       *string `json:"textContent"`
	RawPayload        rawJSON `json:"rawPayload"`
	Timestamp         isoTime `json:"timestamp"`
	BusinessID        *string `json:"businessId"`
	ConversationID    *string `json:"conversationId"`
	CreatedAt         isoTime `json:"createdAt"`
	Type              string  `json:"type"`
	Time              isoTime `json:"time"`
}

type outboundMessageJSON struct {
	ID                string  `json:"id"`
	WhatsappMessageID *string `json:"whatsappMessageId"`
	RecipientPhone    string  `json:"recipientPhone"`
	MessageType       string  `json:"messageType"`
	TextContent       *string `json:"textContent"`
	TemplateName      *string `json:"templateName"`
	ImageURL          *string `json:"imageUrl"`
	ProductID         *string `json:"productId"`
	RawPayload        rawJSON `json:"rawPayload"`
	MetaResponse      rawJSON `json:"metaResponse"`
	Status            string  `json:"status"`
	BusinessID        *string `json:"businessId"`
	ConversationID    *string `json:"conversationId"`
	CreatedAt         isoTime `json:"createdAt"`
	Type              string  `json:"type"`
	Time              isoTime `json:"time"`
}

// rawJSON wraps jsonb bytes so null columns emit null while present values
// pass through verbatim (Prisma Json payloads are plain JSON values).
type rawJSON []byte

func (r rawJSON) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return r, nil
}

func ep_raw(b []byte) rawJSON { return rawJSON(b) }

// ep_messagesLimit reproduces the controller coercion
// (conversations.controller.ts): absent/empty -> 50, Number(x)||50 on parse
// failure, then clamp to [1,200].
func ep_messagesLimit(raw string) int {
	if raw == "" {
		return 50
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 50
	}
	switch {
	case n < 1:
		return 1
	case n > 200:
		return 200
	default:
		return n
	}
}

// conversationMessages ports ConversationsService.getMessages
// (conversations.service.ts:53-88): verify ownership, fetch the most recent
// limit messages per side (timestamp desc / createdAt desc), tag each with
// type+time, merge with a STABLE ascending sort (inbound block first — V8's
// sort is stable and inbound maps before outbound), then keep the newest
// limit. Returns a bare array, not the paginated envelope.
func conversationMessages(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		limit := ep_messagesLimit(r.URL.Query().Get("limit"))
		convID := chi.URLParam(r, "id")

		if _, err := q.GetConversationIDForBusiness(r.Context(), gen.GetConversationIDForBusinessParams{
			ID:         convID,
			BusinessID: bizID,
		}); errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Conversation not found"))
			return
		} else if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		inRows, err := q.ListRecentInboundMessages(r.Context(), gen.ListRecentInboundMessagesParams{
			ConversationID: pgtype.Text{String: convID, Valid: true},
			Limit:          int32(limit),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		outRows, err := q.ListRecentOutboundMessages(r.Context(), gen.ListRecentOutboundMessagesParams{
			ConversationID: pgtype.Text{String: convID, Valid: true},
			Limit:          int32(limit),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		type stamped struct {
			t time.Time
			v any
		}
		merged := make([]stamped, 0, len(inRows)+len(outRows))
		for _, m := range inRows {
			merged = append(merged, stamped{t: m.Timestamp.Time, v: inboundMessageJSON{
				ID:                m.ID,
				WhatsappMessageID: m.WhatsappMessageID,
				SenderPhone:       m.SenderPhone,
				RecipientPhone:    m.RecipientPhone,
				MessageType:       m.MessageType,
				TextContent:       ep_text(m.TextContent),
				RawPayload:        ep_raw(m.RawPayload),
				Timestamp:         epISO(m.Timestamp.Time),
				BusinessID:        ep_text(m.BusinessID),
				ConversationID:    ep_text(m.ConversationID),
				CreatedAt:         epISO(m.CreatedAt.Time),
				Type:              "inbound",
				Time:              epISO(m.Timestamp.Time),
			}})
		}
		for _, m := range outRows {
			merged = append(merged, stamped{t: m.CreatedAt.Time, v: outboundMessageJSON{
				ID:                m.ID,
				WhatsappMessageID: ep_text(m.WhatsappMessageID),
				RecipientPhone:    m.RecipientPhone,
				MessageType:       m.MessageType,
				TextContent:       ep_text(m.TextContent),
				TemplateName:      ep_text(m.TemplateName),
				ImageURL:          ep_text(m.ImageUrl),
				ProductID:         ep_text(m.ProductID),
				RawPayload:        ep_raw(m.RawPayload),
				MetaResponse:      ep_raw(m.MetaResponse),
				Status:            m.Status,
				BusinessID:        ep_text(m.BusinessID),
				ConversationID:    ep_text(m.ConversationID),
				CreatedAt:         epISO(m.CreatedAt.Time),
				Type:              "outbound",
				Time:              epISO(m.CreatedAt.Time),
			}})
		}

		sort.SliceStable(merged, func(a, b int) bool { return merged[a].t.Before(merged[b].t) })
		if len(merged) > limit {
			merged = merged[len(merged)-limit:]
		}

		out := make([]any, 0, len(merged))
		for _, m := range merged {
			out = append(out, m.v)
		}
		_ = httpx.WriteJSON(w, http.StatusOK, out)
	}
}
