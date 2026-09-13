package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// Orders writes port apps/mobile-api/src/orders (T4.16/T4.17):
// PATCH /orders/:id/fulfillment and POST /orders/:id/escalate
// (orders.controller.ts:26-38). Reads stay in orders.go.
//
// Mount: chi r.Route("/orders", func(r chi.Router) { MountOrdersWrite(r, deps);
// r.Mount("/", handlers.NewOrders(pool)) }) — the write subtree shares the
// read prefix so chi URL params resolve identically.

// OrdersWriteDeps carries the write subtree's collaborators.
type OrdersWriteDeps struct {
	Pool *pgxpool.Pool
}

// orderFulfillmentStatuses is UpdateOrderDto's z.enum set
// (dto/update-order.dto.ts:5): the full OrderStatus enum. Originally limited
// to PENDING/CONFIRMED/PAYMENT_PENDING/PAID — that meant DELIVERED (and
// PROCESSING/SHIPPED/CANCELLED) could never actually be set through this
// endpoint, which silently blocked the delivery-confirmation follow-up from
// ever being reachable. Expanded to the full set to fix that.
var orderFulfillmentStatuses = []gen.OrderStatus{
	gen.OrderStatusPENDING,
	gen.OrderStatusCONFIRMED,
	gen.OrderStatusPAYMENTPENDING,
	gen.OrderStatusPAID,
	gen.OrderStatusPROCESSING,
	gen.OrderStatusSHIPPED,
	gen.OrderStatusDELIVERED,
	gen.OrderStatusCANCELLED,
}

// MountOrdersWrite registers PATCH /{id}/fulfillment and POST /{id}/escalate
// on r.
func MountOrdersWrite(r chi.Router, d OrdersWriteDeps) {
	q := gen.New(d.Pool)
	r.Patch("/{id}/fulfillment", ordersFulfillment(q))
	r.Post("/{id}/escalate", ordersEscalate(q))
}

// ordersFulfillment ports OrdersService.updateFulfillment
// (orders.service.ts:77-108): findFirst({id, businessId}) -> 404 'Order not
// found' on miss (also the cross-tenant answer), update status, and — on a
// fresh transition into DELIVERED (not a repeat update already at
// DELIVERED) — schedule the delivery-confirmation follow-up 2h out via the
// same db-backed ScheduledFollowUp mechanism as order-ledger.handler.ts.
func ordersFulfillment(q *gen.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		id := chi.URLParam(r, "id")

		var body struct {
			Status *string `json:"status"`
		}
		if !wo_decodeBody(w, r, &body) {
			return
		}
		if body.Status == nil {
			httpx.WriteZodValidationError(w, []httpx.FieldIssue{{
				Code: "invalid_type", Path: "status", Message: "Required",
			}})
			return
		}
		status, ok := wo_orderStatus(*body.Status)
		if !ok {
			httpx.WriteZodValidationError(w, []httpx.FieldIssue{{
				Code:    "invalid_enum_value",
				Path:    "status",
				Message: "Invalid enum value. Expected 'PENDING' | 'CONFIRMED' | 'PAYMENT_PENDING' | 'PAID' | 'PROCESSING' | 'SHIPPED' | 'DELIVERED' | 'CANCELLED', received '" + *body.Status + "'",
			}})
			return
		}

		existing, err := q.GetOrderByIDAndBusiness(r.Context(), gen.GetOrderByIDAndBusinessParams{
			ID: id, BusinessID: bizID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Order not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		if _, err := q.UpdateOrderFulfillment(r.Context(), gen.UpdateOrderFulfillmentParams{
			Status:     status,
			ID:         id,
			BusinessID: bizID,
		}); err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		if status == gen.OrderStatusDELIVERED && existing.Status != gen.OrderStatusDELIVERED {
			if err := q.InsertScheduledFollowUp(r.Context(), gen.InsertScheduledFollowUpParams{
				ID:          uuid.NewString(),
				OrderID:     id,
				BusinessID:  bizID,
				CustomerID:  existing.CustomerID,
				JobType:     "delivery-confirmation",
				ScheduledAt: pgtype.Timestamp{Time: time.Now().UTC().Add(2 * time.Hour), Valid: true},
			}); err != nil {
				httpx.WriteError(w, r, err)
				return
			}
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			Success bool   `json:"success"`
			Status  string `json:"status"`
		}{Success: true, Status: string(status)})
	}
}

// ordersEscalate ports OrdersService.escalate (orders.service.ts:89-100):
// findOne({id, businessId}) -> 404 'Order not found' on miss; when the order
// links a conversation it is flipped to isEscalatedToHuman=true,
// state='ESCALATED' (no escalation_reason change). The success message is
// returned either way.
func ordersEscalate(q *gen.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		order, err := q.GetOrderByIDAndBusiness(r.Context(), gen.GetOrderByIDAndBusinessParams{
			ID:         chi.URLParam(r, "id"),
			BusinessID: bizID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Order not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		if order.ConversationID.Valid {
			if _, err := q.EscalateConversationByID(r.Context(), order.ConversationID.String); err != nil {
				httpx.WriteError(w, r, err)
				return
			}
		}

		_ = httpx.WriteJSON(w, http.StatusCreated, struct {
			Success bool   `json:"success"`
			Message string `json:"message"`
		}{
			Success: true,
			Message: "Associated conversation escalated to human",
		})
	}
}

// wo_orderStatus resolves a raw string against the DTO enum set.
func wo_orderStatus(raw string) (gen.OrderStatus, bool) {
	for _, s := range orderFulfillmentStatuses {
		if raw == string(s) {
			return s, true
		}
	}
	return "", false
}

// wo_decodeBody decodes the JSON object body; unparseable input answers with
// the zod validation envelope (the Node pipe rejects any body its schema
// cannot parse with the same 400 shape).
func wo_decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		httpx.WriteZodValidationError(w, []httpx.FieldIssue{{
			Code: "invalid_type", Path: "", Message: "Expected object",
		}})
		return false
	}
	return true
}
