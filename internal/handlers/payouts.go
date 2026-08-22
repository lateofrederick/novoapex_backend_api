package handlers

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

// Payouts ports apps/mobile-api/src/payouts (read paths only; requestPayout
// is the Stage 4 money path).
// Mount: chi r.Mount("/payouts", handlers.NewPayouts(pool)).

// NewPayouts returns the /payouts subtree.
func NewPayouts(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/balance", payoutsBalance(pool))
	r.Get("/history", payoutsHistory(pool))
	return r
}

type payoutJSON struct {
	ID                 string       `json:"id"`
	BusinessID         string       `json:"businessId"`
	Amount             money.Number `json:"amount"`
	Currency           string       `json:"currency"`
	Status             string       `json:"status"`
	Reference          *string      `json:"reference"`
	PaystackTransferID *string      `json:"paystackTransferId"`
	CreatedAt          isoTime      `json:"createdAt"`
	UpdatedAt          isoTime      `json:"updatedAt"`
}

// payoutsBalance ports PayoutsService.getBalance (payouts.service.ts:75-109):
// availableBalance = sum(SUCCESS payments) - sum(SUCCESS+PENDING payouts),
// floored at 0 like Math.max(0, ...); unknown business -> 400
// BadRequestException('Business not found').
func payoutsBalance(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		currency, err := q.GetBusinessCurrencyByID(r.Context(), bizID)
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, "Business not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		revenue, err := q.SumSuccessfulPaymentAmounts(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		committed, err := q.SumCommittedPayoutAmounts(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		available := revenue.Sub(committed)
		if available.IsNegative() {
			available = decimal.Zero
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			TotalRevenue     money.Number `json:"totalRevenue"`
			CommittedPayouts money.Number `json:"committedPayouts"`
			AvailableBalance money.Number `json:"availableBalance"`
			Currency         string       `json:"currency"`
		}{
			TotalRevenue:     ep_num(revenue),
			CommittedPayouts: ep_num(committed),
			AvailableBalance: ep_num(available),
			Currency:         currency,
		})
	}
}

// payoutsHistory ports PayoutsService.getHistory (payouts.service.ts:38-61):
// findMany {where businessId, orderBy createdAt desc} + count.
func payoutsHistory(pool *pgxpool.Pool) http.HandlerFunc {
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

		rows, err := q.ListPayoutsByBusiness(r.Context(), gen.ListPayoutsByBusinessParams{
			BusinessID: bizID,
			Limit:      int32(pq.Limit),
			Offset:     int32((pq.Page - 1) * pq.Limit),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		total, err := q.CountPayoutsByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		data := make([]payoutJSON, 0, len(rows))
		for _, row := range rows {
			data = append(data, payoutJSON{
				ID:                 row.ID,
				BusinessID:         row.BusinessID,
				Amount:             ep_num(row.Amount),
				Currency:           row.Currency,
				Status:             string(row.Status),
				Reference:          ep_text(row.Reference),
				PaystackTransferID: ep_text(row.PaystackTransferID),
				CreatedAt:          epISO(row.CreatedAt.Time),
				UpdatedAt:          epISO(row.UpdatedAt.Time),
			})
		}
		_ = httpx.WritePaginated(w, http.StatusOK, data, int(total), pq.Page, pq.Limit)
	}
}
