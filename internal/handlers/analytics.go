package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

// Analytics ports apps/mobile-api/src/analytics (AnalyticsService +
// DashboardController).
// Mount: chi r.Mount("/analytics", handlers.NewAnalytics(pool)) and
// chi r.Mount("/dashboard", handlers.NewDashboard(pool)).

// NewDashboard returns the /dashboard subtree.
func NewDashboard(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/summary", dashboardSummary(pool))
	return r
}

// NewAnalytics returns the /analytics subtree.
func NewAnalytics(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/overview", analyticsOverview(pool))
	r.Get("/handoff-reasons", analyticsHandoffReasons(pool))
	r.Get("/demand", analyticsDemand(pool))
	return r
}

// dashboardSummary ports AnalyticsService.getDashboardSummary
// (analytics.service.ts:8-27). Note the Node field name: totalPayouts is
// actually the sum of SUCCESS payment amounts, not payouts.
func dashboardSummary(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		pendingPayments, err := q.CountPaymentsByStatus(r.Context(), gen.CountPaymentsByStatusParams{
			BusinessID: bizID,
			Status:     gen.PaymentStatusPENDING,
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		handoffs, err := q.CountEscalatedConversations(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		revenue, err := q.SumPaymentAmountsByStatus(r.Context(), gen.SumPaymentAmountsByStatusParams{
			BusinessID: bizID,
			Status:     gen.PaymentStatusSUCCESS,
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			PendingPayments int          `json:"pendingPayments"`
			ActiveHandoffs  int          `json:"activeHandoffs"`
			TotalPayouts    money.Number `json:"totalPayouts"`
		}{
			PendingPayments: int(pendingPayments),
			ActiveHandoffs:  int(handoffs),
			TotalPayouts:    ep_num(revenue),
		})
	}
}

// analyticsOverview ports AnalyticsService.getOverview (analytics.service.ts:29-51).
// averageResponseTimeMins stays the hardcoded stub value 5.
func analyticsOverview(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		rescued, err := q.CountRescuedPaidOrders(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		totalConversations, err := q.CountConversationsByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			RescuedLeads            int `json:"rescuedLeads"`
			TotalConversations      int `json:"totalConversations"`
			AverageResponseTimeMins int `json:"averageResponseTimeMins"`
		}{
			RescuedLeads:            int(rescued),
			TotalConversations:      int(totalConversations),
			AverageResponseTimeMins: 5,
		})
	}
}

// analyticsHandoffReasons ports AnalyticsService.getHandoffReasons
// (analytics.service.ts:53-74): groupBy escalationReason where not null,
// ordered by count desc (plus a deterministic reason tiebreak; Node leaves
// tie order undefined).
func analyticsHandoffReasons(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		rows, err := q.CountConversationsByEscalationReason(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		out := make([]struct {
			Reason string `json:"reason"`
			Count  int    `json:"count"`
		}, 0, len(rows))
		for _, row := range rows {
			out = append(out, struct {
				Reason string `json:"reason"`
				Count  int    `json:"count"`
			}{Reason: row.Reason.String, Count: int(row.Total)})
		}
		_ = httpx.WriteJSON(w, http.StatusOK, out)
	}
}

// analyticsDemand ports AnalyticsService.getDemand (analytics.service.ts:76-89):
// top five products by request_count with exactly {id,name,requestCount}.
func analyticsDemand(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		rows, err := q.TopDemandProducts(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		out := make([]struct {
			ID           string `json:"id"`
			Name         string `json:"name"`
			RequestCount int32  `json:"requestCount"`
		}, 0, len(rows))
		for _, row := range rows {
			out = append(out, struct {
				ID           string `json:"id"`
				Name         string `json:"name"`
				RequestCount int32  `json:"requestCount"`
			}{ID: row.ID, Name: row.Name, RequestCount: row.RequestCount})
		}
		_ = httpx.WriteJSON(w, http.StatusOK, out)
	}
}
