package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

// Customers ports apps/mobile-api/src/customers (controller + service).
// Mount: chi r.Mount("/customers", handlers.NewCustomers(pool)).

// NewCustomers returns the /customers subtree.
func NewCustomers(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/", customersList(pool))
	r.Get("/summary", customersSummary(pool))
	r.Get("/{id}", customersGet(pool))
	return r
}

// customerJSON mirrors the Prisma Customer payload exactly (camelCase, null
// keys preserved for nullable columns).
type customerJSON struct {
	ID                 string  `json:"id"`
	BusinessID         string  `json:"businessId"`
	Phone              string  `json:"phone"`
	Name               *string `json:"name"`
	AcquisitionChannel *string `json:"acquisitionChannel"`
	FirstContactAt     isoTime `json:"firstContactAt"`
	LastContactAt      isoTime `json:"lastContactAt"`
	CreatedAt          isoTime `json:"createdAt"`
	UpdatedAt          isoTime `json:"updatedAt"`
}

func ep_customer(row gen.Customer) customerJSON {
	return customerJSON{
		ID:                 row.ID,
		BusinessID:         row.BusinessID,
		Phone:              row.Phone,
		Name:               ep_text(row.Name),
		AcquisitionChannel: ep_text(row.AcquisitionChannel),
		FirstContactAt:     epISO(row.FirstContactAt.Time),
		LastContactAt:      epISO(row.LastContactAt.Time),
		CreatedAt:          epISO(row.CreatedAt.Time),
		UpdatedAt:          epISO(row.UpdatedAt.Time),
	}
}

// profileJSON mirrors the Prisma CustomerProfile payload.
type profileJSON struct {
	ID                 string          `json:"id"`
	CustomerID         string          `json:"customerId"`
	Preferences        json.RawMessage `json:"preferences"`
	DeliveryArea       *string         `json:"deliveryArea"`
	AverageOrderValue  *money.Number   `json:"averageOrderValue"`
	OrderFrequencyDays *float64        `json:"orderFrequencyDays"`
	LastOrderAt        *isoTime        `json:"lastOrderAt"`
	LastReengagementAt *isoTime        `json:"lastReengagementAt"`
	TotalOrders        int32           `json:"totalOrders"`
	TotalSpent         money.Number    `json:"totalSpent"`
	Sentiment          *string         `json:"sentiment"`
	UpdatedAt          isoTime         `json:"updatedAt"`
}

// customersList ports CustomersService.findAll (customers.service.ts:10-33):
// findMany {where businessId, orderBy lastContactAt desc, skip, take} +
// count, wrapped in the PaginatedResponse envelope.
func customersList(pool *pgxpool.Pool) http.HandlerFunc {
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

		rows, err := q.ListCustomersByBusiness(r.Context(), gen.ListCustomersByBusinessParams{
			BusinessID: bizID,
			Limit:      int32(pq.Limit),
			Offset:     int32((pq.Page - 1) * pq.Limit),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		total, err := q.CountCustomersByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		data := make([]customerJSON, 0, len(rows))
		for _, row := range rows {
			data = append(data, ep_customer(row))
		}
		_ = httpx.WritePaginated(w, http.StatusOK, data, int(total), pq.Page, pq.Limit)
	}
}

// customersSummary ports CustomersService.getSummary (customers.service.ts:50-68):
// total count + count created within the last 30 days (computed from "now").
func customersSummary(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		total, err := q.CountCustomersByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		thirtyDaysAgo := pgtype.Timestamp{Time: time.Now().UTC().AddDate(0, 0, -30), Valid: true}
		recent, err := q.CountCustomersCreatedSince(r.Context(), gen.CountCustomersCreatedSinceParams{
			BusinessID: bizID,
			CreatedAt:  thirtyDaysAgo,
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			TotalCustomers         int `json:"totalCustomers"`
			NewCustomersLast30Days int `json:"newCustomersLast30Days"`
		}{
			TotalCustomers:         int(total),
			NewCustomersLast30Days: int(recent),
		})
	}
}

// customersGet ports CustomersService.findOne (customers.service.ts:35-48):
// findFirst {id, businessId} include profile:true; a miss throws
// NotFoundException('Customer not found'). A customer without a profile row
// serialises profile:null (Prisma include of an optional to-one relation).
func customersGet(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		row, err := q.GetCustomerByIDAndBusiness(r.Context(), gen.GetCustomerByIDAndBusinessParams{
			ID:         chi.URLParam(r, "id"),
			BusinessID: bizID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Customer not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		var profile any // marshals to null when left nil
		prow, perr := q.GetCustomerProfileByCustomerID(r.Context(), row.ID)
		switch {
		case errors.Is(perr, pgx.ErrNoRows):
			// profile stays null
		case perr != nil:
			httpx.WriteError(w, r, perr)
			return
		default:
			profile = profileJSON{
				ID:                 prow.ID,
				CustomerID:         prow.CustomerID,
				Preferences:        json.RawMessage(prow.Preferences),
				DeliveryArea:       ep_text(prow.DeliveryArea),
				AverageOrderValue:  ep_numeric(prow.AverageOrderValue),
				OrderFrequencyDays: ep_float8(prow.OrderFrequencyDays),
				LastOrderAt:        epISOPtr(prow.LastOrderAt),
				LastReengagementAt: epISOPtr(prow.LastReengagementAt),
				TotalOrders:        prow.TotalOrders,
				TotalSpent:         ep_num(prow.TotalSpent),
				Sentiment:          ep_text(prow.Sentiment),
				UpdatedAt:          epISO(prow.UpdatedAt.Time),
			}
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			customerJSON
			Profile any `json:"profile"`
		}{customerJSON: ep_customer(row), Profile: profile})
	}
}
