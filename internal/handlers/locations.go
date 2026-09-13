package handlers

// locations.go ports the read side of apps/mobile-api/src/locations
// (Node, this session): physical shop locations a business can add, each
// flagged for delivery and/or pickup fulfillment.
//
// Mount: chi r.Mount("/locations", handlers.NewLocations(pool)) alongside
// the write subtree from MountLocationsWrite.

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// NewLocations returns the /locations read subtree.
func NewLocations(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/", locationsList(pool))
	r.Get("/{id}", locationsGet(pool))
	return r
}

// locationJSON mirrors the Node locations response payload key-for-key
// (camelCase, explicit nulls for nullable columns).
type locationJSON struct {
	ID             string  `json:"id"`
	BusinessID     string  `json:"businessId"`
	Name           string  `json:"name"`
	Address        string  `json:"address"`
	ShopNumber     *string `json:"shopNumber"`
	Landmark       *string `json:"landmark"`
	OpeningTime    string  `json:"openingTime"`
	ClosingTime    string  `json:"closingTime"`
	OffersDelivery bool    `json:"offersDelivery"`
	OffersPickup   bool    `json:"offersPickup"`
	IsActive       bool    `json:"isActive"`
	CreatedAt      isoTime `json:"createdAt"`
	UpdatedAt      isoTime `json:"updatedAt"`
}

func ep_location(row gen.BusinessLocation) locationJSON {
	return locationJSON{
		ID:             row.ID,
		BusinessID:     row.BusinessID,
		Name:           row.Name,
		Address:        row.Address,
		ShopNumber:     ep_text(row.ShopNumber),
		Landmark:       ep_text(row.Landmark),
		OpeningTime:    row.OpeningTime,
		ClosingTime:    row.ClosingTime,
		OffersDelivery: row.OffersDelivery,
		OffersPickup:   row.OffersPickup,
		IsActive:       row.IsActive,
		CreatedAt:      epISO(row.CreatedAt.Time),
		UpdatedAt:      epISO(row.UpdatedAt.Time),
	}
}

// locationsList ports LocationsService.findAll: findMany {where businessId}
// + count, paginated envelope.
func locationsList(pool *pgxpool.Pool) http.HandlerFunc {
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

		rows, err := q.ListLocationsByBusiness(r.Context(), gen.ListLocationsByBusinessParams{
			BusinessID: bizID,
			Limit:      int32(pq.Limit),
			Offset:     int32((pq.Page - 1) * pq.Limit),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		total, err := q.CountLocationsByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		data := make([]locationJSON, 0, len(rows))
		for _, row := range rows {
			data = append(data, ep_location(row))
		}
		_ = httpx.WritePaginated(w, http.StatusOK, data, int(total), pq.Page, pq.Limit)
	}
}

// locationsGet ports LocationsService.findOne (404 'Location not found').
func locationsGet(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		row, err := q.GetLocationByIDAndBusiness(r.Context(), gen.GetLocationByIDAndBusinessParams{
			ID:         chi.URLParam(r, "id"),
			BusinessID: bizID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Location not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusOK, ep_location(row))
	}
}
