package handlers

// locations_write.go ports the write surface of
// apps/mobile-api/src/locations/*.ts (Node, this session): CreateLocationDto/
// UpdateLocationDto's zod-shaped 400s (reusing the s4p_* validator helpers
// from products_write.go), the "must offer delivery, pickup, or both"
// refinement (enforced on create by zod-shape parity, and on update by
// merging against the existing row exactly like LocationsService.update),
// and a dynamic partial UPDATE mirroring the products PATCH pattern.
//
// Mount: chi Mount("/locations", ...) with MountLocationsWrite alongside the
// read subtree from NewLocations.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// LocationsWriteDeps carries the write subtree's collaborators.
type LocationsWriteDeps struct {
	Pool *pgxpool.Pool
}

// MountLocationsWrite registers the location write routes on r.
func MountLocationsWrite(r chi.Router, deps LocationsWriteDeps) {
	r.Post("/", s4l_locationCreate(deps))
	r.Patch("/{id}", s4l_locationPatch(deps))
	r.Delete("/{id}", s4l_locationDelete(deps))
}

// s4l_timeRegex mirrors TIME_REGEX (create-location.dto.ts): 24-hour "HH:mm".
var s4l_timeRegex = regexp.MustCompile(`^([01]\d|2[0-3]):([0-5]\d)$`)

// s4l_checkTime validates a required/optional z.string().regex(TIME_REGEX,
// customMessage) leaf. A custom regex message keeps zod's code as
// "invalid_format" (format: "regex") — only .refine() produces "custom".
func s4l_checkTime(issues *[]httpx.FieldIssue, key string, raw json.RawMessage, required bool, customMessage string) *string {
	if raw == nil {
		if required {
			*issues = append(*issues, s4p_invalidType(key, "string", nil))
		}
		return nil
	}
	if s4p_receivedType(raw) != "string" {
		*issues = append(*issues, s4p_invalidType(key, "string", raw))
		return nil
	}
	var s string
	_ = json.Unmarshal(raw, &s)
	if !s4l_timeRegex.MatchString(s) {
		*issues = append(*issues, httpx.FieldIssue{Code: "invalid_format", Path: key, Message: customMessage})
		return nil
	}
	return &s
}

// s4l_locationInput is the validated shape of Create/UpdateLocationSchema.
type s4l_locationInput struct {
	Name           *string
	Address        *string
	ShopNumber     *string
	Landmark       *string
	OpeningTime    *string
	ClosingTime    *string
	OffersDelivery *bool
	OffersPickup   *bool
}

// s4l_validateLocation walks the schema keys in declaration order collecting
// ALL issues (zod does not short-circuit across object properties).
// required selects CreateLocationSchema vs UpdateLocationSchema.
func s4l_validateLocation(m map[string]json.RawMessage, required bool) (*s4l_locationInput, []httpx.FieldIssue) {
	in := &s4l_locationInput{}
	issues := make([]httpx.FieldIssue, 0, 4)

	in.Name = s4p_checkString(&issues, "name", m["name"], required, 2)
	in.Address = s4p_checkString(&issues, "address", m["address"], required, 2)
	in.ShopNumber = s4p_optString(&issues, "shopNumber", m["shopNumber"])
	in.Landmark = s4p_optString(&issues, "landmark", m["landmark"])
	in.OpeningTime = s4l_checkTime(&issues, "openingTime", m["openingTime"], required,
		"openingTime must be in 24-hour HH:mm format")
	in.ClosingTime = s4l_checkTime(&issues, "closingTime", m["closingTime"], required,
		"closingTime must be in 24-hour HH:mm format")
	in.OffersDelivery = s4p_checkBool(&issues, "offersDelivery", m["offersDelivery"], false)
	in.OffersPickup = s4p_checkBool(&issues, "offersPickup", m["offersPickup"], false)

	if len(issues) > 0 {
		return nil, issues
	}
	return in, nil
}

// s4l_fulfillmentRefine is the ".refine((data) => data.offersDelivery ||
// data.offersPickup, ...)" check — zod code "custom", path "" (object-level).
func s4l_fulfillmentRefine(offersDelivery, offersPickup bool) []httpx.FieldIssue {
	if offersDelivery || offersPickup {
		return nil
	}
	return []httpx.FieldIssue{{
		Code: "custom", Path: "",
		Message: "A location must offer delivery, pickup, or both",
	}}
}

// ---- POST /locations -------------------------------------------------------

// s4l_locationCreate ports LocationsService.create: dto spread over
// businessId, offersDelivery/offersPickup default true when absent.
func s4l_locationCreate(deps LocationsWriteDeps) http.HandlerFunc {
	q := gen.New(deps.Pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		m, ok := s4p_rawBody(w, r)
		if !ok {
			return
		}
		in, issues := s4l_validateLocation(m, true)
		if issues != nil {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		offersDelivery := true
		if in.OffersDelivery != nil {
			offersDelivery = *in.OffersDelivery
		}
		offersPickup := true
		if in.OffersPickup != nil {
			offersPickup = *in.OffersPickup
		}
		if issues := s4l_fulfillmentRefine(offersDelivery, offersPickup); issues != nil {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		created, err := q.CreateLocation(r.Context(), gen.CreateLocationParams{
			ID:             s4p_newUUID(),
			BusinessID:     bizID,
			Name:           deref(in.Name),
			Address:        deref(in.Address),
			ShopNumber:     s4p_nillableText(in.ShopNumber),
			Landmark:       s4p_nillableText(in.Landmark),
			OpeningTime:    deref(in.OpeningTime),
			ClosingTime:    deref(in.ClosingTime),
			OffersDelivery: offersDelivery,
			OffersPickup:   offersPickup,
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusCreated, ep_location(created))
	}
}

// ---- PATCH /locations/{id} -------------------------------------------------

// s4l_locationPatch ports LocationsService.update: ownership check via
// findOne, partial UPDATE of only provided columns, then the "at least one
// fulfillment flag" refinement re-checked against the MERGED (existing +
// incoming) row — mirrors the Node service's own merge-then-validate.
func s4l_locationPatch(deps LocationsWriteDeps) http.HandlerFunc {
	q := gen.New(deps.Pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		id := chi.URLParam(r, "id")

		m, ok := s4p_rawBody(w, r)
		if !ok {
			return
		}
		in, issues := s4l_validateLocation(m, false)
		if issues != nil {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		existing, err := q.GetLocationByIDAndBusiness(r.Context(), gen.GetLocationByIDAndBusinessParams{
			ID: id, BusinessID: bizID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Location not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		offersDelivery := existing.OffersDelivery
		if in.OffersDelivery != nil {
			offersDelivery = *in.OffersDelivery
		}
		offersPickup := existing.OffersPickup
		if in.OffersPickup != nil {
			offersPickup = *in.OffersPickup
		}
		if issues := s4l_fulfillmentRefine(offersDelivery, offersPickup); issues != nil {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		sets := make([]s4p_set, 0, 8)
		if in.Name != nil {
			sets = append(sets, s4p_set{"name", *in.Name})
		}
		if in.Address != nil {
			sets = append(sets, s4p_set{"address", *in.Address})
		}
		if in.ShopNumber != nil {
			sets = append(sets, s4p_set{"shop_number", s4p_textArg(in.ShopNumber)})
		}
		if in.Landmark != nil {
			sets = append(sets, s4p_set{"landmark", s4p_textArg(in.Landmark)})
		}
		if in.OpeningTime != nil {
			sets = append(sets, s4p_set{"opening_time", *in.OpeningTime})
		}
		if in.ClosingTime != nil {
			sets = append(sets, s4p_set{"closing_time", *in.ClosingTime})
		}
		if in.OffersDelivery != nil {
			sets = append(sets, s4p_set{"offers_delivery", *in.OffersDelivery})
		}
		if in.OffersPickup != nil {
			sets = append(sets, s4p_set{"offers_pickup", *in.OffersPickup})
		}

		row, err := s4l_execUpdate(r.Context(), deps.Pool, id, sets)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusOK, ep_location(row))
	}
}

// s4l_execUpdate builds the partial UPDATE (Prisma data spread) and re-reads
// the payload columns in one statement, mirroring products_write.go's
// s4p_execUpdate for the business_locations table.
func s4l_execUpdate(ctx context.Context, pool *pgxpool.Pool, id string, sets []s4p_set) (gen.BusinessLocation, error) {
	query := "UPDATE business_locations SET "
	args := make([]any, 0, len(sets)+1)
	for i, s := range sets {
		if i > 0 {
			query += ", "
		}
		args = append(args, s.val)
		query += fmt.Sprintf("%s = $%d", s.col, len(args))
	}
	query += ", updated_at = CURRENT_TIMESTAMP WHERE id = $" + fmt.Sprint(len(args)+1)
	args = append(args, id)
	query += ` RETURNING id, business_id, name, address, shop_number, landmark, opening_time,
		closing_time, offers_delivery, offers_pickup, is_active, created_at, updated_at`

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return gen.BusinessLocation{}, err
	}
	defer rows.Close()
	var row gen.BusinessLocation
	if !rows.Next() {
		return gen.BusinessLocation{}, pgx.ErrNoRows
	}
	if err := rows.Scan(&row.ID, &row.BusinessID, &row.Name, &row.Address, &row.ShopNumber,
		&row.Landmark, &row.OpeningTime, &row.ClosingTime, &row.OffersDelivery, &row.OffersPickup,
		&row.IsActive, &row.CreatedAt, &row.UpdatedAt); err != nil {
		return gen.BusinessLocation{}, err
	}
	return row, nil
}

// ---- DELETE /locations/{id} ------------------------------------------------

// s4l_locationDelete ports LocationsService.remove: 404 when the row isn't
// scoped to this business, otherwise delete and confirm.
func s4l_locationDelete(deps LocationsWriteDeps) http.HandlerFunc {
	q := gen.New(deps.Pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		id := chi.URLParam(r, "id")

		affected, err := q.DeleteLocationByIDAndBusiness(r.Context(), gen.DeleteLocationByIDAndBusinessParams{
			ID: id, BusinessID: bizID,
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		if affected == 0 {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Location not found"))
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{
			"message": "Location deleted successfully",
		})
	}
}
