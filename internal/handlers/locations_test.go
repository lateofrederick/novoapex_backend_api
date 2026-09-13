package handlers

// locations_test.go — physical shop locations CRUD (Node, this session's
// apps/mobile-api/src/locations/*.ts port): create/list/get/patch/delete,
// the "must offer delivery, pickup, or both" refinement on both create and
// update, and cross-tenant 404 scoping (T0.17 discipline).

import (
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

func s4l_handler(e *s4pEnv) http.Handler {
	r := chi.NewRouter()
	r.Route("/locations", func(l chi.Router) {
		l.Mount("/", NewLocations(e.Pool))
		MountLocationsWrite(l, LocationsWriteDeps{Pool: e.Pool})
	})
	return s4p_withClaims(r)
}

func TestS4L_LocationsCRUD(t *testing.T) {
	e := s4p_start(t)
	h := s4l_handler(e)
	biz := e.F.Business()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}

	var locationID string

	t.Run("create with defaults", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodPost, "/locations", &claims, map[string]any{
			"name":        "Main Street Shop",
			"address":     "12 Main Street",
			"openingTime": "08:00",
			"closingTime": "18:00",
		})
		s4p_status(t, rec, http.StatusCreated)
		body := s4p_decode(t, rec)
		locationID = s4p_str(body, "id")
		if locationID == "" {
			t.Fatal("no id in response")
		}
		if body["businessId"] != biz.ID {
			t.Errorf("businessId = %v, want %s", body["businessId"], biz.ID)
		}
		if body["offersDelivery"] != true || body["offersPickup"] != true {
			t.Errorf("defaults = %v/%v, want true/true", body["offersDelivery"], body["offersPickup"])
		}
		if body["isActive"] != true {
			t.Errorf("isActive = %v, want true", body["isActive"])
		}
		if body["shopNumber"] != nil || body["landmark"] != nil {
			t.Errorf("optional fields = %v/%v, want nil/nil", body["shopNumber"], body["landmark"])
		}
	})

	t.Run("create rejects name/address shorter than 2 chars", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodPost, "/locations", &claims, map[string]any{
			"name": "A", "address": "12 Main Street", "openingTime": "08:00", "closingTime": "18:00",
		})
		s4p_status(t, rec, http.StatusBadRequest)
	})

	t.Run("create rejects malformed opening/closing time", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodPost, "/locations", &claims, map[string]any{
			"name": "Shop Two", "address": "1 Second St", "openingTime": "8am", "closingTime": "18:00",
		})
		s4p_status(t, rec, http.StatusBadRequest)
		body := s4p_decode(t, rec)
		errs, ok := body["errors"].([]any)
		if !ok || len(errs) == 0 {
			t.Fatalf("expected zod errors, got %v", body)
		}
	})

	t.Run("create rejects delivery=false pickup=false", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodPost, "/locations", &claims, map[string]any{
			"name": "Shop Three", "address": "3 Third St", "openingTime": "08:00", "closingTime": "18:00",
			"offersDelivery": false, "offersPickup": false,
		})
		s4p_status(t, rec, http.StatusBadRequest)
	})

	t.Run("get by id", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodGet, "/locations/"+locationID, &claims, nil)
		s4p_status(t, rec, http.StatusOK)
		body := s4p_decode(t, rec)
		if s4p_str(body, "id") != locationID {
			t.Errorf("id = %v, want %s", body["id"], locationID)
		}
	})

	t.Run("list is paginated", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodGet, "/locations?page=1&limit=20", &claims, nil)
		s4p_status(t, rec, http.StatusOK)
		body := s4p_decode(t, rec)
		data, ok := body["data"].([]any)
		if !ok || len(data) == 0 {
			t.Fatalf("data = %v", body["data"])
		}
		meta, ok := body["meta"].(map[string]any)
		if !ok || meta["page"] == nil || meta["limit"] == nil || meta["total"] == nil {
			t.Fatalf("meta = %v", body["meta"])
		}
	})

	t.Run("patch partial update", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodPatch, "/locations/"+locationID, &claims, map[string]any{
			"landmark": "Opposite the post office",
		})
		s4p_status(t, rec, http.StatusOK)
		body := s4p_decode(t, rec)
		if body["landmark"] != "Opposite the post office" {
			t.Errorf("landmark = %v", body["landmark"])
		}
		if body["name"] != "Main Street Shop" {
			t.Errorf("unrelated field name changed: %v", body["name"])
		}
	})

	t.Run("patch rejects disabling the only remaining fulfillment option", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodPatch, "/locations/"+locationID, &claims, map[string]any{
			"offersDelivery": false, "offersPickup": false,
		})
		s4p_status(t, rec, http.StatusBadRequest)
	})

	t.Run("patch allows disabling one flag when the other stays on (merge against existing row)", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodPatch, "/locations/"+locationID, &claims, map[string]any{
			"offersDelivery": false,
		})
		s4p_status(t, rec, http.StatusOK)
		body := s4p_decode(t, rec)
		if body["offersDelivery"] != false || body["offersPickup"] != true {
			t.Errorf("offersDelivery/offersPickup = %v/%v, want false/true", body["offersDelivery"], body["offersPickup"])
		}
	})

	t.Run("delete", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodDelete, "/locations/"+locationID, &claims, nil)
		s4p_status(t, rec, http.StatusOK)

		rec2 := s4p_json(t, h, http.MethodGet, "/locations/"+locationID, &claims, nil)
		s4p_status(t, rec2, http.StatusNotFound)
	})
}

func TestS4L_LocationsCrossTenant404(t *testing.T) {
	e := s4p_start(t)
	h := s4l_handler(e)
	bizA := e.F.Business()
	bizB := e.F.Business()
	claimsA := auth.Claims{Phone: bizA.OwnerPhone, BusinessID: bizA.ID}
	claimsB := auth.Claims{Phone: bizB.OwnerPhone, BusinessID: bizB.ID}

	rec := s4p_json(t, h, http.MethodPost, "/locations", &claimsA, map[string]any{
		"name": "A's Shop", "address": "1 A Street", "openingTime": "08:00", "closingTime": "18:00",
	})
	s4p_status(t, rec, http.StatusCreated)
	locationID := s4p_str(s4p_decode(t, rec), "id")

	t.Run("get across tenants 404s", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodGet, "/locations/"+locationID, &claimsB, nil)
		s4p_status(t, rec, http.StatusNotFound)
	})
	t.Run("patch across tenants 404s", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodPatch, "/locations/"+locationID, &claimsB, map[string]any{"landmark": "x"})
		s4p_status(t, rec, http.StatusNotFound)
	})
	t.Run("delete across tenants 404s and does not delete", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodDelete, "/locations/"+locationID, &claimsB, nil)
		s4p_status(t, rec, http.StatusNotFound)

		still := s4p_json(t, h, http.MethodGet, "/locations/"+locationID, &claimsA, nil)
		s4p_status(t, still, http.StatusOK)
	})
	t.Run("list scoped to owning business only", func(t *testing.T) {
		rec := s4p_json(t, h, http.MethodGet, "/locations?page=1&limit=20", &claimsB, nil)
		s4p_status(t, rec, http.StatusOK)
		body := s4p_decode(t, rec)
		for _, raw := range body["data"].([]any) {
			row := raw.(map[string]any)
			if row["id"] == locationID {
				t.Fatalf("business B's list leaked business A's location")
			}
		}
	})
}
