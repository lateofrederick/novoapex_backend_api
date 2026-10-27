package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// DevicesDeps carries dependencies for device token routes.
type DevicesDeps struct {
	Pool *pgxpool.Pool
}

// MountDevices registers the device push token routes.
func MountDevices(r chi.Router, deps DevicesDeps) {
	r.Post("/push-token", devicesRegisterToken(deps))
	r.Delete("/push-token", devicesDeleteToken(deps))
}

type deviceTokenResponse struct {
	ID            string  `json:"id"`
	BusinessID    string  `json:"businessId"`
	ExpoPushToken string  `json:"expoPushToken"`
	Platform      string  `json:"platform"`
	DeviceName    *string `json:"deviceName"`
	UpdatedAt     isoTime `json:"updatedAt"`
}

func devicesRegisterToken(deps DevicesDeps) http.HandlerFunc {
	q := gen.New(deps.Pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		claims, ok := auth.FromContext(r.Context())
		if !ok || claims.Phone == "" {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusUnauthorized, "Unauthorized"))
			return
		}

		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, "Invalid JSON body"))
			return
		}

		var issues []httpx.FieldIssue
		tokenVal := s4p_checkString(&issues, "token", raw["token"], true, 1)
		platformVal := s4p_checkString(&issues, "platform", raw["platform"], true, 1)
		deviceNameVal := s4p_checkString(&issues, "deviceName", raw["deviceName"], false, 0)

		if platformVal != nil {
			p := strings.ToLower(*platformVal)
			if p != "ios" && p != "android" && p != "web" {
				issues = append(issues, httpx.FieldIssue{
					Code:    "custom",
					Path:    "platform",
					Message: "platform must be one of: ios, android, web",
				})
			}
		}

		if len(issues) > 0 {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		var devName pgtype.Text
		if deviceNameVal != nil && *deviceNameVal != "" {
			devName = pgtype.Text{String: *deviceNameVal, Valid: true}
		}

		row, err := q.UpsertDeviceToken(r.Context(), gen.UpsertDeviceTokenParams{
			ID:            uuid.NewString(),
			BusinessID:    bizID,
			OwnerPhone:    claims.Phone,
			ExpoPushToken: *tokenVal,
			Platform:      strings.ToLower(*platformVal),
			DeviceName:    devName,
		})
		if err != nil {
			httpx.WriteError(w, r, fmt.Errorf("upsert device token: %w", err))
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, deviceTokenResponse{
			ID:            row.ID,
			BusinessID:    row.BusinessID,
			ExpoPushToken: row.ExpoPushToken,
			Platform:      row.Platform,
			DeviceName:    ep_text(row.DeviceName),
			UpdatedAt:     epISO(row.UpdatedAt.Time),
		})
	}
}

func devicesDeleteToken(deps DevicesDeps) http.HandlerFunc {
	q := gen.New(deps.Pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		var raw map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, "Invalid JSON body"))
			return
		}

		var issues []httpx.FieldIssue
		tokenVal := s4p_checkString(&issues, "token", raw["token"], true, 1)
		if len(issues) > 0 {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		_, err := q.DeleteDeviceToken(r.Context(), gen.DeleteDeviceTokenParams{
			ExpoPushToken: *tokenVal,
			BusinessID:    bizID,
		})
		if err != nil {
			httpx.WriteError(w, r, fmt.Errorf("delete device token: %w", err))
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, map[string]any{"success": true})
	}
}
