package handlers

import (
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

func s4p_devicesHandler(e *s4pEnv) http.Handler {
	r := chi.NewRouter()
	r.Route("/devices", func(dr chi.Router) {
		MountDevices(dr, DevicesDeps{Pool: e.Pool})
	})
	return s4p_withClaims(r)
}

func TestDeviceTokenValidation(t *testing.T) {
	e := s4p_start(t)
	h := s4p_devicesHandler(e)

	biz := e.F.Business()
	claims := auth.Claims{BusinessID: biz.ID, Phone: biz.OwnerPhone}

	// 1. Missing token
	rec := s4p_json(t, h, http.MethodPost, "/devices/push-token", &claims, map[string]any{
		"platform": "ios",
	})
	s4p_status(t, rec, http.StatusBadRequest)

	// 2. Invalid platform
	rec = s4p_json(t, h, http.MethodPost, "/devices/push-token", &claims, map[string]any{
		"token":    "ExponentPushToken[abc123xyz]",
		"platform": "blackberry",
	})
	s4p_status(t, rec, http.StatusBadRequest)

	// 3. No business in context
	rec = s4p_json(t, h, http.MethodPost, "/devices/push-token", &auth.Claims{Phone: biz.OwnerPhone}, map[string]any{
		"token":    "ExponentPushToken[abc123xyz]",
		"platform": "ios",
	})
	s4p_status(t, rec, http.StatusForbidden)
}

func TestDeviceTokenUpsertAndDelete(t *testing.T) {
	e := s4p_start(t)
	h := s4p_devicesHandler(e)

	biz := e.F.Business()
	claims := auth.Claims{BusinessID: biz.ID, Phone: biz.OwnerPhone}
	expoToken := "ExponentPushToken[device-test-token-12345]"

	// Register device token
	rec := s4p_json(t, h, http.MethodPost, "/devices/push-token", &claims, map[string]any{
		"token":      expoToken,
		"platform":   "ios",
		"deviceName": "Vendor iPhone",
	})
	s4p_status(t, rec, http.StatusOK)
	body := s4p_decode(t, rec)
	if s4p_str(body, "expoPushToken") != expoToken {
		t.Fatalf("expected token %s, got %v", expoToken, body["expoPushToken"])
	}
	if s4p_str(body, "platform") != "ios" {
		t.Fatalf("expected platform ios, got %v", body["platform"])
	}
	if s4p_str(body, "deviceName") != "Vendor iPhone" {
		t.Fatalf("expected deviceName 'Vendor iPhone', got %v", body["deviceName"])
	}

	// Upsert again with changed name
	rec = s4p_json(t, h, http.MethodPost, "/devices/push-token", &claims, map[string]any{
		"token":      expoToken,
		"platform":   "ios",
		"deviceName": "Vendor iPhone Updated",
	})
	s4p_status(t, rec, http.StatusOK)
	body = s4p_decode(t, rec)
	if s4p_str(body, "deviceName") != "Vendor iPhone Updated" {
		t.Fatalf("expected deviceName 'Vendor iPhone Updated', got %v", body["deviceName"])
	}

	// Delete device token
	rec = s4p_json(t, h, http.MethodDelete, "/devices/push-token", &claims, map[string]any{
		"token": expoToken,
	})
	s4p_status(t, rec, http.StatusOK)
	delBody := s4p_decode(t, rec)
	if delBody["success"] != true {
		t.Fatalf("expected success true, got %v", delBody)
	}
}
