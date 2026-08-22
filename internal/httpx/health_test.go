package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func hlthServe(t *testing.T, deps HealthDeps, url string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	k := New()
	k.MountHealth(deps)
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	k.Handler().ServeHTTP(w, r)
	return w, envDecode(t, w.Body.Bytes())
}

func TestHealth_AllUp(t *testing.T) {
	deps := HealthDeps{DB: fakePinger{}, Redis: fakePinger{}}
	w, body := hlthServe(t, deps, "/health")

	if w.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", w.Code)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache, no-store, must-revalidate" {
		t.Errorf("cache-control = %q", cc)
	}

	upIndicator := map[string]any{"status": "up"}
	want := map[string]any{
		"status": "ok",
		"info": map[string]any{
			"prisma": upIndicator,
			"redis":  upIndicator,
		},
		"error": map[string]any{},
		"details": map[string]any{
			"prisma": upIndicator,
			"redis":  upIndicator,
		},
	}
	if len(body) != 4 {
		t.Errorf("top-level keys = %v", body)
	}
	for k, v := range want {
		if !envEqual(body[k], v) {
			t.Errorf("%s = %#v, want %#v", k, body[k], v)
		}
	}
}

func TestHealth_DBDown_Partial503(t *testing.T) {
	deps := HealthDeps{
		DB:    fakePinger{err: errors.New("connection refused")},
		Redis: fakePinger{},
	}
	for _, path := range []string{"/health", "/health/ready"} {
		w, body := hlthServe(t, deps, path)

		// Terminus throws ServiceUnavailableException(result); the global
		// filter wraps it: {statusCode, timestamp, path, message:{...}}.
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s code = %d, want 503", path, w.Code)
		}
		if got := body["statusCode"]; got != float64(503) {
			t.Errorf("%s statusCode = %v", path, got)
		}
		if got := body["path"]; got != path {
			t.Errorf("%s path = %v", path, got)
		}
		msg, ok := body["message"].(map[string]any)
		if !ok {
			t.Fatalf("%s message not an object: %#v", path, body["message"])
		}
		if msg["status"] != "error" {
			t.Errorf("%s message.status = %v", path, msg["status"])
		}
		info := msg["info"].(map[string]any)
		errs := msg["error"].(map[string]any)
		details := msg["details"].(map[string]any)
		if _, ok := info["prisma"]; ok {
			t.Errorf("%s info.prisma present, want only redis", path)
		}
		envAssertIndicator(t, path+".info.redis", info["redis"], "up", "")
		prismaDown := errs["prisma"].(map[string]any)
		if prismaDown["status"] != "down" || prismaDown["message"] != "connection refused" {
			t.Errorf("%s error.prisma = %v", path, prismaDown)
		}
		if _, ok := errs["redis"]; ok {
			t.Errorf("%s error.redis present, want empty", path)
		}
		envAssertIndicator(t, path+".details.prisma", details["prisma"], "down", "connection refused")
		envAssertIndicator(t, path+".details.redis", details["redis"], "up", "")
	}
}

func TestHealth_RedisDown_UsesFallbackMessage(t *testing.T) {
	deps := HealthDeps{
		DB:    fakePinger{},
		Redis: fakePinger{err: errors.New("")}, // ioredis error.message fallback path
	}
	w, body := hlthServe(t, deps, "/health/ready")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", w.Code)
	}
	msg := body["message"].(map[string]any)
	down := msg["error"].(map[string]any)["redis"].(map[string]any)
	if down["status"] != "down" || down["message"] != "Redis connection failed" {
		t.Errorf("error.redis = %v, want fallback message", down)
	}
}

func TestHealth_NilDeps_ReportDown(t *testing.T) {
	w, body := hlthServe(t, HealthDeps{}, "/health")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", w.Code)
	}
	msg := body["message"].(map[string]any)
	dbDown := msg["error"].(map[string]any)["prisma"].(map[string]any)
	redisDown := msg["error"].(map[string]any)["redis"].(map[string]any)
	if dbDown["status"] != "down" || dbDown["message"] != "Database connection failed" {
		t.Errorf("error.prisma = %v", dbDown)
	}
	if redisDown["status"] != "down" || redisDown["message"] != "Redis connection failed" {
		t.Errorf("error.redis = %v", redisDown)
	}
}

func TestHealth_Live(t *testing.T) {
	w := httptest.NewRecorder()
	k := New()
	k.MountHealth(HealthDeps{})
	r := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	k.Handler().ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("code = %d", w.Code)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "" {
		t.Errorf("live must not carry Cache-Control, got %q", cc)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body) != 1 || body["status"] != "ok" {
		t.Errorf("body = %v, want exactly {status:ok}", body)
	}
}

func TestHealth_ShuttingDown(t *testing.T) {
	k := New()
	k.MountHealth(HealthDeps{DB: fakePinger{}, Redis: fakePinger{}})
	k.BeginShutdown()

	w := httptest.NewRecorder()
	k.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", w.Code)
	}
	body := envDecode(t, w.Body.Bytes())
	msg := body["message"].(map[string]any)
	if msg["status"] != "shutting_down" {
		t.Errorf("message.status = %v, want shutting_down", msg["status"])
	}
	info := msg["info"].(map[string]any)
	if len(info) != 2 {
		t.Errorf("info during shutdown should still report indicators: %v", info)
	}
}

func TestHealth_CacheControlPresentOn503Too(t *testing.T) {
	w, _ := hlthServe(t, HealthDeps{DB: fakePinger{err: errors.New("x")}}, "/health")
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache, no-store, must-revalidate" {
		t.Errorf("cache-control = %q", cc)
	}
}

func envAssertIndicator(t *testing.T, name string, got any, status, message string) {
	t.Helper()
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("%s not an indicator object: %#v", name, got)
	}
	if m["status"] != status {
		t.Errorf("%s.status = %v, want %s", name, m["status"], status)
	}
	if message == "" {
		if _, exists := m["message"]; exists {
			t.Errorf("%s has unexpected message key (Terminus up() emits only status)", name)
		}
	} else if m["message"] != message {
		t.Errorf("%s.message = %v, want %q", name, m["message"], message)
	}
}

func envEqual(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}
