package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestKernel_SmokeHealth(t *testing.T) {
	k := New()
	k.MountHealth(HealthDeps{DB: fakePinger{}, Redis: fakePinger{}})

	srv := httptest.NewServer(k.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	if out["status"] != "ok" {
		t.Errorf("status = %v", out["status"])
	}
}

func TestKernel_StartAndShutdown(t *testing.T) {
	k := New()
	k.MountHealth(HealthDeps{DB: fakePinger{}, Redis: fakePinger{}})

	srv, err := k.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	addr := srv.Addr
	resp, err := http.Get("http://" + addr + "/health/live")
	if err != nil {
		t.Fatalf("GET /health/live: %v", err)
	}
	resp.Body.Close() //nolint:errcheck // probe response
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/health/live status = %d", resp.StatusCode)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := Shutdown(shutdownCtx, srv); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestKernel_Recoverer(t *testing.T) {
	k := New()
	panicRoute := "/krn-panic-test-" + t.Name()
	k.Router.Get(panicRoute, func(w http.ResponseWriter, _ *http.Request) {
		panic("boom")
	})

	srv := httptest.NewServer(k.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + panicRoute)
	if err != nil {
		t.Fatalf("request after panic failed (server died?): %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 from Recoverer", resp.StatusCode)
	}

	resp2, err := http.Get(srv.URL + panicRoute)
	if err != nil {
		t.Fatalf("second request: %v", err)
	}
	_ = resp2.Body.Close()
}

func TestShutdown_NilServer(t *testing.T) {
	if err := Shutdown(context.Background(), nil); !errors.Is(err, nil) {
		t.Errorf("nil server shutdown = %v, want nil", err)
	}
}
