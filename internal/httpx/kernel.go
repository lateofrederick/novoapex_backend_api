package httpx

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	metrics "github.com/novoapex/novoapex-backend-api/internal/observability/metrics"
)

// Kernel is the HTTP kernel: chi router with base middleware, health
// endpoints and a /metrics passthrough (Nest MetricsModule served
// GET /metrics via nestjs-prometheus).
type Kernel struct {
	Router chi.Router

	shuttingDown atomic.Bool
}

// New builds the kernel. Extra middlewares (error tracking and tracing) run
// inside the base stack, so the recoverer still answers a panic they re-raise.
func New(extra ...func(http.Handler) http.Handler) *Kernel {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP) //nolint:staticcheck // mandated parity with Express app.set('trust proxy', 1)
	r.Use(RequestLogger)
	r.Use(middleware.Recoverer)
	r.Use(extra...)

	// /metrics passthrough: Prometheus exposition endpoint.
	r.Method(http.MethodGet, "/metrics", metrics.Handler())

	return &Kernel{Router: r}
}

func (k *Kernel) Handler() http.Handler {
	return k.Router
}

// Start binds addr and serves in a background goroutine. The returned
// server is ready to accept connections when Start returns without error.
func (k *Kernel) Start(addr string) (*http.Server, error) {
	srv := &http.Server{
		Addr:              addr,
		Handler:           k.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv.Addr = ln.Addr().String()
	go func() {
		_ = srv.Serve(ln)
	}()
	return srv, nil
}

// BeginShutdown marks the kernel as shutting down; /health and
// /health/ready start reporting Terminus' "shutting_down" status,
// mirroring HealthCheckExecutor.beforeApplicationShutdown.
func (k *Kernel) BeginShutdown() {
	k.shuttingDown.Store(true)
}

func (k *Kernel) ShuttingDown() bool {
	return k.shuttingDown.Load()
}

// Shutdown gracefully drains srv using ctx as deadline. Callers wire
// SIGTERM/SIGINT to this with a 10s timeout, matching
// app.enableShutdownHooks() semantics from the Node service.
func Shutdown(ctx context.Context, srv *http.Server) error {
	if srv == nil {
		return nil
	}
	err := srv.Shutdown(ctx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
