package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
)

// Pinger abstracts a dependency connectivity probe.
type Pinger interface {
	Ping(ctx context.Context) error
}

// HealthDeps carries the checked dependencies; nil means the dependency
// is unavailable and reports down exactly as the Node indicators do when
// Prisma/Redis fail to connect.
type HealthDeps struct {
	DB    Pinger
	Redis Pinger
}

// Terminus indicator keys from apps/api/src/health/health.service.ts:28-30.
const (
	hlthDBKey    = "prisma"
	hlthRedisKey = "redis"
)

// Fallback messages from prisma.health.ts:28 / redis.health.ts:39.
const (
	hlthDBDownMessage    = "Database connection failed"
	hlthRedisDownMessage = "Redis connection failed"
)

// hlthIndicator reproduces HealthIndicatorService.up()/down() output;
// field order matches JS insertion order for down(): message then status.
type hlthIndicator struct {
	Message string `json:"message,omitempty"`
	Status  string `json:"status"`
}

// hlthCheckResult reproduces HealthCheckExecutor.getResult().
type hlthCheckResult struct {
	Status  string                   `json:"status"`
	Info    map[string]hlthIndicator `json:"info"`
	Error   map[string]hlthIndicator `json:"error"`
	Details map[string]hlthIndicator `json:"details"`
}

// MountHealth wires the health probes onto the kernel:
//
//	GET /health       full check (prisma + redis), 200 or filtered 503
//	GET /health/live  bare {"status":"ok"} liveness
//	GET /health/ready full check, same contract as /health
func (k *Kernel) MountHealth(deps HealthDeps) {
	k.Router.Get("/health", k.hlthHandler(deps))
	k.Router.Get("/health/live", hlthLive)
	k.Router.Get("/health/ready", k.hlthHandler(deps))
}

// hlthLive ports HealthController.liveness (health.controller.ts:34-37).
func hlthLive(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (k *Kernel) hlthHandler(deps HealthDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// @HealthCheck() decorator sets Cache-Control as route metadata,
		// so it applies regardless of outcome (health-check.decorator.js:30).
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		result := hlthRunChecks(r.Context(), deps)
		if len(result.Error) == 0 && !k.ShuttingDown() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			enc := json.NewEncoder(w)
			enc.SetEscapeHTML(false)
			_ = enc.Encode(result)
			return
		}

		if k.ShuttingDown() {
			result.Status = "shutting_down"
		}

		// HealthCheckService throws ServiceUnavailableException(result);
		// the global exception filter wraps it into the standard envelope.
		WriteError(w, r, NewHTTPException(http.StatusServiceUnavailable, result))
	}
}

// hlthRunChecks executes both indicators concurrently like Terminus'
// Promise.allSettled executor.
func hlthRunChecks(ctx context.Context, deps HealthDeps) hlthCheckResult {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		info = map[string]hlthIndicator{}
		errs = map[string]hlthIndicator{}
	)

	check := func(key string, p Pinger, downMessage string) {
		defer wg.Done()
		res := hlthIndicator{Status: "up"}
		if p == nil {
			res = hlthIndicator{Message: downMessage, Status: "down"}
		} else if err := p.Ping(ctx); err != nil {
			msg := err.Error()
			if errors.Is(err, context.Canceled) || msg == "" {
				msg = downMessage
			}
			res = hlthIndicator{Message: msg, Status: "down"}
		}

		mu.Lock()
		defer mu.Unlock()
		if res.Status == "up" {
			info[key] = res
		} else {
			errs[key] = res
		}
	}

	wg.Add(2)
	go check(hlthDBKey, deps.DB, hlthDBDownMessage)
	go check(hlthRedisKey, deps.Redis, hlthRedisDownMessage)
	wg.Wait()

	details := map[string]hlthIndicator{}
	for key, v := range info {
		details[key] = v
	}
	for key, v := range errs {
		details[key] = v
	}

	status := "ok"
	if len(errs) > 0 {
		status = "error"
	}
	return hlthCheckResult{
		Status:  status,
		Info:    info,
		Error:   errs,
		Details: details,
	}
}
