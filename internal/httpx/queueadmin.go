package httpx

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
)

// QueueDashboardPath mirrors BullBoardModule.forRootAsync({route: '/admin/queues'})
// (libs/queue/src/queue-admin.module.ts).
const QueueDashboardPath = "/admin/queues"

// QueueDashboardConfig configures MountQueueDashboard.
type QueueDashboardConfig struct {
	Redis    asynq.RedisConnOpt
	User     string // BULL_BOARD_USER
	Password string // BULL_BOARD_PASSWORD
}

// MountQueueDashboard ports QueueAdminModule: the queue dashboard (asynqmon,
// the asynq counterpart of Bull Board) served by the API process under
// /admin/queues, behind BasicAuth. It returns a closer for the dashboard's
// Redis connections.
func MountQueueDashboard(r chi.Router, cfg QueueDashboardConfig) func() error {
	dashboard := asynqmon.New(asynqmon.Options{
		RootPath:     QueueDashboardPath,
		RedisConnOpt: cfg.Redis,
	})
	guarded := BasicAuth(cfg.User, cfg.Password)(dashboard)
	r.Handle(QueueDashboardPath, guarded)
	r.Handle(QueueDashboardPath+"/*", guarded)
	return dashboard.Close
}

// BasicAuth ports createBullBoardAuthMiddleware
// (libs/queue/src/bull-board-auth.middleware.ts). The dashboard exposes raw
// job payloads (customer phone numbers, message contents, payment webhooks),
// so it fails closed: with no credentials configured every request gets 503;
// otherwise a missing or wrong Authorization header gets 401 with the realm
// challenge. Both comparisons always run to avoid short-circuit timing leaks.
func BasicAuth(user, password string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if user == "" || password == "" {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("Queue dashboard is disabled: set BULL_BOARD_USER and BULL_BOARD_PASSWORD."))
				return
			}
			if u, p, ok := r.BasicAuth(); ok && strings.EqualFold(strings.SplitN(r.Header.Get("Authorization"), " ", 2)[0], "Basic") {
				userOK := subtle.ConstantTimeCompare([]byte(u), []byte(user)) == 1
				passOK := subtle.ConstantTimeCompare([]byte(p), []byte(password)) == 1
				if userOK && passOK {
					next.ServeHTTP(w, r)
					return
				}
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="NovoApex Queue Dashboard"`)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("Authentication required"))
		})
	}
}
