package httpx

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// RequestLogger ports the pino-http request logging wired by LoggerModule
// (libs/common/src/logger/logger.module.ts): one structured line per request
// once the response is written, with the req serializer's reduced field set —
// id, method, url, query and only the host and user-agent headers, so
// Authorization and Cookie values never reach the logs (the module's redact
// paths). Like pino-http, 5xx responses log at error as "request errored" and
// everything else at info as "request completed".
func RequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		defer func() {
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			level, msg := slog.LevelInfo, "request completed"
			if status >= http.StatusInternalServerError {
				level, msg = slog.LevelError, "request errored"
			}
			query := map[string]any{}
			for k, v := range r.URL.Query() {
				if len(v) == 1 {
					query[k] = v[0]
				} else {
					query[k] = v
				}
			}
			slog.Default().LogAttrs(r.Context(), level, msg,
				slog.Group("req",
					slog.String("id", middleware.GetReqID(r.Context())),
					slog.String("method", r.Method),
					slog.String("url", r.URL.RequestURI()),
					slog.Any("query", query),
					slog.Group("headers",
						slog.String("host", r.Host),
						slog.String("user-agent", r.UserAgent()),
					),
				),
				slog.Group("res", slog.Int("statusCode", status)),
				slog.Int64("responseTime", time.Since(start).Milliseconds()),
			)
		}()
		next.ServeHTTP(ww, r)
	})
}
