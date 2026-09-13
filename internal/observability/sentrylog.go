package observability

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

// sentryLogHandler ports the Sentry pino integration configured in
// libs/observability/src/instrument.ts: every record the process logs (at or
// above LOG_LEVEL) is also forwarded to Sentry's Logs product (enableLogs),
// and fatal records are additionally raised as Sentry issues
// (pinoIntegration({error:{levels:['fatal']}})) — carrying the record's error
// attribute, when present, so the issue has a real exception.
//
// PII in forwarded logs is scrubbed by the client's BeforeSendLog unless
// SENTRY_SEND_PII=true (InitObservability).
type sentryLogHandler struct {
	next   slog.Handler
	attrs  []slog.Attr
	groups []string
}

func newSentryLogHandler(next slog.Handler) slog.Handler {
	return &sentryLogHandler{next: next}
}

func (h *sentryLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *sentryLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	prefixed := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	prefixed = append(prefixed, h.attrs...)
	for _, a := range attrs {
		prefixed = append(prefixed, prefixAttr(h.groups, a))
	}
	return &sentryLogHandler{next: h.next.WithAttrs(attrs), attrs: prefixed, groups: h.groups}
}

func (h *sentryLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	groups := append(append([]string(nil), h.groups...), name)
	return &sentryLogHandler{next: h.next.WithGroup(name), attrs: h.attrs, groups: groups}
}

func (h *sentryLogHandler) Handle(ctx context.Context, rec slog.Record) error {
	err := h.next.Handle(ctx, rec)

	attrs := append([]slog.Attr(nil), h.attrs...)
	var cause error
	rec.Attrs(func(a slog.Attr) bool {
		if e, ok := a.Value.Any().(error); ok && cause == nil {
			cause = e
		}
		attrs = append(attrs, prefixAttr(h.groups, a))
		return true
	})

	logger := sentry.NewLogger(ctx)
	entry := sentryEntry(logger, rec.Level)
	flat := map[string]string{}
	for _, a := range attrs {
		flattenAttr(flat, "", a)
	}
	for k, v := range flat {
		entry = entry.String(k, v)
	}
	entry.Emit(rec.Message)

	if rec.Level >= LevelFatal {
		hub := sentry.GetHubFromContext(ctx)
		if hub == nil {
			hub = sentry.CurrentHub()
		}
		hub = hub.Clone()
		hub.WithScope(func(scope *sentry.Scope) {
			scope.SetLevel(sentry.LevelFatal)
			logCtx := sentry.Context{}
			for k, v := range flat {
				logCtx[k] = v
			}
			scope.SetContext("log", logCtx) // scrubbed by BeforeSend unless SENTRY_SEND_PII
			if cause != nil {
				hub.CaptureException(fmt.Errorf("%s: %w", rec.Message, cause))
				return
			}
			hub.CaptureMessage(rec.Message)
		})
	}
	return err
}

func sentryEntry(l sentry.Logger, level slog.Level) sentry.LogEntry {
	switch {
	case level >= LevelFatal:
		return l.LFatal()
	case level >= slog.LevelError:
		return l.Error()
	case level >= slog.LevelWarn:
		return l.Warn()
	case level >= slog.LevelInfo:
		return l.Info()
	case level >= slog.LevelDebug:
		return l.Debug()
	default:
		return l.Trace()
	}
}

func prefixAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 {
		return a
	}
	return slog.Attr{Key: strings.Join(groups, ".") + "." + a.Key, Value: a.Value}
}

// flattenAttr renders nested groups as dotted keys with string values — the
// attribute shape Sentry logs index and search on.
func flattenAttr(out map[string]string, prefix string, a slog.Attr) {
	key := a.Key
	if prefix != "" {
		key = prefix + "." + key
	}
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindGroup:
		for _, ga := range v.Group() {
			flattenAttr(out, key, ga)
		}
	case slog.KindTime:
		out[key] = v.Time().UTC().Format(time.RFC3339Nano)
	default:
		if e, ok := v.Any().(error); ok {
			out[key] = e.Error()
			return
		}
		out[key] = fmt.Sprint(v.Any())
	}
}
