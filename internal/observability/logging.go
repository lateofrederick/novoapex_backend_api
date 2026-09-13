package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	PinoLevelTrace = 10
	PinoLevelDebug = 20
	PinoLevelInfo  = 30
	PinoLevelWarn  = 40
	PinoLevelError = 50
	PinoLevelFatal = 60
)

const LevelFatal = slog.LevelError + 8
const LevelTrace = slog.LevelDebug - 4

// LevelSilent is above every emitted level: LOG_LEVEL=silent disables logs.
const LevelSilent = LevelFatal + 4

type LoggerConfig struct {
	Level        string
	Context      string
	Output       io.Writer
	SendPII      bool
	PID          int
	Hostname     string
	NoBaseFields bool
}

func NewLogger(cfg LoggerConfig) *slog.Logger {
	out := cfg.Output
	if out == nil {
		out = os.Stdout
	}
	var handler slog.Handler = slog.NewJSONHandler(out, &slog.HandlerOptions{
		Level:       ParseLogLevel(cfg.Level),
		ReplaceAttr: ObsReplacePinoAttrs,
	})
	if !cfg.SendPII {
		handler = obsScrubHandler{next: handler}
	}
	logger := slog.New(handler)
	if !cfg.NoBaseFields {
		pid := cfg.PID
		if pid == 0 {
			pid = os.Getpid()
		}
		hostname := cfg.Hostname
		if hostname == "" {
			hostname, _ = os.Hostname()
		}
		logger = logger.With(slog.Int("pid", pid), slog.String("hostname", hostname))
	}
	if cfg.Context != "" {
		logger = WithContext(logger, cfg.Context)
	}
	return logger
}

func ParseLogLevel(name string) slog.Leveler {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "trace":
		return LevelTrace
	case "debug":
		return slog.LevelDebug
	case "info", "":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "fatal":
		return LevelFatal
	case "silent":
		return LevelSilent
	default:
		return slog.LevelInfo
	}
}

func PinoLevelNumber(level slog.Level) int64 {
	switch {
	case level >= LevelFatal:
		return PinoLevelFatal
	case level >= slog.LevelError:
		return PinoLevelError
	case level >= slog.LevelWarn:
		return PinoLevelWarn
	case level >= slog.LevelInfo:
		return PinoLevelInfo
	case level >= slog.LevelDebug:
		return PinoLevelDebug
	default:
		return PinoLevelTrace
	}
}

func ObsReplacePinoAttrs(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 {
		return a
	}
	switch a.Key {
	case slog.TimeKey:
		if t, ok := a.Value.Any().(time.Time); ok {
			a.Value = slog.Int64Value(t.UnixMilli())
		}
	case slog.LevelKey:
		if level, ok := a.Value.Any().(slog.Level); ok {
			a.Value = slog.Int64Value(PinoLevelNumber(level))
		}
	}
	return a
}

func WithContext(logger *slog.Logger, context string) *slog.Logger {
	return logger.With(slog.String("context", context))
}

func Fatal(ctx context.Context, logger *slog.Logger, msg string, args ...any) {
	logger.Log(ctx, LevelFatal, msg, args...)
}

type obsScrubHandler struct {
	next slog.Handler
}

func (h obsScrubHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h obsScrubHandler) Handle(ctx context.Context, record slog.Record) error {
	scrubbed := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(a slog.Attr) bool {
		scrubbed.AddAttrs(ObsScrubAttr(a))
		return true
	})
	return h.next.Handle(ctx, scrubbed)
}

func (h obsScrubHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	scrubbed := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		scrubbed[i] = ObsScrubAttr(a)
	}
	return obsScrubHandler{next: h.next.WithAttrs(scrubbed)}
}

func (h obsScrubHandler) WithGroup(name string) slog.Handler {
	return obsScrubHandler{next: h.next.WithGroup(name)}
}

func ObsScrubAttr(a slog.Attr) slog.Attr {
	if IsPIIKey(a.Key) {
		return slog.String(a.Key, RedactedValue)
	}
	value := a.Value.Resolve()
	switch value.Kind() {
	case slog.KindGroup:
		group := value.Group()
		for i := range group {
			group[i] = ObsScrubAttr(group[i])
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(group...)}
	case slog.KindAny:
		switch typed := value.Any().(type) {
		case map[string]any:
			ScrubPII(typed)
			return slog.Any(a.Key, typed)
		case []any:
			for i := range typed {
				typed[i] = scrubValue(typed[i], 1)
			}
			return slog.Any(a.Key, typed)
		case []map[string]any:
			for i := range typed {
				scrubMap(typed[i], 1)
			}
			return slog.Any(a.Key, typed)
		}
	}
	return a
}
