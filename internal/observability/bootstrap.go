package observability

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
)

// BootstrapConfig describes the process being started.
type BootstrapConfig struct {
	Service  string // "api" | "worker"
	NodeEnv  string // NODE_ENV
	LogLevel string // LOG_LEVEL
}

// Bootstrap wires process-wide structured logging and DSN-gated Sentry for an
// entrypoint (instrument.ts + LoggerModule). It returns the logger (also set
// as slog's default), whether Sentry is active, and a flush func to defer so
// buffered events survive a rolling deploy (SentryFlushService).
//
// Log PII policy mirrors LoggerModule: phone numbers and conversation ids are
// redacted outside production and logged in full in production, where they
// are needed operationally. Sentry scrubbing is governed separately by
// SENTRY_SEND_PII (see InitObservability).
func Bootstrap(cfg BootstrapConfig) (*slog.Logger, bool, func()) {
	enabled, initErr := InitObservability(ConfigFromEnv(os.Getenv))
	if initErr != nil {
		enabled = false
	}

	logger := NewLogger(LoggerConfig{
		Level:   cfg.LogLevel,
		Context: cfg.Service,
		SendPII: cfg.NodeEnv == "production",
	})
	if enabled {
		// Tee every record to Sentry Logs (+ fatal records as issues).
		logger = slog.New(newSentryLogHandler(logger.Handler()))
		sentry.ConfigureScope(func(scope *sentry.Scope) { scope.SetTag("service", cfg.Service) })
	}
	slog.SetDefault(logger)

	if initErr != nil {
		logger.Error("sentry init failed — continuing without error reporting", slog.Any("error", initErr))
	}
	if enabled {
		logger.Info("sentry error reporting enabled")
	}
	flush := func() {
		if enabled {
			sentry.Flush(2 * time.Second)
		}
	}
	return logger, enabled, flush
}

// CaptureTaskFailure reports a queue task whose retries are exhausted (the
// processors' fatal-level onFailed path).
func CaptureTaskFailure(taskType string, err error) {
	hub := sentry.CurrentHub().Clone()
	hub.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("task", taskType)
		scope.SetLevel(sentry.LevelFatal)
		hub.CaptureException(err)
	})
}

// CapturePanic reports a recovered process-level panic before re-panicking
// (the worker's uncaughtException safety net).
func CapturePanic(ctx context.Context) {
	if r := recover(); r != nil {
		sentry.CurrentHub().RecoverWithContext(ctx, r)
		sentry.Flush(2 * time.Second)
		panic(r)
	}
}
