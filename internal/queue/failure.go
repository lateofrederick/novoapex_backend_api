package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/hibiken/asynq"
)

// Failure plumbing (T6.4 explicit failure events, T6.5 permanent-failure
// reporting, T6.6 stage context, T6.7 panic safety nets).
//
// SentryHook mirrors the httpx.SentryHook pattern: a nil-able process-global
// hook that main wires to sentry.CaptureException; tests swap it out. It is
// nil-safe everywhere — never dereferenced unset.
var SentryHook func(taskType string, err error)

// FailureInfo describes one failed attempt as seen by asynq's ErrorHandler.
type FailureInfo struct {
	TaskType string
	// Retried is the zero-based index of the attempt that just failed.
	Retried int
	// MaxRetry is the configured retry budget (retries after first attempt).
	MaxRetry int
}

// Exhausted reports whether this was the final allowed attempt.
func (f FailureInfo) Exhausted() bool { return f.Retried >= f.MaxRetry }

// StackProvider is implemented by errors carrying a captured stack (panics).
type StackProvider interface {
	StackTrace() []byte
}

// ReportFailure handles one failed attempt:
//
//   - every attempt logs an error line with attempt/maxAttempts (the explicit
//     failure event that Nest's @OnQueueEvent silently dropped — T6.4);
//   - on exhaustion it logs a FATAL-classified record with the full stack when
//     available and fires SentryHook exactly once (T6.5, matching the
//     processors' `attemptsMade >= maxAttempts - 1` captureMessage path).
//
// Safe under nil logger and nil hook.
func ReportFailure(log *slog.Logger, info FailureInfo, err error) {
	if log == nil {
		log = slog.Default()
	}
	attrs := []any{
		slog.String("event", "task_failed"),
		slog.String("task", info.TaskType),
		slog.Int("attempt", info.Retried+1),
		slog.Int("max_attempts", info.MaxRetry+1),
		slog.Any("error", err),
	}
	if sp, ok := err.(StackProvider); ok {
		attrs = append(attrs, slog.String("stack", string(sp.StackTrace())))
	}
	log.Error("task processing failed", attrs...)

	if !info.Exhausted() {
		return
	}

	fatalAttrs := append(attrs[:len(attrs):len(attrs)],
		slog.String("event", "task_failed_permanently"),
		slog.String("severity", "fatal"))
	log.Error("task retries exhausted — permanent failure", fatalAttrs...)

	if SentryHook != nil {
		SentryHook(info.TaskType, err)
	}
}

// PanicError converts a recovered panic into an error carrying the stack at
// recovery time (T6.7 — Go equivalent of unhandledRejection/uncaughtException
// handlers in apps/worker/src/main.ts).
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

func (e *PanicError) StackTrace() []byte { return e.Stack }

// Recover wraps a Handler so a panic becomes an error with a full stack,
// which flows through the normal retry/failure machinery instead of crashing
// the worker goroutine.
func Recover(h Handler) (wrapped Handler) {
	return func(ctx context.Context, payload []byte) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &PanicError{Value: r, Stack: debug.Stack()}
			}
		}()
		return h(ctx, payload)
	}
}

// StageError annotates an error with the pipeline stage that produced it
// (T6.6 — port of runStage in crm-materialiser.processor.ts, which wraps each
// handler call so "a failure anywhere in the chain names the stage that broke
// before it propagates").
type StageError struct {
	Stage string
	Err   error
}

func (e *StageError) Error() string { return fmt.Sprintf("stage %s failed: %v", e.Stage, e.Err) }

func (e *StageError) Unwrap() error { return e.Err }

func (e *StageError) StackTrace() []byte {
	var sp StackProvider
	if errors.As(e.Err, &sp) {
		return sp.StackTrace()
	}
	return nil
}

// RunStage runs fn and, on failure, logs a structured stage-failure record
// (event/stage/attempt fields like the Node original) and returns the error
// wrapped in StageError so callers upstream see which stage broke. The
// original error remains retrievable via errors.Unwrap/errors.Is/As.
func RunStage(ctx context.Context, log *slog.Logger, stage string, fn func(ctx context.Context) error) error {
	if log == nil {
		log = slog.Default()
	}
	if err := fn(ctx); err != nil {
		log.Error("pipeline stage failed",
			slog.String("event", "stage_failed"),
			slog.String("stage", stage),
			slog.Any("error", err))
		return &StageError{Stage: stage, Err: err}
	}
	return nil
}

// discardError marks a failure as terminal AND frees the task's id: asynq
// runs the ErrorHandler (so ReportFailure/Sentry still fire) and then deletes
// the task instead of retrying or archiving it.
//
// Use it for fixed-TaskID debounce jobs. An archived task keeps its id
// reserved, and asynq rejects any enqueue that reuses the id — so one failed
// orchestrator run would otherwise silently swallow every later message from
// that customer.
type discardError struct{ err error }

func (e *discardError) Error() string   { return e.err.Error() }
func (e *discardError) Unwrap() []error { return []error{e.err, asynq.RevokeTask} }

// Discard wraps err so the failed task is reported but neither retried nor
// archived. A nil err stays nil.
func Discard(err error) error {
	if err == nil {
		return nil
	}
	return &discardError{err: err}
}
