package queue

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
)

// captureLogger returns a logger plus a pointer to its buffer.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, nil)), &buf
}

func countRecords(buf *bytes.Buffer, substr string) int {
	return strings.Count(buf.String(), substr)
}

// T6.4/T6.5: every failed attempt logs; the permanent-failure FATAL record and
// the Sentry hook fire exactly once, only when retries are exhausted.
func TestS6ReportFailureHookOnceOnExhaustion(t *testing.T) {
	orig := SentryHook
	defer func() { SentryHook = orig }()

	var hookCalls atomic.Int64
	SentryHook = func(string, error) { hookCalls.Add(1) }

	log, buf := captureLogger()
	const taskType = "embedding:embed-product"

	ReportFailure(log, FailureInfo{TaskType: taskType, Retried: 0, MaxRetry: 2}, errors.New("boom"))
	if got := hookCalls.Load(); got != 0 {
		t.Fatalf("hook fired on attempt 1 (%d), want 0", got)
	}
	ReportFailure(log, FailureInfo{TaskType: taskType, Retried: 1, MaxRetry: 2}, errors.New("boom"))
	if got := hookCalls.Load(); got != 0 {
		t.Fatalf("hook fired on attempt 2 (%d), want 0", got)
	}

	ReportFailure(log, FailureInfo{TaskType: taskType, Retried: 2, MaxRetry: 2}, errors.New("boom"))
	if got := hookCalls.Load(); got != 1 {
		t.Fatalf("hook fired %d times on exhaustion, want exactly 1", got)
	}

	logs := buf.String()
	// Three attempt records plus the permanent record (which carries both the
	// per-attempt attrs and the escalation attrs).
	if n := countRecords(buf, `"event":"task_failed",`); n != 4 {
		t.Errorf("per-attempt events = %d, want 4", n)
	}
	if n := countRecords(buf, "task_failed_permanently"); n != 1 {
		t.Errorf("permanent-failure records = %d, want 1", n)
	}
	if n := countRecords(buf, `"severity":"fatal"`); n != 1 {
		t.Errorf("fatal severity records = %d, want 1", n)
	}
	if !strings.Contains(logs, `"max_attempts":3`) || !strings.Contains(logs, `"attempt":3`) {
		t.Error("attempt/max_attempts fields missing or wrong")
	}
}

func TestS6ReportFailureNilSafe(t *testing.T) {
	orig := SentryHook
	defer func() { SentryHook = orig }()
	SentryHook = nil

	// nil hook + nil logger must not panic.
	ReportFailure(nil, FailureInfo{TaskType: "x:y", Retried: 5, MaxRetry: 5}, errors.New("boom"))
}

type boomErr struct{ msg string }

func (e *boomErr) Error() string { return e.msg }

func TestS6RecoverConvertsPanicToStackedError(t *testing.T) {
	h := Recover(func(ctx context.Context, payload []byte) error {
		panic(errors.New("kaboom")) // panic with an error value must unwrap
	})
	err := h(context.Background(), []byte(`{}`))
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %T(%v), want *PanicError", err, err)
	}
	if pe.Value == nil {
		t.Error("PanicError.Value lost")
	}
	if !strings.Contains(pe.Error(), "panic:") || !strings.Contains(pe.Error(), "kaboom") {
		t.Errorf("Error() = %q, want panic-prefixed message with value", pe.Error())
	}
	if unwrapped := errors.Unwrap(err); unwrapped == nil || unwrapped.Error() != "kaboom" {
		t.Errorf("Unwrap() = %v, want underlying error", unwrapped)
	}
	if len(pe.StackTrace()) == 0 || !bytes.Contains(pe.StackTrace(), []byte("goroutine")) {
		t.Error("stack trace missing/garbled")
	}
}

func TestS6RunStageWrapsAndLogs(t *testing.T) {
	log, buf := captureLogger()
	sentinel := &boomErr{msg: "db down"}

	err := RunStage(context.Background(), log, "order_ledger", func(ctx context.Context) error {
		return sentinel
	})
	var se *StageError
	if !errors.As(err, &se) {
		t.Fatalf("err = %T(%v), want *StageError", err, err)
	}
	if se.Stage != "order_ledger" {
		t.Errorf("stage = %q, want order_ledger", se.Stage)
	}
	var target *boomErr
	if !errors.As(err, &target) || target.msg != "db down" {
		t.Errorf("original error not retrievable through wrap: %v", err)
	}
	if want := "stage order_ledger failed: db down"; se.Error() != want {
		t.Errorf("Error() = %q, want %q", se.Error(), want)
	}
	if n := countRecords(buf, `"event":"stage_failed"`); n != 1 {
		t.Errorf("stage_failed records = %d, want 1", n)
	}
	if !strings.Contains(buf.String(), `"stage":"order_ledger"`) {
		t.Error("structured stage field missing")
	}

	buf.Reset()
	if err := RunStage(context.Background(), log, "profile_builder", func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Errorf("success path returned %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("success path logged: %s", buf.String())
	}
}

func TestS6StageErrorCarriesPanicStack(t *testing.T) {
	base := Recover(func(ctx context.Context, payload []byte) error {
		panic("raw value")
	})(context.Background(), nil)
	wrapped := &StageError{Stage: "s", Err: base}
	if !bytes.Contains(wrapped.StackTrace(), []byte("goroutine")) {
		t.Error("StageError failed to surface inner PanicError stack")
	}
}
