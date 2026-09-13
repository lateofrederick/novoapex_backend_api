package observability

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

func TestSentryLogHandler_ForwardsLogsAndRaisesFatalIssues(t *testing.T) {
	transport := &sentry.MockTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:       "https://public@sentry.example.com/1",
		Transport: transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub := sentry.NewHub(client, sentry.NewScope())
	ctx := sentry.SetHubOnContext(context.Background(), hub)

	logger := slog.New(newSentryLogHandler(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})))
	logger.With("service", "worker").WithGroup("req").InfoContext(ctx, "request completed",
		slog.String("method", "GET"), slog.Int("status", 200))
	logger.DebugContext(ctx, "below LOG_LEVEL is not forwarded")
	logger.Log(ctx, LevelFatal, "payment event permanently failed", slog.Any("err", errors.New("paystack down")))

	hub.Flush(2 * time.Second)

	var logs []sentry.Log
	var issues []*sentry.Event
	for _, ev := range transport.Events() {
		logs = append(logs, ev.Logs...)
		if len(ev.Exception) > 0 {
			issues = append(issues, ev)
		}
	}

	byBody := map[string]sentry.Log{}
	for _, l := range logs {
		byBody[l.Body] = l
	}
	info, ok := byBody["request completed"]
	if !ok {
		t.Fatalf("info record not forwarded to Sentry Logs; got %d logs", len(logs))
	}
	if info.Level != sentry.LogLevelInfo {
		t.Errorf("level = %s, want info", info.Level)
	}
	for key, want := range map[string]string{"service": "worker", "req.method": "GET", "req.status": "200"} {
		if got := info.Attributes[key].AsString(); got != want {
			t.Errorf("attribute %s = %q, want %q", key, got, want)
		}
	}
	if _, ok := byBody["below LOG_LEVEL is not forwarded"]; ok {
		t.Error("records below the handler level must not reach Sentry")
	}
	if fatal, ok := byBody["payment event permanently failed"]; !ok || fatal.Level != sentry.LogLevelFatal {
		t.Errorf("fatal record not forwarded as a fatal log: %+v", fatal)
	}

	if len(issues) != 1 {
		t.Fatalf("issues = %d, want exactly 1 (fatal only)", len(issues))
	}
	if issues[0].Level != sentry.LevelFatal {
		t.Errorf("issue level = %s, want fatal", issues[0].Level)
	}
	if got := issues[0].Exception[len(issues[0].Exception)-1].Value; got != "payment event permanently failed: paystack down" {
		t.Errorf("issue exception = %q", got)
	}
}
