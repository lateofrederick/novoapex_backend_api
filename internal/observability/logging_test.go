package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"
)

const obs_TimeToleranceMs = 60_000

func obs_DecodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatalf("expected at least one log line, got %q", buf.String())
	}
	out := make([]map[string]any, 0, len(lines))
	for i, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("line %d is not valid json: %v (%q)", i+1, err, line)
		}
		out = append(out, decoded)
	}
	return out
}

func obs_AssertNumber(t *testing.T, record map[string]any, key string, want float64) {
	t.Helper()
	value, ok := record[key].(float64)
	if !ok {
		t.Fatalf("key %q = %#v (%T), want JSON number", key, record[key], record[key])
	}
	if value != want {
		t.Fatalf("key %q = %v, want %v", key, value, want)
	}
}

func obs_AssertString(t *testing.T, record map[string]any, key, want string) {
	t.Helper()
	value, ok := record[key].(string)
	if !ok {
		t.Fatalf("key %q = %#v (%T), want JSON string", key, record[key], record[key])
	}
	if value != want {
		t.Fatalf("key %q = %q, want %q", key, value, want)
	}
}

func obs_AssertEpochMillis(t *testing.T, record map[string]any, key string) {
	t.Helper()
	value, ok := record[key].(float64)
	if !ok {
		t.Fatalf("time must be a JSON number (pino epochTime), got %#v (%T)", record[key], record[key])
	}
	if value != math.Trunc(value) {
		t.Fatalf("time must be integer epoch milliseconds, got %v", value)
	}
	nowMs := float64(time.Now().UnixMilli())
	if math.Abs(value-nowMs) > obs_TimeToleranceMs {
		t.Fatalf("time = %v, want epoch-ms within %vms of now (%v)", value, obs_TimeToleranceMs, nowMs)
	}
}

func TestObsT028LogFieldNamesMatchPino(t *testing.T) {
	obs_TestT028_LogFieldNamesMatchPino(t)
}

func obs_TestT028_LogFieldNamesMatchPino(t *testing.T) {
	t.Run("plain info record matches pino raw contract", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewLogger(LoggerConfig{
			Context:  "MessagesService",
			Output:   &buf,
			PID:      4242,
			Hostname: "obs-host",
		})
		logger.Info("Conversation outbound turn")

		lines := obs_DecodeLines(t, &buf)
		if len(lines) != 1 {
			t.Fatalf("expected exactly one line, got %d", len(lines))
		}
		record := lines[0]
		wantKeys := []string{"level", "time", "msg", "pid", "hostname", "context"}
		if len(record) != len(wantKeys) {
			t.Fatalf("unexpected key set %#v, want exactly %v", record, wantKeys)
		}
		for _, key := range wantKeys {
			if _, ok := record[key]; !ok {
				t.Fatalf("missing pino contract key %q in %#v", key, record)
			}
		}
		obs_AssertNumber(t, record, "level", PinoLevelInfo)
		obs_AssertEpochMillis(t, record, "time")
		obs_AssertNumber(t, record, "pid", 4242)
		obs_AssertString(t, record, "hostname", "obs-host")
		obs_AssertString(t, record, "context", "MessagesService")
		obs_AssertString(t, record, "msg", "Conversation outbound turn")
	})

	t.Run("slog levels map to identical pino numbers", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewLogger(LoggerConfig{Level: "trace", Output: &buf})
		logger.Debug("d")
		logger.Info("i")
		logger.Warn("w")
		logger.Error("e")
		Fatal(context.Background(), logger, "f")

		lines := obs_DecodeLines(t, &buf)
		if len(lines) != 5 {
			t.Fatalf("expected 5 records incl fatal, got %d", len(lines))
		}
		wantLevels := []float64{PinoLevelDebug, PinoLevelInfo, PinoLevelWarn, PinoLevelError, PinoLevelFatal}
		for i, want := range wantLevels {
			level, ok := lines[i]["level"].(float64)
			if !ok {
				t.Fatalf("record %d level not a number: %#v", i, lines[i]["level"])
			}
			if level != want {
				t.Fatalf("record %d level = %v, want %v", i, level, want)
			}
		}
	})

	t.Run("level filter follows LOG_LEVEL names", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewLogger(LoggerConfig{Level: "warn", Output: &buf})
		logger.Info("suppressed")
		logger.Warn("kept")

		lines := obs_DecodeLines(t, &buf)
		if len(lines) != 1 {
			t.Fatalf("info must be filtered at warn level, got %d lines", len(lines))
		}
		obs_AssertNumber(t, lines[0], "level", PinoLevelWarn)

		if ParseLogLevel("") != slog.LevelInfo {
			t.Fatal("default LOG_LEVEL is info")
		}
		if PinoLevelNumber(LevelTrace) != PinoLevelTrace {
			t.Fatal("trace must map to pino 10")
		}
	})

	t.Run("send pii false scrubs attributes before emit", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewLogger(LoggerConfig{
			Output:   &buf,
			PID:      1,
			Hostname: "h",
			Context:  "PaymentsService",
		})
		logger.Info("charge attempt",
			"senderPhone", "+15551234567",
			"conversationId", "conv_42",
			"amount", 1500,
			"meta", map[string]any{"customerPhone": "+15559990000", "order": "o-1"},
		)

		lines := obs_DecodeLines(t, &buf)
		record := lines[len(lines)-1]
		obs_AssertString(t, record, "senderPhone", "[redacted]")
		obs_AssertString(t, record, "conversationId", "[redacted]")
		meta, ok := record["meta"].(map[string]any)
		if !ok {
			t.Fatalf("meta group lost: %#v", record["meta"])
		}
		obs_AssertString(t, meta, "customerPhone", "[redacted]")
		obs_AssertString(t, meta, "order", "o-1")
		obs_AssertNumber(t, record, "amount", 1500)
		raw := buf.String()
		if strings.Contains(raw, "+15551234567") || strings.Contains(raw, "+15559990000") || strings.Contains(raw, "conv_42") {
			t.Fatalf("raw pii leaked to output: %s", raw)
		}
	})

	t.Run("send pii true keeps values verbatim", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewLogger(LoggerConfig{
			Output:   &buf,
			SendPII:  true,
			PID:      1,
			Hostname: "h",
		})
		logger.Info("allowed", "senderPhone", "+15551234567")
		record := obs_DecodeLines(t, &buf)[0]
		obs_AssertString(t, record, "senderPhone", "+15551234567")
	})

	t.Run("base fields can be disabled and context attached dynamically", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewLogger(LoggerConfig{Output: &buf, NoBaseFields: true})
		child := WithContext(logger, "WorkerService")
		child.Info("job done")

		record := obs_DecodeLines(t, &buf)[0]
		if _, ok := record["pid"]; ok {
			t.Fatal("NoBaseFields must drop pid")
		}
		if _, ok := record["hostname"]; ok {
			t.Fatal("NoBaseFields must drop hostname")
		}
		obs_AssertString(t, record, "context", "WorkerService")
		obs_AssertNumber(t, record, "level", PinoLevelInfo)
		obs_AssertEpochMillis(t, record, "time")
	})

	t.Run("each record is one ndjson line", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewLogger(LoggerConfig{Output: &buf, NoBaseFields: true})
		logger.Info("one")
		logger.Info("two")
		lines := obs_DecodeLines(t, &buf)
		if len(lines) != 2 {
			t.Fatalf("want 2 ndjson lines, got %d", len(lines))
		}
		obs_AssertString(t, lines[0], "msg", "one")
		obs_AssertString(t, lines[1], "msg", "two")
	})
}
