package observability

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/getsentry/sentry-go"
	"github.com/getsentry/sentry-go/attribute"
)

func obs_Getenv(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

func obs_NestedPhone(depth int) map[string]any {
	root := map[string]any{}
	current := root
	for i := 0; i < depth; i++ {
		next := map[string]any{}
		current["a"] = next
		current = next
	}
	current["phone"] = "+15551234567"
	return root
}

func obs_FindNested(m map[string]any, path ...string) any {
	var current any = m
	for _, key := range path {
		typed, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = typed[key]
	}
	return current
}

func TestObsT027SentryScrubMirrorsInstrument(t *testing.T) {
	obs_TestT027_SentryScrubMirrorsInstrument(t)
}

func obs_TestT027_SentryScrubMirrorsInstrument(t *testing.T) {
	t.Run("redacts every PII key case insensitively", func(t *testing.T) {
		input := map[string]any{
			"senderPhone":    "+15551234567",
			"recipientPhone": 15551234567,
			"customerPhone":  true,
			"ownerPhone":     []string{"x"},
			"Phone":          map[string]any{"deep": 1},
			"phone":          "short",
			"CONVERSATIONID": "conv_123",
			"ConversationId": 42,
			"email":          "a@b.c",
			"count":          3,
			"nothing":        nil,
			"list": []any{
				"keep",
				map[string]any{"phone": "+15559999999"},
				nil,
				map[string]any{"keepMe": "yes"},
			},
			"typedMaps": []map[string]any{{"conversationId": "c7"}},
			"deep": map[string]any{
				"deeper": map[string]any{
					"deepest": map[string]any{"customerPhone": "+15550001111"},
				},
			},
		}
		want := map[string]any{
			"senderPhone":    "[redacted]",
			"recipientPhone": "[redacted]",
			"customerPhone":  "[redacted]",
			"ownerPhone":     "[redacted]",
			"Phone":          "[redacted]",
			"phone":          "[redacted]",
			"CONVERSATIONID": "[redacted]",
			"ConversationId": "[redacted]",
			"email":          "a@b.c",
			"count":          3,
			"nothing":        nil,
			"list": []any{
				"keep",
				map[string]any{"phone": "[redacted]"},
				nil,
				map[string]any{"keepMe": "yes"},
			},
			"typedMaps": []map[string]any{{"conversationId": "[redacted]"}},
			"deep": map[string]any{
				"deeper": map[string]any{
					"deepest": map[string]any{"customerPhone": "[redacted]"},
				},
			},
		}
		got := ScrubPII(input)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("scrub mismatch:\n got %#v\nwant %#v", got, want)
		}
		if !reflect.DeepEqual(input, want) {
			t.Fatalf("scrub must mutate input in place like instrument.ts")
		}
	})

	t.Run("null valued pii keys are still redacted", func(t *testing.T) {
		got := ScrubPII(map[string]any{"phone": nil, "note": nil})
		if got["phone"] != RedactedValue {
			t.Fatalf("phone = %v, want [redacted]", got["phone"])
		}
		if got["note"] != nil {
			t.Fatalf("note = %v, want nil passthrough", got["note"])
		}
	})

	t.Run("depth cap stops scrubbing past six levels", func(t *testing.T) {
		atSix := ScrubPII(obs_NestedPhone(6))
		if got := obs_FindNested(atSix, obs_Path(6, "a")...); got != RedactedValue {
			t.Fatalf("depth 6 phone = %v, want [redacted]", got)
		}
		atSeven := ScrubPII(obs_NestedPhone(7))
		if got := obs_FindNested(atSeven, obs_Path(7, "a")...); got != "+15551234567" {
			t.Fatalf("depth 7 phone = %v, want untouched value", got)
		}
	})

	t.Run("arrays increase depth like objects", func(t *testing.T) {
		input := map[string]any{
			"arr": []any{map[string]any{"phone": "kept-because-array-adds-depth"}},
		}
		for i := 0; i < 6; i++ {
			input = map[string]any{"a": input}
		}
		scrubbed := ScrubPII(input)
		current := scrubbed
		for i := 0; i < 6; i++ {
			current = current["a"].(map[string]any)
		}
		items := current["arr"].([]any)
		inner := items[0].(map[string]any)
		if inner["phone"] != "kept-because-array-adds-depth" {
			t.Fatalf("expected untouched value past cap through arrays, got %v", inner["phone"])
		}
	})

	t.Run("before send nil when pii allowed", func(t *testing.T) {
		if NewBeforeSend(true) != nil {
			t.Fatal("SENTRY_SEND_PII=true must disable beforeSend like instrument.ts")
		}
		if NewBeforeSendLog(true) != nil {
			t.Fatal("SENTRY_SEND_PII=true must disable beforeSendLog like instrument.ts")
		}
	})

	t.Run("before send scrubs contexts breadcrumbs and request data", func(t *testing.T) {
		hook := NewBeforeSend(false)
		event := &sentry.Event{
			Contexts: map[string]sentry.Context{
				"custom": {"conversationId": "c1", "keep": "yes"},
			},
			Breadcrumbs: []*sentry.Breadcrumb{
				{Data: map[string]any{"customerPhone": "+15550000000", "url": "/x"}},
			},
			Request: &sentry.Request{
				Data: `{"recipientPhone":"+15551112222","items":[{"senderPhone":"+15553334444","qty":2}],"total":10}`,
			},
		}
		got := hook(event, nil)
		if got != event {
			t.Fatal("beforeSend must return the same event pointer")
		}
		if event.Contexts["custom"]["conversationId"] != RedactedValue || event.Contexts["custom"]["keep"] != "yes" {
			t.Fatalf("contexts not scrubbed correctly: %#v", event.Contexts)
		}
		crumbData := event.Breadcrumbs[0].Data
		if crumbData["customerPhone"] != RedactedValue || crumbData["url"] != "/x" {
			t.Fatalf("breadcrumb data not scrubbed correctly: %#v", crumbData)
		}
		var body map[string]any
		if err := json.Unmarshal([]byte(event.Request.Data), &body); err != nil {
			t.Fatalf("request.data must stay valid json: %v", err)
		}
		if body["recipientPhone"] != RedactedValue || body["total"] != float64(10) {
			t.Fatalf("request.data not scrubbed correctly: %s", event.Request.Data)
		}
		items := body["items"].([]any)
		first := items[0].(map[string]any)
		if first["senderPhone"] != RedactedValue || first["qty"] != float64(2) {
			t.Fatalf("nested request.data not scrubbed correctly: %#v", items)
		}
	})

	t.Run("before send leaves non json request data untouched", func(t *testing.T) {
		hook := NewBeforeSend(false)
		event := &sentry.Event{Request: &sentry.Request{Data: "plain text body"}}
		hook(event, nil)
		if event.Request.Data != "plain text body" {
			t.Fatalf("non-json request.data changed: %q", event.Request.Data)
		}
	})

	t.Run("before send log redacts attributes", func(t *testing.T) {
		hook := NewBeforeSendLog(false)
		entry := &sentry.Log{
			Attributes: map[string]attribute.Value{
				"conversationId": attribute.StringValue("c9"),
				"body":           attribute.StringValue("+15551230000"),
				"attempts":       attribute.IntValue(2),
			},
		}
		got := hook(entry)
		if got != entry {
			t.Fatal("beforeSendLog must return the same entry pointer")
		}
		if got.Attributes["conversationId"].AsString() != RedactedValue {
			t.Fatalf("conversationId = %v, want [redacted]", got.Attributes["conversationId"].AsInterface())
		}
		if got.Attributes["body"].AsString() != "+15551230000" {
			t.Fatal("non-keyed values must never be rewritten by key-based scrubbing")
		}
		if got.Attributes["attempts"].AsInt64() != 2 {
			t.Fatal("non-pii typed attributes must be preserved")
		}
	})

	t.Run("init is a no-op without dsn", func(t *testing.T) {
		initialized, err := InitObservability(Config{})
		if err != nil {
			t.Fatalf("empty dsn must not error: %v", err)
		}
		if initialized {
			t.Fatal("empty dsn must be a complete no-op")
		}
		if client := sentry.CurrentHub().Client(); client != nil {
			t.Fatal("sentry.Init must not be called when dsn is empty")
		}
	})

	t.Run("config mirrors instrument.ts env plumbing", func(t *testing.T) {
		cfg := ConfigFromEnv(obs_Getenv(map[string]string{}))
		if cfg.ResolveEnvironment() != "development" {
			t.Fatalf("default environment = %q, want development", cfg.ResolveEnvironment())
		}
		if cfg.SendPII {
			t.Fatal("SENTRY_SEND_PII defaults to false")
		}
		if cfg.TracesRate() != DefaultTracesSampleRate {
			t.Fatalf("default traces rate = %v, want %v", cfg.TracesRate(), DefaultTracesSampleRate)
		}
		if cfg.ProfilesRate() != DefaultProfilesSampleRate {
			t.Fatalf("default profiles rate = %v, want %v", cfg.ProfilesRate(), DefaultProfilesSampleRate)
		}

		cfg = ConfigFromEnv(obs_Getenv(map[string]string{
			"SENTRY_DSN":                  "https://k@o.ingest.sentry.io/42",
			"NODE_ENV":                    "production",
			"SENTRY_SEND_PII":             "true",
			"SENTRY_TRACES_SAMPLE_RATE":   "0.25",
			"SENTRY_PROFILES_SAMPLE_RATE": "0.5",
		}))
		if cfg.ResolveEnvironment() != "production" {
			t.Fatalf("NODE_ENV fallback = %q, want production", cfg.ResolveEnvironment())
		}
		if !cfg.SendPII {
			t.Fatal("SENTRY_SEND_PII=true must parse to SendPII true")
		}
		if cfg.TracesRate() != 0.25 || cfg.ProfilesRate() != 0.5 {
			t.Fatalf("sample rates = %v/%v, want 0.25/0.5", cfg.TracesRate(), cfg.ProfilesRate())
		}

		cfg = ConfigFromEnv(obs_Getenv(map[string]string{
			"NODE_ENV":           "production",
			"SENTRY_ENVIRONMENT": "staging",
		}))
		if cfg.ResolveEnvironment() != "staging" {
			t.Fatalf("SENTRY_ENVIRONMENT must win over NODE_ENV, got %q", cfg.ResolveEnvironment())
		}
	})
}

func obs_Path(depth int, key string) []string {
	path := make([]string, 0, depth+1)
	for i := 0; i < depth; i++ {
		path = append(path, key)
	}
	path = append(path, "phone")
	return path
}
