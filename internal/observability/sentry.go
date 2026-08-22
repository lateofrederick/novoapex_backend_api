package observability

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"

	"github.com/getsentry/sentry-go"
	"github.com/getsentry/sentry-go/attribute"
)

const RedactedValue = "[redacted]"

const (
	DefaultTracesSampleRate   = 0.1
	DefaultProfilesSampleRate = 1.0
)

const maxScrubDepth = 6

var piiKeys = map[string]struct{}{
	"senderphone":    {},
	"recipientphone": {},
	"customerphone":  {},
	"ownerphone":     {},
	"phone":          {},
	"conversationid": {},
}

func IsPIIKey(key string) bool {
	if key == "" {
		return false
	}
	_, ok := piiKeys[strings.ToLower(key)]
	return ok
}

func ScrubPII(value map[string]any) map[string]any {
	scrubMap(value, 0)
	return value
}

func scrubMap(value map[string]any, depth int) {
	for key, child := range value {
		if IsPIIKey(key) {
			value[key] = RedactedValue
			continue
		}
		value[key] = scrubValue(child, depth+1)
	}
}

func scrubValue(value any, depth int) any {
	if value == nil || depth > maxScrubDepth {
		return value
	}
	switch typed := value.(type) {
	case map[string]any:
		scrubMap(typed, depth)
	case []any:
		for i := range typed {
			typed[i] = scrubValue(typed[i], depth+1)
		}
	case []map[string]any:
		for i := range typed {
			scrubMap(typed[i], depth+1)
		}
	}
	return value
}

type Config struct {
	DSN                string
	Environment        string
	NodeEnv            string
	Release            string
	SendPII            bool
	TracesSampleRate   *float64
	ProfilesSampleRate *float64
}

func ConfigFromEnv(getenv func(string) string) Config {
	cfg := Config{
		DSN:         getenv("SENTRY_DSN"),
		Environment: getenv("SENTRY_ENVIRONMENT"),
		NodeEnv:     getenv("NODE_ENV"),
		Release:     getenv("SENTRY_RELEASE"),
		SendPII:     getenv("SENTRY_SEND_PII") == "true",
	}
	if f, ok := parseRate(getenv("SENTRY_TRACES_SAMPLE_RATE")); ok {
		cfg.TracesSampleRate = &f
	}
	if f, ok := parseRate(getenv("SENTRY_PROFILES_SAMPLE_RATE")); ok {
		cfg.ProfilesSampleRate = &f
	}
	return cfg
}

func parseRate(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func (c Config) ResolveEnvironment() string {
	if c.Environment != "" {
		return c.Environment
	}
	if c.NodeEnv != "" {
		return c.NodeEnv
	}
	return "development"
}

func (c Config) TracesRate() float64 {
	if c.TracesSampleRate != nil {
		return *c.TracesSampleRate
	}
	return DefaultTracesSampleRate
}

func (c Config) ProfilesRate() float64 {
	if c.ProfilesSampleRate != nil {
		return *c.ProfilesSampleRate
	}
	return DefaultProfilesSampleRate
}

func InitObservability(cfg Config) (bool, error) {
	if cfg.DSN == "" {
		return false, nil
	}
	options := sentry.ClientOptions{
		Dsn:              cfg.DSN,
		Environment:      cfg.ResolveEnvironment(),
		Release:          cfg.Release,
		TracesSampleRate: cfg.TracesRate(),
		BeforeSend:       NewBeforeSend(cfg.SendPII),
		BeforeSendLog:    NewBeforeSendLog(cfg.SendPII),
	}
	if err := sentry.Init(options); err != nil {
		return false, err
	}
	return true, nil
}

func NewBeforeSend(sendPII bool) func(*sentry.Event, *sentry.EventHint) *sentry.Event {
	if sendPII {
		return nil
	}
	return func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
		if event == nil {
			return nil
		}
		for _, ctx := range event.Contexts {
			if ctx != nil {
				ScrubPII(ctx)
			}
		}
		for _, crumb := range event.Breadcrumbs {
			if crumb != nil && crumb.Data != nil {
				ScrubPII(crumb.Data)
			}
		}
		if event.Request != nil {
			event.Request.Data = scrubJSONString(event.Request.Data)
		}
		return event
	}
}

func NewBeforeSendLog(sendPII bool) func(*sentry.Log) *sentry.Log {
	if sendPII {
		return nil
	}
	return func(entry *sentry.Log) *sentry.Log {
		if entry == nil {
			return nil
		}
		for key := range entry.Attributes {
			if IsPIIKey(key) {
				entry.Attributes[key] = attribute.StringValue(RedactedValue)
			}
		}
		return entry
	}
}

func scrubJSONString(data string) string {
	trimmed := strings.TrimSpace(data)
	if trimmed == "" || (trimmed[0] != '{' && trimmed[0] != '[') {
		return data
	}
	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return data
	}
	scrubbed := scrubValue(decoded, 0)
	encoded, err := json.Marshal(scrubbed)
	if err != nil {
		return data
	}
	return string(encoded)
}

func EnvGetter() func(string) string {
	return os.Getenv
}
