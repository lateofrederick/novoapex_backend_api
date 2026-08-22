package config

import (
	"strings"
	"testing"
)

const validJWT = "0123456789abcdef0123456789abcdef"

func baseEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL": "postgresql://postgres:postgres@localhost:5432/novoapex",
		"JWT_SECRET":   validJWT,
	}
}

func mustLoad(t *testing.T, env map[string]string) *Config {
	t.Helper()
	c, err := LoadFrom(env)
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	return c
}

func TestDefaultsMatchNodeSchema(t *testing.T) {
	c := mustLoad(t, baseEnv())

	if c.NodeEnv != "development" {
		t.Errorf("NodeEnv = %q", c.NodeEnv)
	}
	if c.Port != 3000 || c.MobileAPIPort != 3001 {
		t.Errorf("ports = %d/%d", c.Port, c.MobileAPIPort)
	}
	if c.DatabasePoolMax != 10 {
		t.Errorf("DatabasePoolMax = %d", c.DatabasePoolMax)
	}
	if c.DatabasePoolIdleTimeoutMS != 30_000 {
		t.Errorf("IdleTimeout = %d", c.DatabasePoolIdleTimeoutMS)
	}
	if c.DatabasePoolConnectionTimeoutMS != 10_000 {
		t.Errorf("ConnTimeout = %d", c.DatabasePoolConnectionTimeoutMS)
	}
	if c.RedisHost != "localhost" || c.RedisPort != 6379 {
		t.Errorf("redis = %s:%d", c.RedisHost, c.RedisPort)
	}
	if c.RedisTLS || c.SentrySendPII || c.EnableSwagger {
		t.Error("bool flags must default to false")
	}
	if c.SentryTracesSampleRate != 0.1 {
		t.Errorf("TracesSampleRate = %v", c.SentryTracesSampleRate)
	}
	if c.SentryProfilesSampleRate != 1.0 {
		t.Errorf("ProfilesSampleRate = %v", c.SentryProfilesSampleRate)
	}
	if c.LogLevel != "info" {
		t.Errorf("LogLevel = %q", c.LogLevel)
	}
	if c.WhatsAppAPIVersion != "v25.0" {
		t.Errorf("WhatsAppAPIVersion = %q", c.WhatsAppAPIVersion)
	}
	if c.PaystackBaseURL != "https://api.paystack.co" {
		t.Errorf("PaystackBaseURL = %q", c.PaystackBaseURL)
	}
	if c.SMTPHost != "smtp.zoho.com" || c.SMTPPort != 465 {
		t.Errorf("smtp = %s:%d", c.SMTPHost, c.SMTPPort)
	}
	if c.ReengagementThresholdMultiplier != 1.5 {
		t.Errorf("Multiplier = %v", c.ReengagementThresholdMultiplier)
	}
	if c.ReengagementCooldownDays != 30 {
		t.Errorf("CooldownDays = %d", c.ReengagementCooldownDays)
	}
}

func TestRequiredVarsMissing(t *testing.T) {
	for _, key := range []string{"DATABASE_URL", "JWT_SECRET"} {
		env := baseEnv()
		delete(env, key)
		_, err := LoadFrom(env)
		if err == nil {
			t.Errorf("missing %s must fail", key)
			continue
		}
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error for missing %s should name it, got: %v", key, err)
		}
	}
}

func TestJWTSecretLengthValidation(t *testing.T) {
	cases := []struct {
		secret string
		wantOK bool
	}{
		{strings.Repeat("a", 31), false},
		{strings.Repeat("a", 32), true},
		{strings.Repeat("a", 64), true},
	}
	for _, tc := range cases {
		env := baseEnv()
		env["JWT_SECRET"] = tc.secret
		_, err := LoadFrom(env)
		if tc.wantOK && err != nil {
			t.Errorf("%d chars should pass, got %v", len(tc.secret), err)
		}
		if !tc.wantOK && (err == nil || !strings.Contains(err.Error(), "at least 32 characters")) {
			t.Errorf("%d chars should fail with min-length message, got %v", len(tc.secret), err)
		}
	}
}

func TestBooleanCoercionIsCaseSensitive(t *testing.T) {
	for _, key := range []string{"REDIS_TLS", "SENTRY_SEND_PII", "ENABLE_SWAGGER"} {
		cases := []struct {
			in     string
			wantOK bool
			want   bool
		}{
			{"true", true, true},
			{"false", true, false},
			{"TRUE", false, false},
			{"True", false, false},
			{"1", false, false},
			{"yes", false, false},
			{"", false, false},
		}
		for _, tc := range cases {
			env := baseEnv()
			env[key] = tc.in
			c, err := LoadFrom(env)
			if tc.wantOK {
				if err != nil {
					t.Errorf("%s=%q should parse, got %v", key, tc.in, err)
					continue
				}
				got := boolValue(c, key)
				if got != tc.want {
					t.Errorf("%s=%q = %v, want %v", key, tc.in, got, tc.want)
				}
				continue
			}
			if err == nil {
				t.Errorf("%s=%q must be rejected", key, tc.in)
			}
		}

		delete(baseEnv(), key)
		c := mustLoad(t, baseEnv())
		if boolValue(c, key) {
			t.Errorf("unset %s must default false", key)
		}
	}
}

func boolValue(c *Config, key string) bool {
	switch key {
	case "REDIS_TLS":
		return c.RedisTLS
	case "SENTRY_SEND_PII":
		return c.SentrySendPII
	case "ENABLE_SWAGGER":
		return c.EnableSwagger
	}
	return false
}

func TestNumericCoercionAndConstraints(t *testing.T) {
	cases := []struct {
		key    string
		val    string
		wantOK bool
	}{
		{"PORT", "abc", false},
		{"PORT", "-1", true},
		{"PORT", "3000.5", false},
		{"DATABASE_POOL_MAX", "10", true},
		{"DATABASE_POOL_MAX", "0", false},
		{"DATABASE_POOL_MAX", "-3", false},
		{"DATABASE_POOL_MAX", "10.5", false},
		{"DATABASE_POOL_IDLE_TIMEOUT_MS", "-1", false},
		{"DATABASE_POOL_CONNECTION_TIMEOUT_MS", "0", true},
		{"SMTP_PORT", "-465", true},
		{"REENGAGEMENT_THRESHOLD_MULTIPLIER", "0", false},
		{"REENGAGEMENT_THRESHOLD_MULTIPLIER", "2.75", true},
		{"REENGAGEMENT_COOLDOWN_DAYS", "-1", false},
		{"REENGAGEMENT_COOLDOWN_DAYS", "0", true},
	}
	for _, tc := range cases {
		env := baseEnv()
		env[tc.key] = tc.val
		_, err := LoadFrom(env)
		if tc.wantOK && err != nil {
			t.Errorf("%s=%q should be accepted, got %v", tc.key, tc.val, err)
		}
		if !tc.wantOK && err == nil {
			t.Errorf("%s=%q should be rejected", tc.key, tc.val)
		}
	}
}

func TestSampleRatesBounded(t *testing.T) {
	for _, key := range []string{"SENTRY_TRACES_SAMPLE_RATE", "SENTRY_PROFILES_SAMPLE_RATE"} {
		for _, val := range []string{"-0.01", "1.01", "nan", "abc"} {
			env := baseEnv()
			env[key] = val
			if _, err := LoadFrom(env); err == nil {
				t.Errorf("%s=%q should be rejected", key, val)
			}
		}
		for _, val := range []string{"0", "1", "0.25"} {
			env := baseEnv()
			env[key] = val
			if _, err := LoadFrom(env); err != nil {
				t.Errorf("%s=%q should be accepted, got %v", key, val, err)
			}
		}
	}
}

func TestEnumValidation(t *testing.T) {
	env := baseEnv()
	env["NODE_ENV"] = "staging"
	_, err := LoadFrom(env)
	if err == nil || !strings.Contains(err.Error(), "NODE_ENV") {
		t.Error("bad NODE_ENV must fail")
	}

	env = baseEnv()
	env["LOG_LEVEL"] = "verbose"
	_, err = LoadFrom(env)
	if err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") {
		t.Error("bad LOG_LEVEL must fail")
	}

	for _, level := range []string{"fatal", "error", "warn", "info", "debug", "trace", "silent"} {
		env = baseEnv()
		env["LOG_LEVEL"] = level
		if _, err := LoadFrom(env); err != nil {
			t.Errorf("LOG_LEVEL=%q should pass: %v", level, err)
		}
	}
}

func TestURLValidation(t *testing.T) {
	env := baseEnv()
	env["DATABASE_URL"] = "not-a-url"
	_, err := LoadFrom(env)
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Error("malformed DATABASE_URL must fail")
	}

	env = baseEnv()
	env["PAYSTACK_BASE_URL"] = "api.paystack.co"
	_, err = LoadFrom(env)
	if err == nil || !strings.Contains(err.Error(), "PAYSTACK_BASE_URL") {
		t.Error("PAYSTACK_BASE_URL without scheme must fail")
	}

	env = baseEnv()
	env["PAYSTACK_BASE_URL"] = "https://staging.paystack.example"
	if _, err := LoadFrom(env); err != nil {
		t.Errorf("valid override should pass: %v", err)
	}
}

func TestOptionalStringsPassThrough(t *testing.T) {
	env := baseEnv()
	env["WHATSAPP_APP_SECRET"] = "secret-value"
	env["REDIS_PASSWORD"] = ""
	c := mustLoad(t, env)

	if c.WhatsAppAppSecret != "secret-value" {
		t.Errorf("optional passthrough failed: %q", c.WhatsAppAppSecret)
	}
	if c.RedisPassword != "" {
		t.Errorf("empty optional should stay empty")
	}
}

func TestAllErrorsReportedTogether(t *testing.T) {
	env := map[string]string{
		"JWT_SECRET":        "short",
		"PORT":              "abc",
		"NODE_ENV":          "staging",
		"DATABASE_POOL_MAX": "0",
	}
	_, err := LoadFrom(env)
	if err == nil {
		t.Fatal("expected aggregated errors")
	}
	for _, fragment := range []string{"JWT_SECRET", "PORT", "NODE_ENV", "DATABASE_POOL_MAX", "DATABASE_URL"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("aggregated error should mention %s, got:\n%v", fragment, err)
		}
	}
}
