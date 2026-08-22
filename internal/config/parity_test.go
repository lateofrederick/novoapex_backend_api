package config

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// T0.26 — table-driven CONFIG PARITY between this Go port and the Node zod
// schema (libs/common/src/config/config.schema.ts) across unset / valid /
// invalid inputs. The same single-var scenario matrix runs through Go
// LoadFrom and through a TS runner invoking validateConfig; acceptance,
// rejection and effective values must agree unless the case is listed in
// cfgpExpectedDivergences.

type cfgpKind int

const (
	cfgpKindString cfgpKind = iota // plain z.string(): every input valid, empty-string edge probed instead
	cfgpKindInt
	cfgpKindFloat
	cfgpKindBool
	cfgpKindEnum
	cfgpKindURL
)

type cfgpVar struct {
	Key     string
	Kind    cfgpKind
	Valid   string // canonical accepted value
	Invalid string // canonical rejected value; "" means the schema accepts every string
	Get     func(*Config) any
}

func cfgpBaseEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL": "postgresql://u:p@localhost/db",
		"JWT_SECRET":   "0123456789abcdef0123456789abcdef",
	}
}

func cfgpVarTable() []cfgpVar {
	return []cfgpVar{
		{Key: "NODE_ENV", Kind: cfgpKindEnum, Valid: "production", Invalid: "staging",
			Get: func(c *Config) any { return c.NodeEnv }},
		{Key: "PORT", Kind: cfgpKindInt, Valid: "8080", Invalid: "3000.5", // fractional → documented divergence
			Get: func(c *Config) any { return c.Port }},
		{Key: "MOBILE_API_PORT", Kind: cfgpKindInt, Valid: "4001", Invalid: "3001.5", // fractional → documented divergence
			Get: func(c *Config) any { return c.MobileAPIPort }},
		{Key: "DATABASE_URL", Kind: cfgpKindURL, Valid: "postgresql://u:p@localhost/db", Invalid: "not-a-url",
			Get: func(c *Config) any { return c.DatabaseURL }},
		{Key: "DATABASE_POOL_MAX", Kind: cfgpKindInt, Valid: "20", Invalid: "10.5",
			Get: func(c *Config) any { return c.DatabasePoolMax }},
		{Key: "DATABASE_POOL_IDLE_TIMEOUT_MS", Kind: cfgpKindInt, Valid: "60000", Invalid: "-1",
			Get: func(c *Config) any { return c.DatabasePoolIdleTimeoutMS }},
		{Key: "DATABASE_POOL_CONNECTION_TIMEOUT_MS", Kind: cfgpKindInt, Valid: "5000", Invalid: "-1",
			Get: func(c *Config) any { return c.DatabasePoolConnectionTimeoutMS }},
		{Key: "REDIS_HOST", Kind: cfgpKindString, Valid: "redis.internal.example",
			Get: func(c *Config) any { return c.RedisHost }},
		{Key: "REDIS_PORT", Kind: cfgpKindInt, Valid: "6380", Invalid: "6379.5", // fractional → documented divergence
			Get: func(c *Config) any { return c.RedisPort }},
		{Key: "REDIS_PASSWORD", Kind: cfgpKindString, Valid: "s3cret",
			Get: func(c *Config) any { return c.RedisPassword }},
		{Key: "REDIS_TLS", Kind: cfgpKindBool, Valid: "true", Invalid: "yes",
			Get: func(c *Config) any { return c.RedisTLS }},
		{Key: "SENTRY_DSN", Kind: cfgpKindString, Valid: "https://key@sentry.example/2",
			Get: func(c *Config) any { return c.SentryDSN }},
		{Key: "SENTRY_ENVIRONMENT", Kind: cfgpKindString, Valid: "production",
			Get: func(c *Config) any { return c.SentryEnvironment }},
		{Key: "SENTRY_RELEASE", Kind: cfgpKindString, Valid: "api@1.2.3",
			Get: func(c *Config) any { return c.SentryRelease }},
		{Key: "SENTRY_TRACES_SAMPLE_RATE", Kind: cfgpKindFloat, Valid: "0.25", Invalid: "1.01",
			Get: func(c *Config) any { return c.SentryTracesSampleRate }},
		{Key: "SENTRY_PROFILES_SAMPLE_RATE", Kind: cfgpKindFloat, Valid: "0.5", Invalid: "-0.5",
			Get: func(c *Config) any { return c.SentryProfilesSampleRate }},
		{Key: "SENTRY_SEND_PII", Kind: cfgpKindBool, Valid: "true", Invalid: "1",
			Get: func(c *Config) any { return c.SentrySendPII }},
		{Key: "LOG_LEVEL", Kind: cfgpKindEnum, Valid: "debug", Invalid: "verbose",
			Get: func(c *Config) any { return c.LogLevel }},
		{Key: "WHATSAPP_VERIFY_TOKEN", Kind: cfgpKindString, Valid: "verify-token",
			Get: func(c *Config) any { return c.WhatsAppVerifyToken }},
		{Key: "WHATSAPP_API_VERSION", Kind: cfgpKindString, Valid: "v26.0",
			Get: func(c *Config) any { return c.WhatsAppAPIVersion }},
		{Key: "WHATSAPP_PHONE_NUMBER_ID", Kind: cfgpKindString, Valid: "1234567890",
			Get: func(c *Config) any { return c.WhatsAppPhoneNumberID }},
		{Key: "WHATSAPP_ACCESS_TOKEN", Kind: cfgpKindString, Valid: "eaag-token",
			Get: func(c *Config) any { return c.WhatsAppAccessToken }},
		{Key: "WHATSAPP_APP_SECRET", Kind: cfgpKindString, Valid: "meta-secret",
			Get: func(c *Config) any { return c.WhatsAppAppSecret }},
		{Key: "GOOGLE_GENERATIVE_AI_API_KEY", Kind: cfgpKindString, Valid: "gai-key",
			Get: func(c *Config) any { return c.GoogleGenerativeAIAPIKey }},
		{Key: "OPENAI_API_KEY", Kind: cfgpKindString, Valid: "sk-openai-test",
			Get: func(c *Config) any { return c.OpenAIAPIKey }},
		{Key: "PAYSTACK_SECRET_KEY", Kind: cfgpKindString, Valid: "sk_test_paystack",
			Get: func(c *Config) any { return c.PaystackSecretKey }},
		{Key: "PAYSTACK_PUBLIC_KEY", Kind: cfgpKindString, Valid: "pk_test_paystack",
			Get: func(c *Config) any { return c.PaystackPublicKey }},
		{Key: "PAYSTACK_BASE_URL", Kind: cfgpKindURL, Valid: "https://staging.paystack.example", Invalid: "api.paystack.co",
			Get: func(c *Config) any { return c.PaystackBaseURL }},
		{Key: "SMTP_HOST", Kind: cfgpKindString, Valid: "smtp.example.com",
			Get: func(c *Config) any { return c.SMTPHost }},
		{Key: "SMTP_PORT", Kind: cfgpKindInt, Valid: "587", Invalid: "465.5",
			Get: func(c *Config) any { return c.SMTPPort }},
		{Key: "SMTP_USER", Kind: cfgpKindString, Valid: "smtp-user",
			Get: func(c *Config) any { return c.SMTPUser }},
		{Key: "SMTP_PASS", Kind: cfgpKindString, Valid: "smtp-pass",
			Get: func(c *Config) any { return c.SMTPPass }},
		{Key: "CLOUDINARY_CLOUD_NAME", Kind: cfgpKindString, Valid: "novoapex-cloud",
			Get: func(c *Config) any { return c.CloudinaryCloudName }},
		{Key: "CLOUDINARY_API_KEY", Kind: cfgpKindString, Valid: "cd-key",
			Get: func(c *Config) any { return c.CloudinaryAPIKey }},
		{Key: "CLOUDINARY_API_SECRET", Kind: cfgpKindString, Valid: "cd-secret",
			Get: func(c *Config) any { return c.CloudinaryAPISecret }},
		{Key: "JWT_SECRET", Kind: cfgpKindString, Valid: strings.Repeat("a", 64), Invalid: strings.Repeat("a", 31),
			Get: func(c *Config) any { return c.JWTSecret }},
		{Key: "BULL_BOARD_USER", Kind: cfgpKindString, Valid: "bull-admin",
			Get: func(c *Config) any { return c.BullBoardUser }},
		{Key: "BULL_BOARD_PASSWORD", Kind: cfgpKindString, Valid: "bull-pass",
			Get: func(c *Config) any { return c.BullBoardPassword }},
		{Key: "ENABLE_SWAGGER", Kind: cfgpKindBool, Valid: "true", Invalid: "yes",
			Get: func(c *Config) any { return c.EnableSwagger }},
		{Key: "REENGAGEMENT_THRESHOLD_MULTIPLIER", Kind: cfgpKindFloat, Valid: "2.75", Invalid: "-1",
			Get: func(c *Config) any { return c.ReengagementThresholdMultiplier }},
		{Key: "REENGAGEMENT_COOLDOWN_DAYS", Kind: cfgpKindInt, Valid: "45", Invalid: "-1",
			Get: func(c *Config) any { return c.ReengagementCooldownDays }},
	}
}

// cfgpExpectedDivergences documents known-deliberate Go-vs-zod differences so
// they stay green while remaining visible. Key is "<KEY>#<scenario>". Every
// entry asserts the direction: Go rejects while zod accepts.
var cfgpExpectedDivergences = map[string]string{
	"PORT#invalid":            "fractional unconstrained number: Go requires integral ints, z.coerce.number() has no .int() and coerces '3000.5' to 3000.5 (Go rejects, Node accepts)",
	"MOBILE_API_PORT#invalid": "fractional unconstrained number: Go requires integral ints, z.coerce.number() has no .int() and coerces '3001.5' to 3001.5 (Go rejects, Node accepts)",
	"REDIS_PORT#invalid":      "fractional unconstrained number: Go requires integral ints, z.coerce.number() has no .int() and coerces '6379.5' to 6379.5 (Go rejects, Node accepts)",
}

type cfgpCase struct {
	ID  string // "<KEY>#<scenario>"
	Var cfgpVar
	Scn cfgpScenario
}

// cfgpBuildMatrix emits, per schema var: unset (defaults/required parity),
// one canonical valid value, and either one canonical invalid value or — for
// plain z.string() vars that accept every string — an empty-string edge probe.
func cfgpBuildMatrix(vars []cfgpVar) ([]cfgpCase, []cfgpScenario) {
	cases := make([]cfgpCase, 0, len(vars)*3)
	scenarios := make([]cfgpScenario, 0, len(vars)*3)
	add := func(v cfgpVar, scn string, input *string) {
		s := cfgpScenario{Key: v.Key, Scenario: scn, Input: input}
		cases = append(cases, cfgpCase{ID: v.Key + "#" + scn, Var: v, Scn: s})
		scenarios = append(scenarios, s)
	}
	for _, v := range vars {
		add(v, "unset", nil)
		valid := v.Valid
		add(v, "valid", &valid)
		switch {
		case v.Invalid != "":
			invalid := v.Invalid
			add(v, "invalid", &invalid)
		default:
			edge := ""
			add(v, "edge-empty-string", &edge)
		}
	}
	return cases, scenarios
}

func TestT026_ConfigParityCrossStack(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		if _, npxErr := exec.LookPath("npx"); npxErr != nil {
			t.Skipf("node toolchain not found (%v / %v)", err, npxErr)
		}
	}
	repoDir, err := cfgpRepoDir()
	if err != nil {
		t.Skipf("%v", err)
	}

	vars := cfgpVarTable()
	cases, scenarios := cfgpBuildMatrix(vars)
	base := cfgpBaseEnv()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	results, err := cfgpRunNodeMatrix(ctx, t.TempDir(), repoDir, base, scenarios)
	if err != nil {
		if errors.Is(err, errCfgpNoRunner) {
			t.Skipf("%v", err)
		}
		t.Fatalf("run node matrix: %v", err)
	}

	byID := make(map[string]cfgpNodeResult, len(results))
	for _, r := range results {
		id := r.Key + "#" + r.Scenario
		if _, dup := byID[id]; dup {
			t.Fatalf("duplicate node result for %s", id)
		}
		byID[id] = r
	}

	documented := map[string]bool{}
	newDivergences := 0
	compared := 0
	for _, tc := range cases {
		res, ok := byID[tc.ID]
		if !ok {
			t.Fatalf("node runner produced no result for %s", tc.ID)
		}
		env := cfgpBaseEnv()
		if tc.Scn.Input != nil {
			env[tc.Var.Key] = *tc.Scn.Input
		} else {
			delete(env, tc.Var.Key)
		}
		got, goErr := LoadFrom(env)
		goOK := goErr == nil

		reason, expectDiv := cfgpExpectedDivergences[tc.ID]
		switch {
		case expectDiv:
			if goOK || !res.OK {
				newDivergences++
				t.Errorf("documented divergence %s changed behaviour: Go accepted=%v, Node accepted=%v — reclassify it\nreason was: %s\ngo error: %v | node error: %s",
					tc.ID, goOK, res.OK, reason, goErr, res.ErrorSummary)
				continue
			}
			documented[tc.ID] = true
		case goOK != res.OK:
			newDivergences++
			t.Errorf("NEW divergence at %s (input=%s): Go accepted=%v (%v), Node accepted=%v (%s)",
				tc.ID, cfgpShowInput(tc.Scn.Input), goOK, goErr, res.OK, res.ErrorSummary)
			continue
		case goOK:
			want := tc.Var.Get(got)
			if !cfgpValuesEqual(res.EffectiveValue, want) {
				newDivergences++
				t.Errorf("effective value mismatch at %s (input=%s): node=%#v (%T) go=%#v (%T)",
					tc.ID, cfgpShowInput(tc.Scn.Input), res.EffectiveValue, res.EffectiveValue, want, want)
				continue
			}
		}
		compared++
	}

	var docList []string
	for id := range documented {
		docList = append(docList, id)
	}
	t.Logf("T0.26 compared %d/%d cases: %d agreements + %d documented divergences %v, %d new divergences",
		compared, len(cases), compared-len(documented), len(documented), docList, newDivergences)

	if len(documented) != len(cfgpExpectedDivergences) {
		t.Errorf("expected-divergence entries not all exercised: encoded=%d hit=%d", len(cfgpExpectedDivergences), len(documented))
	}
}

func cfgpShowInput(in *string) string {
	if in == nil {
		return "<unset>"
	}
	return *in
}

// cfgpValuesEqual compares the zod output against the Go field. Unset
// optional strings are undefined in zod but materialise as "" in Go — that
// presence nuance counts as equal here; everything else compares numerically
// for numbers and strictly for bool/string.
func cfgpValuesEqual(node, gov any) bool {
	if node == nil || gov == nil {
		if node == nil && gov == nil {
			return true
		}
		// unset optional string: zod undefined vs Go ""
		return node == nil && gov == ""
	}
	switch nv := node.(type) {
	case bool:
		bv, ok := gov.(bool)
		return ok && bv == nv
	case float64:
		switch gv := gov.(type) {
		case int:
			return float64(gv) == nv
		case float64:
			return gv == nv
		}
		return false
	case string:
		sv, ok := gov.(string)
		return ok && sv == nv
	default:
		return false
	}
}
