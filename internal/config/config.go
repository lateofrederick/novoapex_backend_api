package config

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const jwtSecretMinLength = 32

type Config struct {
	NodeEnv                         string
	Port                            int
	MobileAPIPort                   int
	DatabaseURL                     string
	DatabasePoolMax                 int
	DatabasePoolIdleTimeoutMS       int
	DatabasePoolConnectionTimeoutMS int

	RedisHost     string
	RedisPort     int
	RedisPassword string
	RedisTLS      bool

	SentryDSN                string
	SentryEnvironment        string
	SentryRelease            string
	SentryTracesSampleRate   float64
	SentryProfilesSampleRate float64
	SentrySendPII            bool

	LogLevel string

	WhatsAppVerifyToken   string
	WhatsAppAPIVersion    string
	WhatsAppPhoneNumberID string
	WhatsAppAccessToken   string
	WhatsAppAppSecret     string

	GoogleGenerativeAIAPIKey string
	OpenAIAPIKey             string

	PaystackSecretKey string
	PaystackPublicKey string
	PaystackBaseURL   string

	SMTPHost string
	SMTPPort int
	SMTPUser string
	SMTPPass string

	CloudinaryCloudName string
	CloudinaryAPIKey    string
	CloudinaryAPISecret string

	JWTSecret string

	BullBoardUser     string
	BullBoardPassword string

	EnableSwagger bool

	ReengagementThresholdMultiplier float64
	ReengagementCooldownDays        int
}

type envFunc func(string) (string, bool)

var (
	nodeEnvs   = []string{"development", "production", "test"}
	logLevels  = []string{"fatal", "error", "warn", "info", "debug", "trace", "silent"}
	boolValues = []string{"true", "false"}
)

func Load() (*Config, error) {
	return load(os.LookupEnv)
}

func LoadFrom(env map[string]string) (*Config, error) {
	return load(func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	})
}

func load(lookup envFunc) (*Config, error) {
	var errs []string
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	c := &Config{}

	c.NodeEnv = stringWithDefault(lookup, "NODE_ENV", "development", fail)
	if !oneOf("NODE_ENV", c.NodeEnv, nodeEnvs) {
		fail("NODE_ENV must be one of [%s], got %q", strings.Join(nodeEnvs, ", "), c.NodeEnv)
	}

	c.Port = intFrom(lookup, "PORT", 3000, nil, fail)
	c.MobileAPIPort = intFrom(lookup, "MOBILE_API_PORT", 3001, nil, fail)

	c.DatabaseURL = urlVar(lookup, "DATABASE_URL", "", true, fail)

	c.DatabasePoolMax = intFrom(lookup, "DATABASE_POOL_MAX", 10,
		func(v float64) string {
			if v != math.Trunc(v) || v <= 0 {
				return "must be a positive integer"
			}
			return ""
		}, fail)
	c.DatabasePoolIdleTimeoutMS = intFrom(lookup, "DATABASE_POOL_IDLE_TIMEOUT_MS", 30_000,
		func(v float64) string {
			if v < 0 || v != math.Trunc(v) {
				return "must be a nonnegative integer"
			}
			return ""
		}, fail)
	c.DatabasePoolConnectionTimeoutMS = intFrom(lookup, "DATABASE_POOL_CONNECTION_TIMEOUT_MS", 10_000,
		func(v float64) string {
			if v < 0 || v != math.Trunc(v) {
				return "must be a nonnegative integer"
			}
			return ""
		}, fail)

	c.RedisHost = stringWithDefault(lookup, "REDIS_HOST", "localhost", fail)
	c.RedisPort = intFrom(lookup, "REDIS_PORT", 6379, nil, fail)
	c.RedisPassword = optionalString(lookup, "REDIS_PASSWORD")
	c.RedisTLS = boolVar(lookup, "REDIS_TLS", false, fail)

	c.SentryDSN = optionalString(lookup, "SENTRY_DSN")
	c.SentryEnvironment = optionalString(lookup, "SENTRY_ENVIRONMENT")
	c.SentryRelease = optionalString(lookup, "SENTRY_RELEASE")
	c.SentryTracesSampleRate = sampleRateVar(lookup, "SENTRY_TRACES_SAMPLE_RATE", 0.1, fail)
	c.SentryProfilesSampleRate = sampleRateVar(lookup, "SENTRY_PROFILES_SAMPLE_RATE", 1.0, fail)
	c.SentrySendPII = boolVar(lookup, "SENTRY_SEND_PII", false, fail)

	c.LogLevel = stringWithDefault(lookup, "LOG_LEVEL", "info", fail)
	if !oneOf("LOG_LEVEL", c.LogLevel, logLevels) {
		fail("LOG_LEVEL must be one of [%s], got %q", strings.Join(logLevels, ", "), c.LogLevel)
	}

	c.WhatsAppVerifyToken = optionalString(lookup, "WHATSAPP_VERIFY_TOKEN")
	c.WhatsAppAPIVersion = stringWithDefault(lookup, "WHATSAPP_API_VERSION", "v25.0", fail)
	c.WhatsAppPhoneNumberID = optionalString(lookup, "WHATSAPP_PHONE_NUMBER_ID")
	c.WhatsAppAccessToken = optionalString(lookup, "WHATSAPP_ACCESS_TOKEN")
	c.WhatsAppAppSecret = optionalString(lookup, "WHATSAPP_APP_SECRET")

	c.GoogleGenerativeAIAPIKey = optionalString(lookup, "GOOGLE_GENERATIVE_AI_API_KEY")
	c.OpenAIAPIKey = optionalString(lookup, "OPENAI_API_KEY")

	c.PaystackSecretKey = optionalString(lookup, "PAYSTACK_SECRET_KEY")
	c.PaystackPublicKey = optionalString(lookup, "PAYSTACK_PUBLIC_KEY")
	c.PaystackBaseURL = urlVar(lookup, "PAYSTACK_BASE_URL", "https://api.paystack.co", false, fail)

	c.SMTPHost = stringWithDefault(lookup, "SMTP_HOST", "smtp.zoho.com", fail)
	c.SMTPPort = intFrom(lookup, "SMTP_PORT", 465,
		func(v float64) string {
			if v != math.Trunc(v) {
				return "must be an integer"
			}
			return ""
		}, fail)
	c.SMTPUser = optionalString(lookup, "SMTP_USER")
	c.SMTPPass = optionalString(lookup, "SMTP_PASS")

	c.CloudinaryCloudName = optionalString(lookup, "CLOUDINARY_CLOUD_NAME")
	c.CloudinaryAPIKey = optionalString(lookup, "CLOUDINARY_API_KEY")
	c.CloudinaryAPISecret = optionalString(lookup, "CLOUDINARY_API_SECRET")

	jwtSecret, jwtSet := lookup("JWT_SECRET")
	if !jwtSet {
		fail("JWT_SECRET is required")
	} else if c.JWTSecret = jwtSecret; len(jwtSecret) < jwtSecretMinLength {
		fail("JWT_SECRET must be at least %d characters — set a strong random value", jwtSecretMinLength)
	}

	c.BullBoardUser = optionalString(lookup, "BULL_BOARD_USER")
	c.BullBoardPassword = optionalString(lookup, "BULL_BOARD_PASSWORD")

	c.EnableSwagger = boolVar(lookup, "ENABLE_SWAGGER", false, fail)

	c.ReengagementThresholdMultiplier = numberVar(lookup, "REENGAGEMENT_THRESHOLD_MULTIPLIER", 1.5, false,
		func(v float64) string {
			if v <= 0 {
				return "must be positive"
			}
			return ""
		}, fail)
	c.ReengagementCooldownDays = intFrom(lookup, "REENGAGEMENT_COOLDOWN_DAYS", 30,
		func(v float64) string {
			if v < 0 || v != math.Trunc(v) {
				return "must be a nonnegative integer"
			}
			return ""
		}, fail)

	if len(errs) > 0 {
		return nil, errors.New("invalid configuration:\n  - " + strings.Join(errs, "\n  - "))
	}
	return c, nil
}

func oneOf(name, value string, allowed []string) bool {
	for _, a := range allowed {
		if value == a {
			return true
		}
	}
	return false
}

func optionalString(lookup envFunc, name string) string {
	v, _ := lookup(name)
	return v
}

func stringWithDefault(lookup envFunc, name, def string, fail func(string, ...any)) string {
	v, ok := lookup(name)
	if !ok {
		return def
	}
	return v
}

func boolVar(lookup envFunc, name string, def bool, fail func(string, ...any)) bool {
	v, ok := lookup(name)
	if !ok {
		return def
	}
	switch v {
	case "true":
		return true
	case "false":
		return false
	default:
		fail("%s must be exactly \"true\" or \"false\", got %q", name, v)
		return def
	}
}

func numberVar(lookup envFunc, name string, def float64, integral bool, check func(float64) string, fail func(string, ...any)) float64 {
	v, ok := lookup(name)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		fail("%s must be a number, got %q", name, v)
		return def
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || (integral && f != math.Trunc(f)) {
		fail("%s must be a finite number, got %q", name, v)
		return def
	}
	if check != nil {
		if msg := check(f); msg != "" {
			fail("%s %s, got %q", name, msg, v)
			return def
		}
	}
	return f
}

func sampleRateVar(lookup envFunc, name string, def float64, fail func(string, ...any)) float64 {
	v, ok := lookup(name)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		fail("%s must be a number, got %q", name, v)
		return def
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		fail("%s must be a finite number, got %q", name, v)
		return def
	}
	if f < 0 || f > 1 {
		fail("%s must be between 0 and 1, got %q", name, v)
		return def
	}
	return f
}

func urlVar(lookup envFunc, name, def string, required bool, fail func(string, ...any)) string {
	v, ok := lookup(name)
	if !ok {
		if required {
			fail("%s is required", name)
			return ""
		}
		return def
	}
	u, err := url.Parse(v)
	if err != nil || !u.IsAbs() || u.Host == "" {
		fail("%s must be a valid absolute URL, got %q", name, v)
		return ""
	}
	return v
}

func intFrom(lookup envFunc, name string, def int64, check func(float64) string, fail func(string, ...any)) int {
	return int(numberVar(lookup, name, float64(def), true, check, fail))
}
