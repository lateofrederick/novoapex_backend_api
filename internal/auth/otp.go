package auth

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// OTP service port of apps/mobile-api/src/auth/otp.service.ts.
//
// Redis contracts (otp.service.ts:14-20):
//   - code key     "otp:<phone>"
//   - attempts key "otp:attempts:<phone>"
//   - TTL_SECONDS 300 (line 8), MAX_ATTEMPTS 5 (line 10).
//
// generateOtp (lines 22-36) draws a cryptographically secure code from
// randomInt(100000, 1000000) — i.e. [100000, 999999] inclusive, six decimal
// digits — then resets state atomically via ioredis MULTI:
//
//	multi().set(codeKey, code, 'EX', 300).del(attemptsKey).exec()
//
// verifyOtp (lines 38-68) is deliberately NOT transactional in Node:
//   - GET the code; a missing code short-circuits false WITHOUT touching the
//     attempt counter (pinned by otp.service.spec.ts "returns false when no
//     code is stored" asserting incr was never called);
//   - INCR the counter, and on the first attempt align its TTL with the
//     code's remaining lifetime via EXPIRE attemptsKey 300 (lines 50-53);
//   - attempts > 5 burns both keys and returns false even for a correct code
//     (lines 55-59) — the burn happens BEFORE the comparison;
//   - a matching code burns both keys and returns true (lines 61-65);
//   - anything else returns false leaving both keys intact.

const (
	// OTPTTL mirrors TTL_SECONDS = 300 (otp.service.ts:8) — five minutes.
	OTPTTL = 300 * time.Second

	// MaxAttempts mirrors MAX_ATTEMPTS = 5 (otp.service.ts:10). The counter
	// is burned when INCR exceeds this value, so the sixth verification
	// attempt kills the code.
	MaxAttempts = 5

	// Key formats (otp.service.ts:14-20).
	CodeKeyFmt     = "otp:%s"
	AttemptsKeyFmt = "otp:attempts:%s"

	otpCodeMin = 100000  // inclusive
	otpCodeMax = 1000000 // exclusive, mirroring randomInt(100000, 1000000)
)

// Redis is the minimal command surface the OTP service needs. Keeping it an
// interface lets tests substitute an in-memory fake while production wires
// NewGoRedisAdapter around *redis.Client.
type Redis interface {
	// Get returns the value and whether the key existed.
	Get(ctx context.Context, key string) (string, bool, error)
	// Incr applies INCR and returns the new value.
	Incr(ctx context.Context, key string) (int64, error)
	// Expire sets a TTL on an existing key.
	Expire(ctx context.Context, key string, ttl time.Duration) error
	// Del removes keys (any number).
	Del(ctx context.Context, keys ...string) error
	// MultiSetWithDelete mirrors otp.service.ts:27-31 — one atomic
	// MULTI/EXEC applying SET <setKey> <setValue> EX <ttlSeconds> plus
	// DEL <delKeys...>. Implementations must not apply the DEL when the
	// SET fails or vice versa.
	MultiSetWithDelete(ctx context.Context, setKey, setValue string, ttl time.Duration, delKeys ...string) error
}

// GoRedisAdapter wires a *redis.Client into the Redis mini interface.
type GoRedisAdapter struct {
	client *redis.Client
}

// NewGoRedisAdapter wraps a go-redis client.
func NewGoRedisAdapter(client *redis.Client) *GoRedisAdapter {
	return &GoRedisAdapter{client: client}
}

func (a *GoRedisAdapter) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := a.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("auth: redis GET %s: %w", key, err)
	}
	return v, true, nil
}

func (a *GoRedisAdapter) Incr(ctx context.Context, key string) (int64, error) {
	n, err := a.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("auth: redis INCR %s: %w", key, err)
	}
	return n, nil
}

func (a *GoRedisAdapter) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if err := a.client.Expire(ctx, key, ttl).Err(); err != nil {
		return fmt.Errorf("auth: redis EXPIRE %s: %w", key, err)
	}
	return nil
}

func (a *GoRedisAdapter) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := a.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("auth: redis DEL %s: %w", strings.Join(keys, ","), err)
	}
	return nil
}

// MultiSetWithDelete reproduces the Lua-free MULTI semantics of Node's
// client.multi().set(...).del(...).exec() via go-redis's TxPipeline: both
// commands queue server-side and commit atomically on Exec.
func (a *GoRedisAdapter) MultiSetWithDelete(ctx context.Context, setKey, setValue string, ttl time.Duration, delKeys ...string) error {
	pipe := a.client.TxPipeline()
	pipe.Set(ctx, setKey, setValue, ttl)
	if len(delKeys) > 0 {
		pipe.Del(ctx, delKeys...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("auth: redis MULTI set+del: %w", err)
	}
	return nil
}

// OTPCodeKey builds "otp:<phone>" (otp.service.ts:14-16).
func OTPCodeKey(phone string) string { return fmt.Sprintf(CodeKeyFmt, phone) }

// OTPAttemptsKey builds "otp:attempts:<phone>" (otp.service.ts:18-20).
func OTPAttemptsKey(phone string) string { return fmt.Sprintf(AttemptsKeyFmt, phone) }

// RandomCode mirrors crypto.randomInt(100000, 1000000): a uniformly random
// integer in [100000, 999999], rendered as six decimal digits. Rejection-free
// because 900000 divides no bias into big.Int's uniform [0, 2^k) sampling.
func RandomCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(otpCodeMax-otpCodeMin))
	if err != nil {
		return "", fmt.Errorf("auth: otp randomness: %w", err)
	}
	return fmt.Sprintf("%06d", int(n.Int64())+otpCodeMin), nil
}

// Generate ports OtpService.generateOtp (otp.service.ts:22-36): draw a fresh
// code, then atomically store it with OTPTTL and clear any prior attempt
// counter so the new code starts fresh. Generation happens BEFORE storage,
// and callers deliver AFTER Generate returns (auth.service.ts:24-45).
func Generate(ctx context.Context, r Redis, phone string) (string, error) {
	code, err := RandomCode()
	if err != nil {
		return "", err
	}
	if err := Store(ctx, r, phone, code); err != nil {
		return "", err
	}
	return code, nil
}

// Store writes the code and resets the attempt counter in one atomic
// transaction (otp.service.ts:27-31).
func Store(ctx context.Context, r Redis, phone, code string) error {
	return r.MultiSetWithDelete(ctx, OTPCodeKey(phone), code, OTPTTL, OTPAttemptsKey(phone))
}

// Verify ports OtpService.verifyOtp (otp.service.ts:38-68). See the package
// comment for the exact ordering contract, including burn-before-compare at
// the attempt limit.
func Verify(ctx context.Context, r Redis, phone, code string) (bool, error) {
	codeKey := OTPCodeKey(phone)
	stored, ok, err := r.Get(ctx, codeKey)
	if err != nil {
		return false, err
	}
	if !ok || stored == "" {
		// Node: `if (!storedCode) return false` — counter untouched.
		return false, nil
	}

	attemptsKey := OTPAttemptsKey(phone)
	attempts, err := r.Incr(ctx, attemptsKey)
	if err != nil {
		return false, err
	}
	if attempts == 1 {
		// First attempt — align the counter's TTL with the code lifetime
		// (otp.service.ts:50-53).
		if err := r.Expire(ctx, attemptsKey, OTPTTL); err != nil {
			return false, err
		}
	}

	if attempts > MaxAttempts {
		if err := burnOTP(ctx, r, phone); err != nil {
			return false, err
		}
		// Node logs logger.warn here (otp.service.ts:57).
		return false, nil
	}

	if stored == code {
		// Burn the OTP after successful use (otp.service.ts:61-65).
		if err := burnOTP(ctx, r, phone); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// burnOTP deletes both keys (otp.service.ts:70-72).
func burnOTP(ctx context.Context, r Redis, phone string) error {
	return r.Del(ctx, OTPCodeKey(phone), OTPAttemptsKey(phone))
}
