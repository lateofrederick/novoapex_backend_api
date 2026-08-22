package auth_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

// s3FakeRedis is an in-memory auth.Redis fake recording call shapes so tests
// can assert the exact command sequences otp.service.ts performs.
type s3FakeRedis struct {
	mu sync.Mutex

	strings map[string]string
	expires map[string]time.Time

	// call log
	calls []string

	multiSetDel func() error // hook to inject failures
}

func newS3FakeRedis() *s3FakeRedis {
	return &s3FakeRedis{strings: map[string]string{}, expires: map[string]time.Time{}}
}

func (f *s3FakeRedis) log(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf(format, args...))
}

func (f *s3FakeRedis) Get(_ context.Context, key string) (string, bool, error) {
	f.log("GET %s", key)
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.strings[key]
	return v, ok, nil
}

func (f *s3FakeRedis) Incr(_ context.Context, key string) (int64, error) {
	f.log("INCR %s", key)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.strings[key] += "x" // one 'x' per increment
	return int64(len(f.strings[key])), nil
}

func (f *s3FakeRedis) Expire(_ context.Context, key string, ttl time.Duration) error {
	f.log("EXPIRE %s %d", key, int(ttl.Seconds()))
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.strings[key]; !ok {
		return errors.New("no such key")
	}
	f.expires[key] = time.Now().Add(ttl)
	return nil
}

func (f *s3FakeRedis) Del(_ context.Context, keys ...string) error {
	f.log("DEL %v", keys)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.strings, k)
		delete(f.expires, k)
	}
	return nil
}

// MultiSetWithDelete asserts atomicity by applying both halves or neither.
func (f *s3FakeRedis) MultiSetWithDelete(_ context.Context, setKey, setValue string, ttl time.Duration, delKeys ...string) error {
	f.log("MULTI [SET %s %s EX %d; DEL %v]", setKey, setValue, int(ttl.Seconds()), delKeys)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.multiSetDel != nil {
		if err := f.multiSetDel(); err != nil {
			return err
		}
	}
	f.strings[setKey] = setValue
	f.expires[setKey] = time.Now().Add(ttl)
	for _, k := range delKeys {
		delete(f.strings, k)
		delete(f.expires, k)
	}
	return nil
}

func (f *s3FakeRedis) attempts(phone string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.strings[auth.OTPAttemptsKey(phone)])
}

func (f *s3FakeRedis) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.strings[key]
	return ok
}

func TestS3_Generate_MatchesNodeContract(t *testing.T) {
	fake := newS3FakeRedis()

	code, err := auth.Generate(t.Context(), fake, "+233200000001")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(code) != 6 {
		t.Fatalf("code %q must be 6 digits", code)
	}
	n, err := strconv.Atoi(code)
	if err != nil || n < 100000 || n > 999999 {
		t.Fatalf("code %q outside randomInt(100000,1000000) range", code)
	}

	// MULTI shape: SET otp:<phone> <code> EX 300 + DEL otp:attempts:<phone>
	want := fmt.Sprintf(
		"MULTI [SET %s %s EX 300; DEL [%s]]",
		auth.OTPCodeKey("+233200000001"), code, auth.OTPAttemptsKey("+233200000001"))
	if len(fake.calls) != 1 || fake.calls[0] != want {
		t.Fatalf("redis calls = %v, want exactly [%s]", fake.calls, want)
	}

	// Range/uniqueness sanity over a sample (mirrors the node spec's spread check).
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		c, err := auth.RandomCode()
		if err != nil {
			t.Fatalf("RandomCode: %v", err)
		}
		v, _ := strconv.Atoi(c)
		if v < 100000 || v > 999999 {
			t.Fatalf("RandomCode %q out of range", c)
		}
		seen[c] = true
	}
	if len(seen) < 100 {
		t.Fatalf("RandomCode suspiciously repetitive: %d distinct in 200 draws", len(seen))
	}
}

func TestS3_Verify_HappyPathBurnsBothKeys(t *testing.T) {
	fake := newS3FakeRedis()
	ctx := t.Context()

	code, err := auth.Generate(ctx, fake, "+233200000002")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	ok, err := auth.Verify(ctx, fake, "+233200000002", code)
	if err != nil || !ok {
		t.Fatalf("Verify(valid)=(%v,%v), want true,nil", ok, err)
	}
	if fake.has(auth.OTPCodeKey("+233200000002")) ||
		fake.has(auth.OTPAttemptsKey("+233200000002")) {
		t.Fatal("successful verification must burn both keys")
	}
}

func TestS3_Verify_NoStoredCodeSkipsCounter(t *testing.T) {
	fake := newS3FakeRedis()

	ok, err := auth.Verify(t.Context(), fake, "+233200000003", "123456")
	if err != nil || ok {
		t.Fatalf("Verify(missing)=(%v,%v), want false,nil", ok, err)
	}
	for _, c := range fake.calls {
		if c[:4] == "INCR" {
			t.Fatalf("attempt counter must not be touched when no code stored: %v", fake.calls)
		}
	}
	if fake.attempts("+233200000003") != 0 {
		t.Fatal("attempts recorded despite missing code")
	}
}

func TestS3_Verify_WrongCodeCountsAttemptsUntilLimit(t *testing.T) {
	fake := newS3FakeRedis()
	ctx := t.Context()
	const phone = "+233200000004"

	code, err := auth.Generate(ctx, fake, phone)
	if err != nil {
		t.Fatal(err)
	}
	wrong := code
	if wrong == "000000" {
		wrong = "111111"
	} else {
		wrong = "000000"
	}

	// Attempts 1..5 fail without burning.
	for i := 1; i <= auth.MaxAttempts; i++ {
		ok, err := auth.Verify(ctx, fake, phone, wrong)
		if err != nil {
			t.Fatalf("Verify attempt %d: %v", i, err)
		}
		if ok {
			t.Fatalf("wrong code accepted on attempt %d", i)
		}
		if !fake.has(auth.OTPCodeKey(phone)) {
			t.Fatalf("code burned early at attempt %d", i)
		}
	}

	// Attempt 6 exceeds MAX_ATTEMPTS: burn happens BEFORE comparison, so even
	// the CORRECT code is rejected.
	ok, err := auth.Verify(ctx, fake, phone, code)
	if err != nil || ok {
		t.Fatalf("Verify(correct on 6th attempt)=(%v,%v), want false,nil", ok, err)
	}
	if fake.has(auth.OTPCodeKey(phone)) || fake.has(auth.OTPAttemptsKey(phone)) {
		t.Fatal("burn after exceeding MAX_ATTEMPTS must delete both keys")
	}
}

func TestS3_Verify_TTLAlignmentOnFirstAttemptOnly(t *testing.T) {
	fake := newS3FakeRedis()
	ctx := t.Context()
	const phone = "+233200000005"

	if _, err := auth.Generate(ctx, fake, phone); err != nil {
		t.Fatal(err)
	}

	expireCalls := 0
	for i := 0; i < 3; i++ {
		if _, err := auth.Verify(ctx, fake, phone, "000000"); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range fake.calls {
		if len(c) >= 6 && c[:6] == "EXPIRE" {
			expireCalls++
			if c != "EXPIRE "+auth.OTPAttemptsKey(phone)+" 300" {
				t.Fatalf("EXPIRE payload mismatch: %q", c)
			}
		}
	}
	if expireCalls != 1 {
		t.Fatalf("EXPIRE called %d times, want exactly once (first attempt)", expireCalls)
	}
}

func TestS3_Store_FailureIsAtomicAndPropagates(t *testing.T) {
	fake := newS3FakeRedis()
	boom := errors.New("redis down")
	fake.multiSetDel = func() error { return boom }

	if _, err := auth.Generate(t.Context(), fake, "+233200000006"); !errors.Is(err, boom) {
		t.Fatalf("Generate error = %v, want wrapped boom", err)
	}
	if fake.has(auth.OTPCodeKey("+233200000006")) {
		t.Fatal("SET half applied despite MULTI failure")
	}
}
