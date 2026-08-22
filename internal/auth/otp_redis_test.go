package auth_test

// Integration test against a REAL redis (testcontainers), guarded: skipped
// when docker is unavailable. Mirrors T3.1/T3.2's store semantics on actual
// Redis MULTI/EXPIRE/INCR machinery via the go-redis TxPipeline adapter.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

func s3StartRedis(t *testing.T) *redis.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("docker unavailable or redis failed to start (%v)", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("redis host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("redis port: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", host, port.Port())})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	return client
}

func TestS3_OTPAgainstRealRedis(t *testing.T) {
	rdb := s3StartRedis(t)
	store := auth.NewGoRedisAdapter(rdb)
	ctx := context.Background()
	const phone = "+233200000100"

	code, err := auth.Generate(ctx, store, phone)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(code) != 6 {
		t.Fatalf("code %q not 6 digits", code)
	}

	// Raw redis view: code stored with ~300s TTL; no attempts key.
	got, err := rdb.Get(ctx, auth.OTPCodeKey(phone)).Result()
	if err != nil || got != code {
		t.Fatalf("GET otp key = (%q,%v), want code", got, err)
	}
	ttl, err := rdb.TTL(ctx, auth.OTPCodeKey(phone)).Result()
	if err != nil || ttl <= 0 || ttl > auth.OTPTTL {
		t.Fatalf("TTL = %v err=%v, want in (0,300s]", ttl, err)
	}
	if n, _ := rdb.Exists(ctx, auth.OTPAttemptsKey(phone)).Result(); n != 0 {
		t.Fatal("attempts key must not exist after Generate")
	}

	// Wrong codes 1..5 keep the code alive.
	for i := 1; i <= auth.MaxAttempts; i++ {
		ok, err := auth.Verify(ctx, store, phone, "000000")
		if err != nil || ok {
			t.Fatalf("wrong attempt %d = (%v,%v)", i, ok, err)
		}
	}
	attemptsTTL, _ := rdb.TTL(ctx, auth.OTPAttemptsKey(phone)).Result()
	if attemptsTTL <= 0 {
		t.Fatalf("attempts TTL misaligned: %v", attemptsTTL)
	}

	// Sixth attempt — even the correct code — burns both keys.
	ok, err := auth.Verify(ctx, store, phone, code)
	if err != nil || ok {
		t.Fatalf("6th attempt with correct code = (%v,%v), want false", ok, err)
	}
	if n, _ := rdb.Exists(ctx, auth.OTPCodeKey(phone), auth.OTPAttemptsKey(phone)).Result(); n != 0 {
		t.Fatal("burn must delete both keys")
	}

	// Fresh generate resets the attempt counter (MULTI SET+DEL).
	if _, err := auth.Generate(ctx, store, phone); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Verify(ctx, store, phone, "000000"); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Generate(ctx, store, phone); err != nil {
		t.Fatal(err)
	}
	if n, _ := rdb.Exists(ctx, auth.OTPAttemptsKey(phone)).Result(); n != 0 {
		t.Fatal("Generate must DEL the attempts key atomically with SET")
	}
	ok, err = auth.Verify(ctx, store, phone, func() string {
		c, _ := rdb.Get(ctx, auth.OTPCodeKey(phone)).Result()
		return c
	}())
	if err != nil || !ok {
		t.Fatalf("fresh code must verify (%v,%v); counter reset works", ok, err)
	}
}
