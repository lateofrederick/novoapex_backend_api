package queue

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// Per-queue rate limiting (T6.3).
//
// DESIGN (documented per brief): the limiter is a Redis-backed fixed window —
// an atomic INCR+PEXPIRE Lua script keyed by queue + window index. A local
// golang.org/x/time/rate.Limiter would silently multiply the limit by the
// number of worker replicas; BullMQ limiters live in Redis, so a like-for-like
// port must be honoured cluster-wide.
//
// BEHAVIOUR: when the window is full the middleware BLOCKS (polling) until a
// slot opens, matching BullMQ semantics where a throttled job waits in the
// queue rather than failing. Only if the block would exceed maxBlockWait (or
// the task context is cancelled, e.g. graceful shutdown) does the handler
// return a *RateLimitedError, which retryDelay maps to a flat reschedule after
// the window instead of exponential backoff.

const (
	// rateLimitKeyPrefix namespaces limiter counters away from asynq's keys.
	rateLimitKeyPrefix = "goqueue:ratelimit:"

	// minRetryDelay floors RateLimitedError reschedules so a pathological
	// zero-length window cannot spin the broker.
	minRetryDelay = 250 * time.Millisecond

	// pollInterval caps how often a blocked handler re-checks the window.
	pollInterval = 250 * time.Millisecond
)

// maxBlockWait bounds inline waiting. Windows are ≤10s in the §B.2 table, so
// waiting up to two windows always suffices; beyond that we hand the wait off
// to the scheduler via RateLimitedError instead of pinning a goroutine.
func maxBlockWait(window time.Duration) time.Duration {
	if w := 2 * window; w > 0 {
		return w
	}
	return 30 * time.Second
}

// RateLimitedError signals that the per-queue limiter is saturated. The
// server's RetryDelayFunc reschedules the task flat after RetryIn — it does
// NOT count as a business failure pattern (the attempt budget still ticks, but
// only after >2 windows of continuous saturation, which the §B.2 limits make
// unreachable in practice).
type RateLimitedError struct {
	Queue    string
	RetryIn  time.Duration
	Deadline bool // true when the block was cut short by ctx cancellation
}

func (e *RateLimitedError) Error() string {
	if e.Deadline {
		return fmt.Sprintf("queue %q rate limited (context deadline while waiting %s)", e.Queue, e.RetryIn)
	}
	return fmt.Sprintf("queue %q rate limited, retry after %s", e.Queue, e.RetryIn)
}

// rateLimitScript atomically increments the window counter, sets its TTL on
// first hit, and reports the remaining window when over the limit.
//
// KEYS[1] = counter key   ARGV[1] = limit   ARGV[2] = window ms
// Returns {allowed(0|1), waitMs}.
var rateLimitScript = redis.NewScript(`
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
if count > tonumber(ARGV[1]) then
  return {0, redis.call('PTTL', KEYS[1])}
end
return {1, 0}
`)

// rateLimiter is a shared-redis fixed-window counter. Safe for concurrent use
// (all state lives in Redis).
type rateLimiter struct {
	rdb *redis.Client
	log *slog.Logger
}

func newRateLimiter(addr, password string, tlsOn bool, log *slog.Logger) *rateLimiter {
	if log == nil {
		log = slog.Default()
	}
	opts := &redis.Options{Addr: addr}
	if password != "" {
		opts.Password = password
	}
	if tlsOn {
		host, _, _ := splitHostPort(addr)
		opts.TLSConfig = tlsConfigFor(host)
	}
	return &rateLimiter{rdb: redis.NewClient(opts), log: log}
}

func (l *rateLimiter) close() error { return l.rdb.Close() }

// allow reports whether one more item fits into the current fixed window for
// key suffix queueName; when denied it returns how long until the window
// rolls over.
func (l *rateLimiter) allow(ctx context.Context, queueName string, limit int, window time.Duration) (wait time.Duration, ok bool, err error) {
	windowIdx := time.Now().UnixMilli() / window.Milliseconds()
	key := fmt.Sprintf("%s%s:%d", rateLimitKeyPrefix, queueName, windowIdx)
	res, err := rateLimitScript.Run(ctx, l.rdb, []string{key}, limit, window.Milliseconds()).Slice()
	if err != nil {
		return 0, false, fmt.Errorf("ratelimit: eval %s: %w", queueName, err)
	}
	allowed, _ := res[0].(int64)
	if allowed == 1 {
		return 0, true, nil
	}
	waitMs, _ := res[1].(int64)
	if waitMs < 0 {
		waitMs = window.Milliseconds() // TTL raced; next window is a safe bet
	}
	return time.Duration(waitMs) * time.Millisecond, false, nil
}

// rateLimited wraps a Handler with the queue's limiter policy. The queue name
// is bound at registration time (task-type prefix).
func (s *Server) rateLimited(queueName string, p QueuePolicy) func(next Handler) Handler {
	return func(next Handler) Handler {
		return func(ctx context.Context, payload []byte) error {
			started := time.Now()
			blockCap := maxBlockWait(p.RateWindow)
			for {
				// Check cancellation before touching redis: a dead context is
				// a requeue signal, never a reason to fail-open.
				if ctx.Err() != nil {
					return &RateLimitedError{Queue: queueName, RetryIn: minRetryDelay, Deadline: true}
				}
				wait, ok, err := s.rdb.allow(ctx, queueName, p.RateLimit, p.RateWindow)
				if err != nil {
					// Fail-open on limiter infrastructure errors: BullMQ
					// throttling must not turn a healthy job into a failure.
					s.log.Error("ratelimit: redis error; allowing task", slog.Any("error", err))
					return next(ctx, payload)
				}
				if ok {
					return next(ctx, payload)
				}
				if waited := time.Since(started); waited >= blockCap || wait > blockCap {
					// Hand the remaining wait off to the scheduler instead of
					// pinning the worker slot indefinitely.
					return &RateLimitedError{Queue: queueName, RetryIn: wait}
				}
				s.log.Debug("ratelimit: window full; task waits",
					slog.String("queue", queueName),
					slog.Duration("wait", wait))
				select {
				case <-ctx.Done():
					return &RateLimitedError{Queue: queueName, RetryIn: minRetryDelay, Deadline: true}
				case <-time.After(min(wait, pollInterval)):
				}
			}
		}
	}
}

func splitHostPort(addr string) (host, port string, err error) {
	return net.SplitHostPort(addr)
}

func tlsConfigFor(host string) *tls.Config {
	return &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
}
