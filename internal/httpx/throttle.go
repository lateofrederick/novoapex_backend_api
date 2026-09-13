package httpx

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Throttle ports @nestjs/throttler v6.5.0 as wired in
// apps/mobile-api/src/mobile-api.module.ts:29-34 + APP_GUARD (lines 61-64):
// ThrottlerModule.forRoot([{ttl: 60_000, limit: 120}]) guards every route,
// and @Throttle({default:{limit:5, ttl:60_000}}) tightens the auth OTP routes
// (apps/mobile-api/src/auth/auth.controller.ts:16,25).
//
// Storage mirrors node_modules/@nestjs/throttler/dist/throttler.service.js
// (the default in-memory ThrottlerStorageService), including its decay
// semantics rather than a naive fixed window:
//   - every allowed hit increments totalHits AND schedules a -1 decrement one
//     window later (setExpirationTime's setTimeout);
//   - when totalHits exceeds limit the key becomes blocked for blockDuration
//     (= ttl by default): further hits neither count nor extend anything and
//     immediately raise ThrottlerException;
//   - when a blocked key's block expires, resetBlockdRequest zeroes its
//     counters, clears the pending decay timers (clearExpirationTimes) and
//     re-fires the request as a fresh hit.
//
// Keying mirrors ThrottlerGuard.getTracker (req.ip) + generateKey
// (sha256(`${class}-${handler}-${name}-${tracker}`)): with Express
// `trust proxy = 1` (apps/api/src/main.ts:26), req.ip resolves to the client
// address carried in X-Forwarded-For; without the header it falls back to the
// socket peer. Each route passes a distinct name so counters never collide.
//
// On block the guard sets Retry-After (seconds) then throws
// ThrottlerException('ThrottlerException: Too Many Requests', 429)
// (throttler.exception.js:5), which AllExceptionsFilter renders through the
// standard envelope — reproduced here via WriteError. Successful hits get the
// guard's X-RateLimit-Limit/-Remaining/-Reset headers (throttler.guard.js
// handleRequest, setHeaders defaults true).

const throttlerMessage = "ThrottlerException: Too Many Requests"

type throttleRecord struct {
	hits           int
	expiresAt      time.Time
	blockExpiresAt time.Time
	blocked        bool
	// gen invalidates pending decay timers on block reset
	// (clearExpirationTimes) without retaining the timers themselves.
	gen uint64
}

// Throttler is one named limit family (one Nest throttler entry).
// Safe for concurrent use.
type Throttler struct {
	name   string
	limit  int
	window time.Duration

	mu      sync.Mutex
	storage map[string]*throttleRecord
}

// NewThrottler builds a limiter for one logical route. name participates in
// the storage key exactly like Nest's `${class}-${handler}-${name}` prefix.
func NewThrottler(name string, limit int, window time.Duration) *Throttler {
	return &Throttler{
		name:    name,
		limit:   limit,
		window:  window,
		storage: make(map[string]*throttleRecord),
	}
}

// Middleware applies the throttle before next.
func (t *Throttler) Middleware(next http.Handler) http.Handler {
	return t.middleware(next, func(*http.Request) string { return t.name })
}

// PerRouteMiddleware applies the throttle with one bucket per method+path per
// client, approximating Nest's global ThrottlerGuard whose storage key embeds
// the controller class and handler (every endpoint has its own budget).
func (t *Throttler) PerRouteMiddleware(next http.Handler) http.Handler {
	return t.middleware(next, func(r *http.Request) string {
		return t.name + "-" + r.Method + " " + r.URL.Path
	})
}

func (t *Throttler) middleware(next http.Handler, scope func(*http.Request) string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tracker := ThrottleTracker(r)
		key := throttleStorageKey(scope(r), tracker)

		totalHits, timeToExpire, timeToBlockExpire, blocked := t.increment(key)

		if blocked {
			// throttler.guard.js: Retry-After precedes the exception.
			w.Header().Set("Retry-After", strconv.Itoa(timeToBlockExpire))
			WriteError(w, r, NewHTTPException(http.StatusTooManyRequests, throttlerMessage))
			return
		}

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(t.limit))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(max(0, t.limit-totalHits)))
		w.Header().Set("X-RateLimit-Reset", strconv.Itoa(timeToExpire))

		next.ServeHTTP(w, r)
	})
}

// increment mirrors ThrottlerStorageService.increment line for line.
func (t *Throttler) increment(key string) (totalHits, timeToExpire, timeToBlockExpire int, blocked bool) {
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	rec, ok := t.storage[key]
	if !ok {
		rec = &throttleRecord{expiresAt: now.Add(t.window)}
		t.storage[key] = rec
	}

	timeToExpire = secondsUntil(rec.expiresAt, now)
	if timeToExpire <= 0 {
		rec.expiresAt = now.Add(t.window)
		timeToExpire = secondsUntil(rec.expiresAt, now)
	}

	if !rec.blocked {
		rec.hits++
		t.scheduleDecay(key, rec.gen) // setExpirationTime: this hit ages out after one window
	}

	if rec.hits > t.limit && !rec.blocked {
		rec.blocked = true
		rec.blockExpiresAt = now.Add(t.window) // blockDuration defaults to ttl
	}

	// Node computes timeToBlockExpire BEFORE any unblock reset and returns
	// that (possibly stale) value.
	timeToBlockExpire = secondsUntil(rec.blockExpiresAt, now)
	if timeToBlockExpire <= 0 && rec.blocked {
		// resetBlockdRequest: isBlocked=false, totalHits=0,
		// clearExpirationTimes — then fireHitCount for this request.
		rec.blocked = false
		rec.hits = 0
		rec.gen++ // clearExpirationTimes: earlier decay timers become no-ops
		rec.hits++
		t.scheduleDecay(key, rec.gen)
	}

	return rec.hits, timeToExpire, timeToBlockExpire, rec.blocked
}

// scheduleDecay registers one -1 decrement a full window out (mirrors
// setExpirationTime's setTimeout). Timers are not retained, and a record whose
// hits have fully decayed — with any block lapsed — is dropped, so memory stays
// bounded by the set of currently active clients.
func (t *Throttler) scheduleDecay(key string, gen uint64) {
	time.AfterFunc(t.window, func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		rec, ok := t.storage[key]
		if !ok || rec.gen != gen {
			return
		}
		if rec.hits > 0 {
			rec.hits--
		}
		if rec.hits == 0 && (!rec.blocked || !time.Now().Before(rec.blockExpiresAt)) {
			delete(t.storage, key)
		}
	})
}

func secondsUntil(at, now time.Time) int {
	if !at.After(now) {
		return 0
	}
	return int(math.Ceil(at.Sub(now).Seconds()))
}

// ThrottleTracker reproduces req.ip under Express trust proxy = 1: the
// leftmost X-Forwarded-For entry when present, else the socket peer host.
// Delta vs strict proxy-addr resolution: spoofed multi-hop chains ("fake,
// real") resolve to the LEFTMOST entry rather than the second-from-right
// untrusted hop; a single honest Caddy hop — the deployed topology — behaves
// identically.
func ThrottleTracker(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		for _, part := range strings.Split(xff, ",") {
			if s := strings.TrimSpace(part); s != "" {
				return s
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// throttleStorageKey mirrors generateKey: sha256 over
// `${class}-${handler}-${name}-${tracker}` with name already carrying the
// class-handler-default triple.
func throttleStorageKey(name, tracker string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%s", name, tracker)))
	return hex.EncodeToString(sum[:])
}
