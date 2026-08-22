package httpx_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

func s3ThrottleHandler(name string, limit int, window time.Duration, hits *int) http.Handler {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits != nil {
			*hits++
		}
		w.WriteHeader(http.StatusOK)
	})
	return httpx.NewThrottler(name, limit, window).Middleware(base)
}

func s3ThrottleReq(t *testing.T, h http.Handler, ip string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/auth/request-otp", nil)
	req.RemoteAddr = ip + ":12345"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// The 6th request inside one minute crosses limit 5 -> blocked with the
// @nestjs/throttler default message through the AllExceptionsFilter envelope.
func TestS3_Throttle_SixthRequestBlocked(t *testing.T) {
	var hits int
	h := s3ThrottleHandler("AuthController-requestOtp-default", 5, time.Minute, &hits)

	statuses := make([]int, 0, 7)
	for i := 0; i < 6; i++ {
		rec := s3ThrottleReq(t, h, "10.1.1.1")
		statuses = append(statuses, rec.Code)
		if i < 5 {
			if got := rec.Header().Get("X-RateLimit-Limit"); got != "5" {
				t.Fatalf("hit %d X-RateLimit-Limit = %q", i, got)
			}
			if want := strconv.Itoa(5 - (i + 1)); rec.Header().Get("X-RateLimit-Remaining") != want {
				t.Fatalf("hit %d remaining = %q, want %q", i, rec.Header().Get("X-RateLimit-Remaining"), want)
			}
		}
	}
	wantStatuses := []int{200, 200, 200, 200, 200, 429}
	for i := range wantStatuses {
		if statuses[i] != wantStatuses[i] {
			t.Fatalf("statuses = %v, want %v", statuses, wantStatuses)
		}
	}

	rec := s3ThrottleReq(t, h, "10.1.1.1")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("blocked request status %d", rec.Code)
	}
	if ra := rec.Header().Get("Retry-After"); ra == "" || ra == "0" {
		t.Fatalf("Retry-After header missing/zero: %q", ra)
	}

	var body struct {
		StatusCode int    `json:"statusCode"`
		Timestamp  string `json:"timestamp"`
		Path       string `json:"path"`
		Message    string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body)
	}
	if body.StatusCode != 429 ||
		body.Message != "ThrottlerException: Too Many Requests" ||
		body.Path != "/auth/request-otp" ||
		body.Timestamp == "" {
		t.Fatalf("429 body mismatch: %+v", body)
	}
}

// Trackers are independent: another IP keeps its own budget (req.ip keying).
func TestS3_Throttle_IPKeying(t *testing.T) {
	h := s3ThrottleHandler("AuthController-verifyOtp-default", 5, time.Minute, nil)
	for i := 0; i < 5; i++ {
		if rec := s3ThrottleReq(t, h, "10.2.0.1"); rec.Code != 200 {
			t.Fatalf("ip A hit %d = %d", i, rec.Code)
		}
	}
	if rec := s3ThrottleReq(t, h, "10.2.0.1"); rec.Code != 429 {
		t.Fatal("ip A should be blocked")
	}
	if rec := s3ThrottleReq(t, h, "10.2.0.2"); rec.Code != 200 {
		t.Fatalf("ip B must have a fresh budget, got %d", rec.Code)
	}
}

// Leftmost X-Forwarded-For wins over RemoteAddr (trust proxy = 1).
func TestS3_Throttle_XFFKeying(t *testing.T) {
	var hits int
	h := s3ThrottleHandler("AuthController-requestOtp-default", 2, time.Minute, &hits)

	do := func(xff string) int {
		req := httptest.NewRequest(http.MethodPost, "/auth/request-otp", nil)
		req.RemoteAddr = "172.31.0.9:4444" // the proxy's address
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	for i := 0; i < 2; i++ {
		if code := do("203.0.113.7"); code != 200 {
			t.Fatalf("xff client hit %d = %d", i, code)
		}
	}
	if code := do("203.0.113.7"); code != 429 {
		t.Fatalf("xff client third request = %d, want 429", code)
	}
	// Same socket peer but different forwarded client -> separate bucket.
	if code := do("203.0.113.8"); code != 200 {
		t.Fatalf("different XFF client = %d, want 200", code)
	}
	// No XFF at all -> falls back to RemoteAddr host.
	if code := do(""); code != 200 {
		t.Fatalf("RemoteAddr fallback = %d, want 200", code)
	}

	tracker := func(xff, remote string) string {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = remote
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}
		return httpx.ThrottleTracker(req)
	}
	if got := tracker("198.51.100.3, 10.0.0.5", "10.0.0.5:9"); got != "198.51.100.3" {
		t.Fatalf("multi-entry XFF tracker = %q, want leftmost", got)
	}
	if got := tracker("", "10.0.0.5:9"); got != "10.0.0.5" {
		t.Fatalf("no-XFF tracker = %q, want RemoteAddr host", got)
	}
	if got := tracker("  ,  ,198.51.100.9 ", "x"); got != "198.51.100.9" {
		t.Fatalf("sparse XFF tracker = %q", got)
	}
}

// Names isolate buckets per route even for the same tracker, mirroring Nest's
// `${class}-${handler}-${name}` generateKey prefix.
func TestS3_Throttle_NamesIsolateBuckets(t *testing.T) {
	var hits int
	a := s3ThrottleHandler("AuthController-requestOtp-default", 1, time.Minute, &hits)
	b := s3ThrottleHandler("AuthController-verifyOtp-default", 1, time.Minute, &hits)

	if rec := s3ThrottleReq(t, a, "10.3.0.1"); rec.Code != 200 {
		t.Fatal("route A first")
	}
	if rec := s3ThrottleReq(t, a, "10.3.0.1"); rec.Code != 429 {
		t.Fatal("route A second must block")
	}
	if rec := s3ThrottleReq(t, b, "10.3.0.1"); rec.Code != 200 {
		t.Fatal("route B must not inherit route A's counter")
	}
}

// After one full window the hits decay out and requests pass again
// (setExpirationTime's setTimeout semantics).
func TestS3_Throttle_WindowDecayReleases(t *testing.T) {
	var hits int
	window := 40 * time.Millisecond
	h := s3ThrottleHandler("s3-decay-test", 2, window, &hits)

	for i := 0; i < 2; i++ {
		s3ThrottleReq(t, h, "10.4.0.1")
	}
	if rec := s3ThrottleReq(t, h, "10.4.0.1"); rec.Code != 429 {
		t.Fatal("third immediate request must block")
	}

	time.Sleep(3 * window / 2)

	if rec := s3ThrottleReq(t, h, "10.4.0.1"); rec.Code != 200 {
		t.Fatalf("after decay release, status=%d body=%s", rec.Code, rec.Body)
	}
}
