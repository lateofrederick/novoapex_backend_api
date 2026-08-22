package cloudinary

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestProviderUploadSignedRequestShape drives the real provider against a
// stub server and asserts the SDK-style signed multipart contract.
func TestProviderUploadSignedRequestShape(t *testing.T) {
	const (
		cloud   = "stub-cloud"
		apiKey  = "key123"
		secret  = "secret456"
		fixedTS = int64(1755850123)
	)
	wantSig := func(params string) string {
		// SDK signature = sha1(sortedParams + apiSecret), NOT an HMAC.
		sum := sha1.Sum([]byte(params + secret))
		return hex.EncodeToString(sum[:])
	}

	var mu sync.Mutex
	var gotPath, gotCT, folder, ts_, apiKey_, sig string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotPath = r.URL.Path
		gotCT = r.Header.Get("Content-Type")
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			t.Errorf("parse multipart: %v", err)
			http.Error(w, `{"error":{"message":"parse"}}`, http.StatusBadRequest)
			return
		}
		v := r.MultipartForm.Value
		folder = v["folder"][0]
		ts_ = v["timestamp"][0]
		apiKey_ = v["api_key"][0]
		sig = v["signature"][0]
		_, _ = w.Write([]byte(`{"secure_url":"https://res.example/pic.png","public_id":"novoapex/products/b1/x"}`))
	}))
	defer ts.Close()

	p := New(Options{
		CloudName: cloud, APIKey: apiKey, APISecret: secret,
		BaseURL: ts.URL,
		Now:     func() time.Time { return time.Unix(fixedTS, 0) },
	})
	url, publicID, err := p.UploadImage(context.Background(), []byte("imgbytes"), "novoapex/products/b1")
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotPath != "/"+cloud+"/image/upload" {
		t.Fatalf("path = %q", gotPath)
	}
	if !strings.HasPrefix(gotCT, "multipart/form-data") {
		t.Fatalf("content-type = %q", gotCT)
	}
	if folder != "novoapex/products/b1" || apiKey_ != apiKey ||
		ts_ != strconv.FormatInt(fixedTS, 10) ||
		sig != wantSig("folder=novoapex/products/b1&timestamp="+strconv.FormatInt(fixedTS, 10)) {
		t.Fatalf("signed fields mismatch: folder=%q api_key=%q ts=%q sig=%q", folder, apiKey_, ts_, sig)
	}
	if url != "https://res.example/pic.png" || publicID != "novoapex/products/b1/x" {
		t.Fatalf("result url=%q public_id=%q", url, publicID)
	}
}

// TestProviderDestroyAndBestEffortContract: 'ok' parses; 'not found' AND a
// transport failure are swallowed (resolve rather than throw).
func TestProviderDestroyAndBestEffortContract(t *testing.T) {
	const (
		cloud   = "stub-cloud"
		apiKey  = "k"
		secret  = "s"
		fixedTS = int64(42)
	)
	var result string
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/"+cloud+"/image/destroy" {
			t.Errorf("destroy path = %q", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("form: %v", err)
		}
		if r.PostForm.Get("public_id") != "gone/asset" {
			t.Errorf("public_id = %q", r.PostForm.Get("public_id"))
		}
		mac := sha1.Sum([]byte("public_id=gone/asset&timestamp=" + strconv.FormatInt(fixedTS, 10) + secret))
		if r.PostForm.Get("signature") != hex.EncodeToString(mac[:]) {
			t.Errorf("destroy signature mismatch: %q", r.PostForm.Get("signature"))
		}
		_, _ = w.Write([]byte(`{"result":"` + result + `"}`))
	}))
	defer ts.Close()

	p := New(Options{
		CloudName: cloud, APIKey: apiKey, APISecret: secret,
		BaseURL: ts.URL,
		Now:     func() time.Time { return time.Unix(fixedTS, 0) },
	})

	result = "ok"
	if err := p.DeleteImage(context.Background(), "gone/asset"); err != nil {
		t.Fatalf("ok delete returned error: %v", err)
	}
	result = "not found"
	if err := p.DeleteImage(context.Background(), "gone/asset"); err != nil {
		t.Fatalf("'not found' delete returned error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("destroy calls = %d", calls)
	}

	// Transport failure (server closed -> connection refused): still swallowed.
	bad := New(Options{CloudName: cloud, APIKey: apiKey, APISecret: secret,
		BaseURL: "http://127.0.0.1:1",
		Now:     func() time.Time { return time.Unix(fixedTS, 0) }})
	if err := bad.DeleteImage(context.Background(), "x"); err != nil {
		t.Fatalf("transport failure surfaced: %v", err)
	}
}
