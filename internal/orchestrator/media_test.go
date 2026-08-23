package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
)

// --- MediaDownloader (whatsapp-media.service.ts) ---

type graphStub struct {
	mu        sync.Mutex
	metadata  []http.HandlerFunc // per attempt
	binary    []http.HandlerFunc
	metaCalls int
	binCalls  int
}

func noBackoff(attempt int) time.Duration { return 0 }

func newMediaDownloader(srv *httptest.Server) *orchestrator.MediaDownloader {
	return orchestrator.NewMediaDownloader(orchestrator.MediaConfig{
		Token:   "wa-token",
		Version: "v22.0",
		BaseURL: srv.URL,
		Client:  srv.Client(),
		Backoff: noBackoff,
	})
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadMediaTwoStepFlow(t *testing.T) {
	payload := []byte("OGG-BYTES-0123456789")
	var binaryURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v22.0/media_123" {
			if got := r.Header.Get("Authorization"); got != "Bearer wa-token" {
				t.Errorf("metadata auth = %q", got)
			}
			writeJSON(t, w, 200, map[string]string{
				"url":       binaryURL + "/file",
				"mime_type": "audio/ogg",
			})
			return
		}
		if r.URL.Path == "/file" {
			if got := r.Header.Get("Authorization"); got != "Bearer wa-token" {
				t.Errorf("binary auth = %q", got)
			}
			w.Header().Set("Content-Type", "audio/ogg; codecs=opus")
			_, _ = w.Write(payload)
			return
		}
		t.Errorf("unexpected path %q", r.URL.Path)
	}))
	defer srv.Close()
	binaryURL = srv.URL

	data, mime, err := newMediaDownloader(srv).Download(context.Background(), "media_123")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if string(data) != string(payload) {
		t.Errorf("bytes = %q, want %q", data, payload)
	}
	if mime != "audio/ogg; codecs=opus" {
		t.Errorf("mime = %q, want binary response Content-Type to win", mime)
	}
}

func TestDownloadMediaMetadata404RetriesThenPermanentError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	d := newMediaDownloader(srv)
	_, _, err := d.Download(context.Background(), "gone_id")
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"Permanently failed to download media gone_id", "Failed to fetch media metadata"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}

func TestDownloadMediaAttemptCount(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, _, err := newMediaDownloader(srv).Download(context.Background(), "gone")
	if err == nil || !strings.Contains(err.Error(), "Permanently failed") {
		t.Fatalf("err = %v, want permanent failure", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != orchestrator.MediaDownloadAttempts {
		t.Errorf("metadata attempts = %d, want %d (maxRetries)", calls, orchestrator.MediaDownloadAttempts)
	}
}

func TestDownloadMediaMissingURLInMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]string{})
	}))
	defer srv.Close()

	_, _, err := newMediaDownloader(srv).Download(context.Background(), "m1")
	if err == nil || !strings.Contains(err.Error(), "Media URL not found in metadata response") {
		t.Errorf("err = %v, want missing-url message", err)
	}
}

func TestDownloadMediaBinaryFailureMessage(t *testing.T) {
	var base string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/m1") {
			writeJSON(t, w, 200, map[string]string{"url": base + "/bin"})
			return
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	base = srv.URL

	_, _, err := newMediaDownloader(srv).Download(context.Background(), "m1")
	if err == nil || !strings.Contains(err.Error(), "Failed to download media binary") {
		t.Errorf("err = %v, want binary failure message", err)
	}
}

func TestDownloadMediaRequiresToken(t *testing.T) {
	d := orchestrator.NewMediaDownloader(orchestrator.MediaConfig{BaseURL: "http://127.0.0.1:1"})
	_, _, err := d.Download(context.Background(), "m1")
	if err == nil || !strings.Contains(err.Error(), "WHATSAPP_ACCESS_TOKEN is not configured") {
		t.Errorf("err = %v, want token error", err)
	}
}

func TestShouldEscalateAudioThreshold(t *testing.T) {
	if orchestrator.AudioMaxBytes != 10*1024*1024 {
		t.Errorf("AudioMaxBytes = %d, want 10485760", orchestrator.AudioMaxBytes)
	}
	if orchestrator.ShouldEscalateAudio(orchestrator.AudioMaxBytes) {
		t.Error("exactly 10MB must NOT escalate (source uses strict >)")
	}
	if !orchestrator.ShouldEscalateAudio(orchestrator.AudioMaxBytes + 1) {
		t.Error("10MB+1 must escalate")
	}
}

// --- Audio transcription (audio-transcription.service.ts via openai client) ---

func TestTranscribeAudioMultipartShape(t *testing.T) {
	var (
		gotFileHeader *multipartFileHeaderAlias
		gotModel      string
		auth          string
		path          string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		auth = r.Header.Get("Authorization")
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			t.Errorf("ParseMultipartForm: %v", err)
			w.WriteHeader(400)
			return
		}
		gotModel = r.FormValue("model")
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Errorf("form file: %v", err)
			w.WriteHeader(400)
			return
		}
		defer func() { _ = file.Close() }()
		body, _ := io.ReadAll(file)
		if string(body) != "AUDIO-PAYLOAD" {
			t.Errorf("file bytes = %q", body)
		}
		gotFileHeader = &multipartFileHeaderAlias{Filename: header.Filename, ContentType: header.Header.Get("Content-Type")}
		_, _ = w.Write([]byte(`{"text":"hello from the voice note"}`))
	}))
	defer srv.Close()

	client := openai.New(openai.Config{APIKey: "sk-t", BaseURL: srv.URL, Client: srv.Client()})
	text, err := client.TranscribeAudio(context.Background(), []byte("AUDIO-PAYLOAD"), "audio/ogg; codecs=opus")
	if err != nil {
		t.Fatalf("TranscribeAudio: %v", err)
	}
	if text != "hello from the voice note" {
		t.Errorf("text = %q", text)
	}
	if path != "/audio/transcriptions" {
		t.Errorf("path = %q", path)
	}
	if auth != "Bearer sk-t" {
		t.Errorf("auth = %q", auth)
	}
	if gotModel != "whisper-1" {
		t.Errorf("model field = %q, want whisper-1", gotModel)
	}
	if gotFileHeader == nil || gotFileHeader.Filename != "audio.ogg" {
		t.Errorf("filename = %+v, want audio.ogg (source toFile name)", gotFileHeader)
	}
	if gotFileHeader.ContentType != "audio/ogg; codecs=opus" {
		t.Errorf("part content type = %q", gotFileHeader.ContentType)
	}
}

type multipartFileHeaderAlias struct {
	Filename    string
	ContentType string
}

func TestTranscribeAudioErrorMappingAfterAttempts(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		http.Error(w, `{"error":{"message":"quota exceeded"}}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()

	attempts := 2
	client := openai.New(openai.Config{
		APIKey:             "sk-t",
		BaseURL:            srv.URL,
		Client:             srv.Client(),
		TranscribeAttempts: attempts,
		TranscribeBackoff:  noBackoff,
	})

	_, err := client.TranscribeAudio(context.Background(), []byte("x"), "")
	var apiErr *openai.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("want typed *openai.Error in chain, got %T: %v", err, err)
	}
	if apiErr.Status != http.StatusTooManyRequests {
		t.Errorf("status = %d", apiErr.Status)
	}
	if !strings.Contains(err.Error(), "failed to transcribe audio after 2 attempts") {
		t.Errorf("permanent error should mention attempts, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != attempts {
		t.Errorf("calls = %d, want %d", calls, attempts)
	}
}

func TestTranscribeAudioDefaultsToOggMime(t *testing.T) {
	var gotType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseMultipartForm(1 << 20)
		file, header, err := r.FormFile("file")
		if err != nil {
			w.WriteHeader(400)
			return
		}
		_ = file.Close()
		gotType = header.Header.Get("Content-Type")
		_, _ = w.Write([]byte(`{"text":"ok"}`))
	}))
	defer srv.Close()

	client := openai.New(openai.Config{APIKey: "sk", BaseURL: srv.URL, Client: srv.Client(), TranscribeBackoff: noBackoff})
	if _, err := client.TranscribeAudio(context.Background(), []byte("x"), ""); err != nil {
		t.Fatal(err)
	}
	if gotType != "audio/ogg" {
		t.Errorf("default part content type = %q, want audio/ogg (.ogg filename)", gotType)
	}
}
