package harness

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScrubStringDeterministic(t *testing.T) {
	first := ScrubString("+233555123456")
	second := ScrubString("+233555123456")
	if first != second {
		t.Fatalf("scrub not deterministic: %q vs %q", first, second)
	}
	if strings.Contains(first, "233555") {
		t.Fatalf("phone digits leaked: %q", first)
	}
	if ScrubString("+233555654321") == first {
		t.Fatal("different phones must produce different tokens")
	}
}

func TestScrubJSONKeysAndValues(t *testing.T) {
	input := []byte(`{
		"entry": [{
			"changes": [{
				"value": {
					"contacts": [{"wa_id": "233555123456", "profile": {"name": "Ama"}}],
					"messages": [{"from": "+233555123456", "text": {"body": "my total is 150.50"}}]
				}
			}]
		}],
		"conversation_id": "conv-abc-123",
		"order_total": 12500
	}`)

	scrubbed, ok := ScrubJSON(input)
	if !ok {
		t.Fatal("valid JSON must scrub successfully")
	}
	s := string(scrubbed)

	if strings.Contains(s, "233555123456") || strings.Contains(s, "conv-abc-123") {
		t.Errorf("PII leaked in scrubbed output:\n%s", s)
	}
	if !strings.Contains(s, "Ama") {
		t.Error("non-PII string values must be preserved")
	}
	if !strings.Contains(s, "150.50") {
		t.Error("decimal amounts inside text must not be mangled by phone regex")
	}
	if !strings.Contains(s, "12500") {
		t.Error("numeric amounts must not be touched")
	}

	again, _ := ScrubJSON(input)
	if string(again) != s {
		t.Error("JSON scrubbing must be deterministic")
	}
}

func TestRecorderCapturesAndScrubs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", "req-42")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	var corpus bytes.Buffer
	rec, err := NewRecorder(&corpus)
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}

	client := rec.Client(server.Client())
	req, err := http.NewRequest(http.MethodPost, server.URL+"/webhooks/whatsapp?hub_mode=subscribe",
		strings.NewReader(`{"from":"+233555123456","conversation_id":"conv-9","msg":"hi"}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer secret-token")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	path := filepath.Join(t.TempDir(), "corpus.jsonl")
	if err := os.WriteFile(path, corpus.Bytes(), 0o600); err != nil {
		t.Fatalf("write corpus: %v", err)
	}

	exchanges, err := LoadCorpus(path)
	if err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	if len(exchanges) != 1 {
		t.Fatalf("exchanges = %d, want 1", len(exchanges))
	}

	ex := exchanges[0]
	if ex.Method != http.MethodPost || ex.Path != "/webhooks/whatsapp" ||
		ex.Query != "hub_mode=subscribe" || ex.Status != http.StatusOK {
		t.Errorf("metadata wrong: %+v", ex)
	}

	rawReqBody, err := ex.RequestBody.Bytes()
	if err != nil {
		t.Fatalf("decode req body: %v", err)
	}
	reqText := string(rawReqBody)
	if strings.Contains(reqText, "+233555123456") || strings.Contains(reqText, "conv-9") {
		t.Errorf("PII leaked into recorded request body: %s", reqText)
	}
	if !strings.Contains(reqText, "scrub_") {
		t.Errorf("expected pseudonyms in recorded body: %s", reqText)
	}

	rawRespBody, err := ex.ResponseBody.Bytes()
	if err != nil {
		t.Fatalf("decode resp body: %v", err)
	}
	if string(rawRespBody) != reqText {
		t.Errorf("echo mismatch:\nreq:  %s\nresp: %s", reqText, rawRespBody)
	}

	if got := firstHeader(ex.RequestHeaders, "Authorization"); got != redactedMarker {
		t.Errorf("Authorization = %q, want redacted", got)
	}
	if got := firstHeader(ex.ResponseHeaders, "X-Request-Id"); got != "req-42" {
		t.Errorf("X-Request-Id = %q, want preserved", got)
	}

	replay, err := ex.ReconstructRequest(server.URL)
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if replay.URL.Path != "/webhooks/whatsapp" || replay.Method != http.MethodPost {
		t.Errorf("reconstructed request wrong: %s %s", replay.Method, replay.URL)
	}
	replayBody, _ := io.ReadAll(replay.Body)
	if len(replayBody) == 0 {
		t.Error("reconstructed request lost its body")
	}
}

func TestRecorderHandlesBinaryBodies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	var corpus bytes.Buffer
	rec, err := NewRecorder(&corpus)
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}

	binary := append([]byte{0x89, 'P', 'N', 'G'}, []byte{0xFF, 0xFE, 0x00, 0x01}...)
	client := rec.Client(server.Client())
	resp, err := client.Post(server.URL+"/upload", "application/octet-stream", bytes.NewReader(binary))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	exchanges, err := LoadCorpusFromBytes(corpus.Bytes())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(exchanges) != 1 {
		t.Fatalf("exchanges = %d, want 1", len(exchanges))
	}
	body := exchanges[0].RequestBody
	if body.Encoding != "base64" {
		t.Fatalf("binary body encoding = %q, want base64", body.Encoding)
	}
	decoded, err := body.Bytes()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(decoded, binary) {
		t.Error("binary payload corrupted through record/load cycle")
	}
}

func TestFilterByPathPrefix(t *testing.T) {
	exchanges := []Exchange{
		{Path: "/customers"},
		{Path: "/products/metrics"},
		{Path: "/products"},
		{Path: "/orders"},
	}
	got := FilterByPathPrefix(exchanges, "/products")
	if len(got) != 2 {
		t.Fatalf("filtered = %d, want 2", len(got))
	}
}

func LoadCorpusFromBytes(b []byte) ([]Exchange, error) {
	tmp, err := os.CreateTemp("", "corpus-*.jsonl")
	if err != nil {
		return nil, err
	}
	path := tmp.Name()
	defer func() { _ = os.Remove(path) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	return LoadCorpus(path)
}

func firstHeader(h map[string][]string, key string) string {
	for k, vs := range h {
		if strings.EqualFold(k, key) && len(vs) > 0 {
			return vs[0]
		}
	}
	return ""
}
