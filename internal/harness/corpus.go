package harness

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Exchange struct {
	Timestamp       time.Time           `json:"ts"`
	Method          string              `json:"method"`
	Path            string              `json:"path"`
	Query           string              `json:"query,omitempty"`
	RequestHeaders  map[string][]string `json:"req_headers,omitempty"`
	RequestBody     Body                `json:"req_body,omitempty"`
	Status          int                 `json:"status"`
	ResponseHeaders map[string][]string `json:"resp_headers,omitempty"`
	ResponseBody    Body                `json:"resp_body,omitempty"`
}

type Body struct {
	Encoding string `json:"encoding"`
	Data     string `json:"data"`
}

func (b Body) Bytes() ([]byte, error) {
	switch b.Encoding {
	case "", "utf8":
		return []byte(b.Data), nil
	case "base64":
		return base64.StdEncoding.DecodeString(b.Data)
	default:
		return nil, fmt.Errorf("unknown body encoding %q", b.Encoding)
	}
}

func textBody(b []byte) Body {
	if isValidUTF8(b) {
		return Body{Encoding: "utf8", Data: string(b)}
	}
	return Body{Encoding: "base64", Data: base64.StdEncoding.EncodeToString(b)}
}

func isValidUTF8(b []byte) bool { return utf8.Valid(b) }

var (
	phonePattern  = regexp.MustCompile(`\+?[0-9]{9,15}`)
	piiKeyPattern = regexp.MustCompile(`(?i)(phone|wa_id|whatsapp_id|conversation_?id|^from$|^to$|recipient|sender|customer_id)`)
)

const redactedMarker = "[REDACTED]"

func pseudonymize(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "scrub_" + hex.EncodeToString(sum[:8])
}

func ScrubString(s string) string {
	return phonePattern.ReplaceAllStringFunc(s, pseudonymize)
}

func ScrubJSON(b []byte) ([]byte, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	out, err := json.Marshal(scrubValue(v))
	if err != nil {
		return nil, false
	}
	return out, true
}

func scrubValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if s, ok := val.(string); ok && piiKeyPattern.MatchString(k) {
				t[k] = pseudonymize(s)
				continue
			}
			t[k] = scrubValue(val)
		}
		return t
	case []any:
		for i, item := range t {
			t[i] = scrubValue(item)
		}
		return t
	case string:
		return ScrubString(t)
	default:
		return v
	}
}

var sensitiveHeaders = map[string]bool{
	"authorization":        true,
	"cookie":               true,
	"set-cookie":           true,
	"x-hub-signature-256":  true,
	"x-paystack-signature": true,
	"x-forwarded-for":      true,
	"x-real-ip":            true,
	"x-api-key":            true,
	"apikey":               true,
}

func scrubHeaders(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		if sensitiveHeaders[strings.ToLower(k)] {
			out[k] = []string{redactedMarker}
			continue
		}
		scrubbed := make([]string, len(vs))
		copy(scrubbed, vs)
		for i, v := range vs {
			scrubbed[i] = ScrubString(v)
		}
		out[k] = scrubbed
	}
	return out
}

func scrubBody(contentType string, raw []byte) Body {
	isJSON := strings.Contains(contentType, "json")
	var data []byte
	if isJSON {
		if scrubbed, ok := ScrubJSON(raw); ok {
			data = scrubbed
		} else {
			data = []byte(ScrubString(string(raw)))
		}
	} else if isValidUTF8(raw) {
		data = []byte(ScrubString(string(raw)))
	} else {
		data = raw
	}
	return textBody(data)
}

type Recorder struct {
	mu  sync.Mutex
	enc *json.Encoder
	seq int
}

func NewRecorder(w io.Writer) (*Recorder, error) {
	enc := json.NewEncoder(w)
	return &Recorder{enc: enc}, nil
}

func (r *Recorder) Client(base *http.Client) *http.Client {
	next := http.DefaultTransport
	if base != nil && base.Transport != nil {
		next = base.Transport
	}
	timeout := time.Duration(0)
	if base != nil {
		timeout = base.Timeout
	}
	return &http.Client{Transport: &recordingTransport{r: r, next: next}, Timeout: timeout}
}

type recordingTransport struct {
	r    *Recorder
	next http.RoundTripper
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var reqRaw []byte
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("record request body: %w", err)
		}
		reqRaw = b
		req.Body = io.NopCloser(bytes.NewReader(b))
	}

	resp, err := rt.next.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	var respRaw []byte
	if resp.Body != nil {
		b, rerr := io.ReadAll(resp.Body)
		if rerr != nil {
			return nil, fmt.Errorf("record response body: %w", rerr)
		}
		_ = resp.Body.Close()
		respRaw = b
		resp.Body = io.NopCloser(bytes.NewReader(b))
	}

	ex := Exchange{
		Timestamp:       time.Now().UTC(),
		Method:          req.Method,
		Path:            req.URL.Path,
		Query:           req.URL.RawQuery,
		RequestHeaders:  scrubHeaders(req.Header),
		RequestBody:     scrubBody(req.Header.Get("Content-Type"), reqRaw),
		Status:          resp.StatusCode,
		ResponseHeaders: scrubHeaders(resp.Header),
		ResponseBody:    scrubBody(resp.Header.Get("Content-Type"), respRaw),
	}
	rt.r.record(ex)
	return resp, nil
}

func (r *Recorder) record(ex Exchange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.enc.Encode(ex); err != nil {
		panic(fmt.Sprintf("write corpus line: %v", err))
	}
}

func LoadCorpus(path string) ([]Exchange, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open corpus: %w", err)
	}
	defer func() { _ = f.Close() }()

	var out []Exchange
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		text := bytes.TrimSpace(sc.Bytes())
		if len(text) == 0 {
			continue
		}
		var ex Exchange
		if err := json.Unmarshal(text, &ex); err != nil {
			return nil, fmt.Errorf("corpus line %d: %w", line, err)
		}
		out = append(out, ex)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan corpus: %w", err)
	}
	return out, nil
}

func FilterByPathPrefix(exchanges []Exchange, prefix string) []Exchange {
	var out []Exchange
	for _, ex := range exchanges {
		if strings.HasPrefix(ex.Path, prefix) {
			out = append(out, ex)
		}
	}
	return out
}

func (e Exchange) ReconstructRequest(baseURL string) (*http.Request, error) {
	u := baseURL + e.Path
	if e.Query != "" {
		u += "?" + e.Query
	}
	body, err := e.RequestBody.Bytes()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(e.Method, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, vs := range e.RequestHeaders {
		if strings.EqualFold(k, "Host") || strings.EqualFold(k, "Content-Length") {
			continue
		}
		if sensitiveHeaders[strings.ToLower(k)] {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return req, nil
}
