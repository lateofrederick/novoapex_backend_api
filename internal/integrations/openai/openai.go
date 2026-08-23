// Package openai is a minimal HTTP client for OpenAI endpoints: embeddings
// (T6.16), chat/responses structured output + audio transcription (T8.13–T8.16).
// No SDK dependency: request/response shapes mirror exactly what the Node side
// (`@ai-sdk/openai`, `openai`) puts on the wire so behaviour stays identical
// across stacks.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"
)

const DefaultBaseURL = "https://api.openai.com/v1"

type Config struct {
	APIKey  string
	BaseURL string // optional override (OPENAI_BASE_URL); default DefaultBaseURL
	Client  *http.Client

	// Transcription retry policy (audio-transcription.service.ts): 3 attempts,
	// exponential backoff 2^attempt * 1s after each failed attempt. Zero values
	// select those source defaults; tests may shrink them.
	TranscribeAttempts int
	TranscribeBackoff  func(attempt int) time.Duration // attempt starts at 1
}

type Client struct {
	apiKey            string
	baseURL           string
	http              *http.Client
	transcribeAttempt int
	transcribeBackoff func(attempt int) time.Duration
}

func New(cfg Config) *Client {
	base := cfg.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	hc := cfg.Client
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	attempts := cfg.TranscribeAttempts
	if attempts <= 0 {
		attempts = 3
	}
	backoff := cfg.TranscribeBackoff
	if backoff == nil {
		backoff = func(attempt int) time.Duration { return time.Duration(1<<uint(attempt)) * time.Second }
	}
	return &Client{
		apiKey:            cfg.APIKey,
		baseURL:           strings.TrimRight(base, "/"),
		http:              hc,
		transcribeAttempt: attempts,
		transcribeBackoff: backoff,
	}
}

// Error is a deterministic non-2xx response.
type Error struct {
	Status int
	Body   string
}

func (e *Error) Error() string {
	body := e.Body
	const limit = 512
	if len(body) > limit {
		body = body[:limit] + "..."
	}
	return fmt.Sprintf("openai: status %d: %s", e.Status, body)
}

type embeddingsRequest struct {
	Model          string   `json:"model"`
	Input          []string `json:"input"`
	EncodingFormat string   `json:"encoding_format"`
	Dimensions     int      `json:"dimensions,omitempty"`
}

type embeddingsResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

// Embedding generates one embedding vector for input using model, truncated/
// reduced to dimensions when dimensions > 0 (text-embedding-3-small @ 768 in
// production — the dimension-reduction parameter must be passed identically
// to the Node side or every vector silently changes).
func (c *Client) Embedding(ctx context.Context, model string, dimensions int, input string) ([]float32, error) {
	reqBody, err := json.Marshal(embeddingsRequest{
		Model:          model,
		Input:          []string{input},
		EncodingFormat: "float",
		Dimensions:     dimensions,
	})
	if err != nil {
		return nil, fmt.Errorf("openai: encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embeddings", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai: post /embeddings: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("openai: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &Error{Status: resp.StatusCode, Body: string(body)}
	}

	var decoded embeddingsResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("openai: decode response: %w", err)
	}
	if decoded.Error != nil {
		return nil, &Error{Status: resp.StatusCode, Body: decoded.Error.Message}
	}
	if len(decoded.Data) == 0 {
		return nil, fmt.Errorf("openai: empty data array for %q", model)
	}
	return decoded.Data[0].Embedding, nil
}

// --- Responses API (structured output), T8.13/T8.14 ---

// ResponsesUsage mirrors the usage block of a /responses reply.
type ResponsesUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

// ResponsesContentPart is one content part of an output message item
// ({"type":"output_text","text":...}).
type ResponsesContentPart struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// ResponsesOutputItem is one entry of the output array (message items carry
// content parts; reasoning/web_search items are decoded but ignored).
type ResponsesOutputItem struct {
	Type    string                 `json:"type,omitempty"`
	Role    string                 `json:"role,omitempty"`
	Content []ResponsesContentPart `json:"content,omitempty"`
}

// ResponsesReply is the parsed envelope of POST {base}/responses.
type ResponsesReply struct {
	ID     string                `json:"id"`
	Model  string                `json:"model,omitempty"`
	Status string                `json:"status,omitempty"`
	Output []ResponsesOutputItem `json:"output"`
	Usage  ResponsesUsage        `json:"usage"`
}

// OutputText concatenates every output_text part across output message items —
// the equivalent of the SDK's response.output_text convenience property.
func (r *ResponsesReply) OutputText() string {
	var b strings.Builder
	for _, item := range r.Output {
		for _, part := range item.Content {
			if part.Type == "output_text" {
				b.WriteString(part.Text)
			}
		}
	}
	return b.String()
}

// ChatResponses posts a pre-marshalled /responses request body. The caller
// owns the wire format (model, input messages, text.format json_schema) so the
// structured-output engine can evolve without transport changes.
func (c *Client) ChatResponses(ctx context.Context, body []byte) (*ResponsesReply, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai: post /responses: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("openai: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &Error{Status: resp.StatusCode, Body: string(raw)}
	}

	var decoded ResponsesReply
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("openai: decode /responses envelope: %w", err)
	}
	return &decoded, nil
}

// TranscribeAudio ports AudioTranscriptionService.transcribe
// (audio-transcription.service.ts): whisper-1 multipart upload with the buffer
// always named audio.ogg, retried up to 3 times with exponential backoff
// before surfacing a permanent error.
func (c *Client) TranscribeAudio(ctx context.Context, audio []byte, mime string) (string, error) {
	if mime == "" {
		mime = "audio/ogg"
	}
	var lastErr error
	for attempt := 1; attempt <= c.transcribeAttempt; attempt++ {
		text, err := c.transcribeOnce(ctx, audio, mime)
		if err == nil {
			return text, nil
		}
		lastErr = err
		slog.WarnContext(ctx, fmt.Sprintf("attempt %d to transcribe audio failed: %v", attempt, err))
		if attempt < c.transcribeAttempt {
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("openai: transcribe cancelled: %w", ctx.Err())
			case <-time.After(c.transcribeBackoff(attempt)):
			}
		}
	}
	// Source: "Failed to transcribe audio. Last error: ${lastError?.message}".
	return "", fmt.Errorf("openai: failed to transcribe audio after %d attempts. Last error: %w", c.transcribeAttempt, lastErr)
}

func (c *Client) transcribeOnce(ctx context.Context, audio []byte, mime string) (string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	// Source wraps via toFile(audioBuffer, 'audio.ogg') — filename fixed.
	partHeader := textproto.MIMEHeader{}
	partHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, "audio.ogg"))
	partHeader.Set("Content-Type", mime)
	part, err := w.CreatePart(partHeader)
	if err != nil {
		return "", fmt.Errorf("openai: build multipart file part: %w", err)
	}
	if _, err := part.Write(audio); err != nil {
		return "", fmt.Errorf("openai: write multipart file part: %w", err)
	}
	if err := w.WriteField("model", "whisper-1"); err != nil {
		return "", fmt.Errorf("openai: write multipart model field: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("openai: close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/audio/transcriptions", bytes.NewReader(buf.Bytes()))
	if err != nil {
		return "", fmt.Errorf("openai: build request: %w", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("openai: post /audio/transcriptions: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return "", fmt.Errorf("openai: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &Error{Status: resp.StatusCode, Body: string(body)}
	}

	var decoded struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", fmt.Errorf("openai: decode transcription: %w", err)
	}
	return decoded.Text, nil
}
