// Package google is a minimal HTTP client for the Google Generative AI
// embedContent endpoint (T6.19). The wire shape mirrors what the AI SDK
// (`@ai-sdk/google` embedding model) sends so image vectors stay identical.
package google

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const DefaultBaseURL = "https://generativelanguage.googleapis.com/v1beta"

type Config struct {
	APIKey  string
	BaseURL string // optional override; default DefaultBaseURL
	Client  *http.Client
}

type Client struct {
	apiKey  string
	baseURL string
	http    *http.Client
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
	return &Client{apiKey: cfg.APIKey, baseURL: strings.TrimRight(base, "/"), http: hc}
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
	return fmt.Sprintf("google: status %d: %s", e.Status, body)
}

// InlineData mirrors the Gemini inlineData content part (base64 media).
type InlineData struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}

// Part is one content part: either text or inline media. An empty Part list
// entry (Text=="" && InlineData==nil) matches the AI SDK's falsy-value
// behaviour of omitting the empty text part entirely.
type Part struct {
	Text       string      `json:"text,omitempty"`
	InlineData *InlineData `json:"inlineData,omitempty"`
}

type embedRequest struct {
	Model                string                 `json:"model"`
	Content              struct{ Parts []Part } `json:"content"`
	OutputDimensionality int                    `json:"outputDimensionality,omitempty"`
	TaskType             string                 `json:"taskType,omitempty"`
}

type embedResponse struct {
	Embedding struct {
		Values []float32 `json:"values"`
	} `json:"embedding"`
	Error *struct {
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

// EmbedContent calls POST {base}/models/{model}:embedContent with the given
// parts and returns the embedding values.
func (c *Client) EmbedContent(ctx context.Context, model string, dimensions int, parts []Part) ([]float32, error) {
	var reqBody embedRequest
	reqBody.Model = "models/" + model
	reqBody.Content.Parts = parts
	reqBody.OutputDimensionality = dimensions

	raw, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("google: encode request: %w", err)
	}

	url := fmt.Sprintf("%s/models/%s:embedContent", c.baseURL, model)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("google: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("x-goog-api-key", c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("google: post embedContent: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("google: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &Error{Status: resp.StatusCode, Body: string(body)}
	}

	var decoded embedResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("google: decode response: %w", err)
	}
	if decoded.Error != nil {
		return nil, &Error{Status: resp.StatusCode, Body: decoded.Error.Message}
	}
	if len(decoded.Embedding.Values) == 0 {
		return nil, fmt.Errorf("google: empty embedding for %q", model)
	}
	return decoded.Embedding.Values, nil
}
