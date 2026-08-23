// Package openai is a minimal HTTP client for the OpenAI embeddings endpoint
// (T6.16). No SDK dependency: the request/response shape mirrors exactly what
// the AI SDK (`@ai-sdk/openai` embed) puts on the wire so vectors stay
// bit-identical across stacks.
package openai

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

const DefaultBaseURL = "https://api.openai.com/v1"

type Config struct {
	APIKey  string
	BaseURL string // optional override (OPENAI_BASE_URL); default DefaultBaseURL
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
