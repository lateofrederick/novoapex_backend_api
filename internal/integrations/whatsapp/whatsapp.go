// Package whatsapp ports libs/common/src/whatsapp/whatsapp.service.ts's Meta
// Cloud API text-message call. The URL shape (whatsapp.service.ts sendMessage):
//
//	POST https://graph.facebook.com/${WHATSAPP_API_VERSION}/${phoneNumberId}/messages
//	Authorization: Bearer ${WHATSAPP_ACCESS_TOKEN}
//
// with payload {messaging_product:'whatsapp', to, type:'text', text:{body}}.
//
// WHATSAPP_BASE_URL overrides the graph origin for tests; production defaults
// to https://graph.facebook.com.
package whatsapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// DefaultBaseURL is the Meta Graph API origin.
const DefaultBaseURL = "https://graph.facebook.com"

const defaultTimeout = 30 * time.Second

// Client sends messages to one WhatsApp Cloud API number.
type Client struct {
	BaseURL string // WHATSAPP_BASE_URL override, else DefaultBaseURL
	Version string // e.g. "v25.0" (config default)
	Token   string // WHATSAPP_ACCESS_TOKEN
	HTTP    *http.Client
}

// New builds a client from the standard config surface.
func New(version, token string) *Client {
	base := os.Getenv("WHATSAPP_BASE_URL")
	if base == "" {
		base = DefaultBaseURL
	}
	return &Client{BaseURL: base, Version: version, Token: token}
}

// sendPayload preserves the JSON key order of the Node payload object
// (whatsapp.service.ts): messaging_product first via the spread, then the
// per-type fields.
type sendPayload struct {
	MessagingProduct string      `json:"messaging_product"`
	To               string      `json:"to"`
	Type             string      `json:"type"`
	Text             *textObject `json:"text,omitempty"`
}

type textObject struct {
	Body string `json:"body"`
}

// SendTextMessage ports WhatsAppService.sendTextMessage -> sendMessage
// (whatsapp.service.ts). Non-2xx replies are surfaced as
// "Meta API error: <raw body>" exactly like the Node service wraps them.
func (c *Client) SendTextMessage(ctx context.Context, phoneNumberID, to, text string) error {
	url := fmt.Sprintf("%s/%s/%s/messages", strings.TrimSuffix(c.BaseURL, "/"), c.Version, phoneNumberID)

	payload := sendPayload{
		MessagingProduct: "whatsapp",
		To:               to,
		Type:             "text",
		Text:             &textObject{Body: text},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("whatsapp: encode payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("whatsapp: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("whatsapp: POST %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	var data any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return fmt.Errorf("whatsapp: decode response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Meta error envelope: {"error":{"message":...,"type":...,"code":...}}
		// Capitalisation mirrors whatsapp.service.ts's exact Error text.
		return fmt.Errorf("Meta API error: %s", reEncode(data)) //nolint:staticcheck // node parity
	}
	return nil
}

func reEncode(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "<unserializable>"
	}
	return string(b)
}
