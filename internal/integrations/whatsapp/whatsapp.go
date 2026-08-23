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
	_, err := c.SendTextMessageData(ctx, phoneNumberID, to, text)
	return err
}

// SendTextMessageData is sendTextMessage with its return value: the parsed
// Meta response (messages.controller.ts feeds it into
// {success:true,data:<result>}). Shares the sendMessage HTTP path with the
// template/image sends below (T5.19d).
func (c *Client) SendTextMessageData(ctx context.Context, phoneNumberID, to, text string) (map[string]any, error) {
	return c.sendMessage(ctx, phoneNumberID, sendPayload{
		MessagingProduct: "whatsapp",
		To:               to,
		Type:             "text",
		Text:             &textObject{Body: text},
	})
}

// templateObject renders whatsapp.service.ts's
// {template:{name, language:{code}}} block.
type templateObject struct {
	Name     string         `json:"name"`
	Language languageObject `json:"language"`
}

type languageObject struct {
	Code string `json:"code"`
}

type templatePayload struct {
	MessagingProduct string         `json:"messaging_product"`
	To               string         `json:"to"`
	Type             string         `json:"type"`
	Template         templateObject `json:"template"`
}

// SendTemplateMessage ports sendTemplateMessage (whatsapp.service.ts:25-41).
// languageCode "" applies the source's default parameter en_US.
func (c *Client) SendTemplateMessage(ctx context.Context, phoneNumberID, to, templateName, languageCode string) (map[string]any, error) {
	if languageCode == "" {
		languageCode = "en_US"
	}
	return c.sendMessage(ctx, phoneNumberID, templatePayload{
		MessagingProduct: "whatsapp",
		To:               to,
		Type:             "template",
		Template: templateObject{
			Name:     templateName,
			Language: languageObject{Code: languageCode},
		},
	})
}

// imageObject carries the link-based image; Caption "" is omitted exactly
// like the source's `if (caption)` guard (whatsapp.service.ts:55-57).
type imageObject struct {
	Link    string `json:"link"`
	Caption string `json:"caption,omitempty"`
}

type imagePayload struct {
	MessagingProduct string      `json:"messaging_product"`
	To               string      `json:"to"`
	Type             string      `json:"type"`
	Image            imageObject `json:"image"`
}

// SendImageMessage ports sendImageMessage (whatsapp.service.ts:43-59): a
// LINK-based image payload ({image:{link}}), not a Meta media id upload.
func (c *Client) SendImageMessage(ctx context.Context, phoneNumberID, to, imageURL, caption string) (map[string]any, error) {
	return c.sendMessage(ctx, phoneNumberID, imagePayload{
		MessagingProduct: "whatsapp",
		To:               to,
		Type:             "image",
		Image:            imageObject{Link: imageURL, Caption: caption},
	})
}

// ExtractMessageId ports extractMessageId (whatsapp.service.ts:90-92):
// metaResponse?.messages?.[0]?.id — empty string stands in for undefined.
func ExtractMessageId(respJSON []byte) string {
	var resp struct {
		Messages []struct {
			ID string `json:"id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(respJSON, &resp); err != nil || len(resp.Messages) == 0 {
		return ""
	}
	return resp.Messages[0].ID
}

// sendMessage is the shared HTTP path of whatsapp.service.ts's private
// sendMessage (:61-88): POST ${base}/${version}/${phoneNumberID}/messages
// with Bearer auth, then parse; non-2xx becomes
// "Meta API error: <re-encoded body>". The legacy SendTextMessage keeps its
// Stage 3 observable behaviour by delegating here.
func (c *Client) sendMessage(ctx context.Context, phoneNumberID string, payload any) (map[string]any, error) {
	url := fmt.Sprintf("%s/%s/%s/messages", strings.TrimSuffix(c.BaseURL, "/"), c.Version, phoneNumberID)

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("whatsapp: encode payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("whatsapp: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")

	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("whatsapp: POST %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil, fmt.Errorf("whatsapp: decode response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Meta error envelope: {"error":{"message":...,"type":...,"code":...}}
		// Capitalisation mirrors whatsapp.service.ts's exact Error text.
		return nil, fmt.Errorf("Meta API error: %s", reEncode(data)) //nolint:staticcheck // node parity
	}
	return data, nil
}

func reEncode(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "<unserializable>"
	}
	return string(b)
}
