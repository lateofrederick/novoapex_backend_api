package expo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DefaultBaseURL is Expo's production push notification API.
const DefaultBaseURL = "https://exp.host"

// Config carries settings for the Expo Push client.
type Config struct {
	BaseURL     string
	AccessToken string
	HTTPClient  *http.Client
}

// Client sends push notifications via Expo's HTTP push API.
type Client struct {
	baseURL     string
	accessToken string
	httpClient  *http.Client
}

// New creates an Expo push client.
func New(cfg Config) *Client {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{
		baseURL:     baseURL,
		accessToken: cfg.AccessToken,
		httpClient:  client,
	}
}

// PushMessage represents one message sent to the Expo push API.
type PushMessage struct {
	To        string         `json:"to"`
	Title     string         `json:"title,omitempty"`
	Subtitle  string         `json:"subtitle,omitempty"`
	Body      string         `json:"body,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	Sound     string         `json:"sound,omitempty"`
	Badge     int            `json:"badge,omitempty"`
	ChannelID string         `json:"channelId,omitempty"`
	Priority  string         `json:"priority,omitempty"`
}

// PushTicket is the result returned by Expo for each message.
type PushTicket struct {
	Status  string         `json:"status"` // "ok" or "error"
	ID      string         `json:"id,omitempty"`
	Message string         `json:"message,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

type PushResponse struct {
	Data   []PushTicket `json:"data"`
	Errors []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors,omitempty"`
}

// SendPushNotifications sends up to 100 messages in a single batch.
// Returns the slice of tickets and a slice of tokens that are invalid/unregistered.
func (c *Client) SendPushNotifications(ctx context.Context, messages []PushMessage) ([]PushTicket, []string, error) {
	if len(messages) == 0 {
		return nil, nil, nil
	}

	bodyBytes, err := json.Marshal(messages)
	if err != nil {
		return nil, nil, fmt.Errorf("expo: marshal messages: %w", err)
	}

	url := c.baseURL + "/--/api/v2/push/send"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("expo: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	if c.accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.accessToken)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("expo: post push notifications: %w", err)
	}
	defer res.Body.Close()

	respBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("expo: read response: %w", err)
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("expo: api returned status %d: %s", res.StatusCode, string(respBody))
	}

	var parsed PushResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, nil, fmt.Errorf("expo: unmarshal response: %w", err)
	}

	var unregisteredTokens []string
	for i, ticket := range parsed.Data {
		if ticket.Status == "error" && ticket.Details != nil {
			if errCode, ok := ticket.Details["error"].(string); ok && errCode == "DeviceNotRegistered" {
				if i < len(messages) {
					unregisteredTokens = append(unregisteredTokens, messages[i].To)
				}
			}
		}
	}

	return parsed.Data, unregisteredTokens, nil
}
