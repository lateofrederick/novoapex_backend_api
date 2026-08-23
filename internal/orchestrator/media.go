// WhatsApp media download (T8.15) and audio escalation threshold (T8.16),
// ported from libs/orchestrator/src/media/whatsapp-media.service.ts and the
// media handling in conversation-orchestrator.service.ts.
package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	// DefaultGraphAPIVersion mirrors WHATSAPP_API_VERSION || 'v22.0'.
	DefaultGraphAPIVersion = "v22.0"
	// DefaultGraphBaseURL is the Graph API root the metadata GET hits.
	DefaultGraphBaseURL = "https://graph.facebook.com"

	// MediaDownloadAttempts mirrors maxRetries in whatsapp-media.service.ts.
	MediaDownloadAttempts = 3
)

// AudioMaxBytes is the 10 MB voice-note cap: a downloaded audio buffer larger
// than this escalates to a human instead of being transcribed
// (conversation-orchestrator.service.ts: `buffer.byteLength > 10 * 1024 * 1024`).
const AudioMaxBytes = 10 << 20

// ShouldEscalateAudio reports whether an audio payload exceeds the
// transcription threshold. Strictly greater-than, matching the source.
func ShouldEscalateAudio(size int) bool { return size > AudioMaxBytes }

// MediaConfig configures a MediaDownloader. Token is required; Version and
// BaseURL default to the Graph API production values and exist for tests.
type MediaConfig struct {
	Token   string // WHATSAPP_ACCESS_TOKEN
	Version string // WHATSAPP_API_VERSION, default DefaultGraphAPIVersion
	BaseURL string // Graph root, default DefaultGraphBaseURL

	Client  *http.Client
	Backoff func(attempt int) time.Duration // attempt starts at 1; default 2^attempt * 1s
}

// MediaDownloader ports WhatsAppMediaService.downloadMedia: a two-step fetch —
// GET {graph}/{version}/{mediaId} for the metadata (Bearer token), then GET of
// metadata.url for the bytes — retried up to three times with exponential
// backoff before failing permanently.
type MediaDownloader struct {
	token   string
	version string
	base    string
	client  *http.Client
	backoff func(attempt int) time.Duration
}

// NewMediaDownloader builds a downloader; empty fields fall back to source
// defaults.
func NewMediaDownloader(cfg MediaConfig) *MediaDownloader {
	version := cfg.Version
	if version == "" {
		version = DefaultGraphAPIVersion
	}
	base := cfg.BaseURL
	if base == "" {
		base = DefaultGraphBaseURL
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	backoff := cfg.Backoff
	if backoff == nil {
		backoff = func(attempt int) time.Duration { return time.Duration(1<<uint(attempt)) * time.Second }
	}
	return &MediaDownloader{
		token:   cfg.Token,
		version: version,
		base:    strings.TrimRight(base, "/"),
		client:  client,
		backoff: backoff,
	}
}

// Download returns the media bytes and their MIME type (Content-Type of the
// binary response; "" if absent). Error text mirrors the source service so
// escalation logs stay comparable across stacks, behind a "whatsapp media:"
// prefix:
//
//	"whatsapp media: WHATSAPP_ACCESS_TOKEN is not configured"
//	"whatsapp media: Failed to fetch media metadata: <status>"
//	"whatsapp media: Media URL not found in metadata response"
//	"whatsapp media: Failed to download media binary: <status>"
//	"whatsapp media: Permanently failed to download media <id>. Last error: <err>" after retries.
func (d *MediaDownloader) Download(ctx context.Context, mediaID string) ([]byte, string, error) {
	if d.token == "" {
		return nil, "", errors.New("whatsapp media: WHATSAPP_ACCESS_TOKEN is not configured")
	}
	var lastErr error
	for attempt := 1; attempt <= MediaDownloadAttempts; attempt++ {
		data, mime, err := d.downloadOnce(ctx, mediaID)
		if err == nil {
			return data, mime, nil
		}
		lastErr = err
		slog.WarnContext(ctx, fmt.Sprintf("attempt %d failed to download media %s: %v", attempt, mediaID, err))
		if attempt < MediaDownloadAttempts {
			select {
			case <-ctx.Done():
				return nil, "", fmt.Errorf("download media %s cancelled: %w", mediaID, ctx.Err())
			case <-time.After(d.backoff(attempt)):
			}
		}
	}
	return nil, "", fmt.Errorf("whatsapp media: Permanently failed to download media %s. Last error: %w", mediaID, lastErr)
}

func (d *MediaDownloader) downloadOnce(ctx context.Context, mediaID string) ([]byte, string, error) {
	metadataURL := fmt.Sprintf("%s/%s/%s", d.base, d.version, mediaID)

	// Step 1: fetch media metadata to get the URL.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build metadata request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+d.token)
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("fetch media metadata: %w", err)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	_ = resp.Body.Close()
	if err != nil {
		return nil, "", fmt.Errorf("read media metadata: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("whatsapp media: Failed to fetch media metadata: %s", resp.Status)
	}
	var metadata struct {
		URL      string `json:"url"`
		MimeType string `json:"mime_type"`
	}
	if err := json.Unmarshal(body, &metadata); err != nil {
		return nil, "", fmt.Errorf("decode media metadata: %w", err)
	}
	if metadata.URL == "" {
		return nil, "", fmt.Errorf("whatsapp media: Media URL not found in metadata response")
	}

	// Step 2: download the binary data.
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, metadata.URL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build binary request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+d.token)
	resp, err = d.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("fetch media binary: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("whatsapp media: Failed to download media binary: %s", resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, "", fmt.Errorf("read media binary: %w", err)
	}
	mime := metadata.MimeType
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		mime = ct // the actual bytes' type wins over metadata
	}
	return data, mime, nil
}
