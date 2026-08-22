// Package cloudinary ports libs/common/src/storage/providers/cloudinary.provider.ts
// (T4.9) onto Cloudinary's plain REST API: upload_stream becomes a multipart
// POST /v1_1/{cloud}/image/upload and uploader.destroy a form POST to
// /v1_1/{cloud}/image/destroy, both signed the way the JS SDK signs —
// sha1 hex of the alphabetically sorted parameter string (excluding file,
// api_key, resource_type, cloud_name) suffixed with the API secret.
package cloudinary

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.cloudinary.com/v1_1"

// Options carries the CLOUDINARY_* env contract. BaseURL is overridable so
// tests can point the provider at a local stub server; Now allows freezing
// the signing timestamp in tests.
type Options struct {
	CloudName string
	APIKey    string
	APISecret string

	BaseURL string
	Now     func() time.Time
}

// Provider implements storage.StorageProvider without importing it (the
// interface's (url, publicID) returns keep the dependency one-way).
type Provider struct {
	cloudName string
	apiKey    string
	apiSecret string
	baseURL   string
	now       func() time.Time
	hc        *http.Client
}

func New(opts Options) *Provider {
	if opts.BaseURL == "" {
		opts.BaseURL = defaultBaseURL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Provider{
		cloudName: opts.CloudName,
		apiKey:    opts.APIKey,
		apiSecret: opts.APISecret,
		baseURL:   strings.TrimRight(opts.BaseURL, "/"),
		now:       opts.Now,
		hc:        &http.Client{},
	}
}

// ProviderName mirrors readonly providerName = 'cloudinary'.
func (p *Provider) ProviderName() string { return "cloudinary" }

// UploadImage ports upload_stream({folder}) -> {secure_url, public_id}.
func (p *Provider) UploadImage(ctx context.Context, file []byte, folder string) (string, string, error) {
	timestamp := strconv.FormatInt(p.now().Unix(), 10)
	signature := p.sign("folder=" + folder + "&timestamp=" + timestamp)

	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	_ = mw.WriteField("api_key", p.apiKey)
	_ = mw.WriteField("timestamp", timestamp)
	if folder != "" {
		_ = mw.WriteField("folder", folder)
	}
	_ = mw.WriteField("signature", signature)
	fw, err := mw.CreateFormFile("file", "upload")
	if err != nil {
		return "", "", fmt.Errorf("cloudinary: %w", err)
	}
	if _, err := fw.Write(file); err != nil {
		return "", "", fmt.Errorf("cloudinary: %w", err)
	}
	if err := mw.Close(); err != nil {
		return "", "", fmt.Errorf("cloudinary: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/"+p.cloudName+"/image/upload", body)
	if err != nil {
		return "", "", fmt.Errorf("cloudinary: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	var out struct {
		SecureURL string `json:"secure_url"`
		PublicID  string `json:"public_id"`
		Error     struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := p.do(req, &out); err != nil {
		return "", "", err
	}
	if out.Error.Message != "" {
		return "", "", errors.New("cloudinary: upload failed: " + out.Error.Message)
	}
	slog.DebugContext(ctx, "uploaded image to Cloudinary folder: "+folder)
	return out.SecureURL, out.PublicID, nil
}

// DeleteImage ports uploader.destroy(storageKey). Cloudinary answers
// {result:'not found'} for an already-deleted asset; both that and any
// transport failure are logged and swallowed — cleanup must never fail the
// caller's request (provider contract).
func (p *Provider) DeleteImage(ctx context.Context, publicID string) error {
	timestamp := strconv.FormatInt(p.now().Unix(), 10)
	signature := p.sign("public_id=" + publicID + "&timestamp=" + timestamp)

	form := url.Values{}
	form.Set("api_key", p.apiKey)
	form.Set("timestamp", timestamp)
	form.Set("public_id", publicID)
	form.Set("signature", signature)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/"+p.cloudName+"/image/destroy", strings.NewReader(form.Encode()))
	if err != nil {
		return nil // swallowed by contract
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var out struct {
		Result string `json:"result"`
	}
	if err := p.do(req, &out); err != nil {
		slog.WarnContext(ctx, "Failed to delete Cloudinary asset '"+publicID+"' (non-blocking): "+err.Error())
		return nil
	}
	if out.Result != "ok" {
		slog.WarnContext(ctx, fmt.Sprintf(
			"Cloudinary delete for '%s' returned '%s' (asset may already be gone).", publicID, out.Result))
		return nil
	}
	slog.DebugContext(ctx, "Deleted Cloudinary asset: "+publicID)
	return nil
}

// sign reproduces the SDK's signature: sha1(sortedParams + api_secret) hex.
func (p *Provider) sign(payload string) string {
	sum := sha1.Sum([]byte(payload + p.apiSecret))
	return hex.EncodeToString(sum[:])
}

func (p *Provider) do(req *http.Request, out any) error {
	resp, err := p.hc.Do(req)
	if err != nil {
		return fmt.Errorf("cloudinary request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("cloudinary response read failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("cloudinary responded %d: %s", resp.StatusCode, bytes.TrimSpace(raw))
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("cloudinary response decode failed: %w", err)
	}
	return nil
}
