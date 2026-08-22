// Package paystack ports the transfer half of
// libs/common/src/payments/providers/paystack.provider.ts (T4.21): the
// createTransferRecipient + initiateTransfer calls the payout money path
// depends on. Webhook signature verification and transaction/initialize stay
// with Stage 5 (T5.12-T5.15).
//
// Wire contract (paystack.provider.ts:213-238 and :256-284):
//   - auth header "Authorization: Bearer <secret>"
//   - POST /transferrecipient body {type:"momo", name, account_number,
//     bank_code, currency} -> data.recipient_code
//   - POST /transfer body {source:"balance", amount:<minor units>, recipient,
//     reason, reference} -> {data.status, data.transfer_code}
//   - every response is the envelope {status, message, data}; a call counts as
//     successful only when HTTP ok AND status===true, exactly the provider's
//     `if (!response.ok || !data.status)` guard.
//
// The Node provider signals failure by returning null; this client returns
// typed errors instead so handlers can log the cause while mapping to the
// same BadRequestException messages (see payouts_write.go). Currency is part
// of TransferRequest for call-site symmetry with createTransferRecipient, but
// is deliberately NOT serialised into /transfer — the source never sends it.
package paystack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultBaseURL is the production Paystack API root; override via Config for
// tests (httptest stubs) — the source reads PAYSTACK_BASE_URL the same way.
const DefaultBaseURL = "https://api.paystack.co"

const defaultRecipientType = "momo" // paystack.provider.ts:220 ("could be 'nuban'")

// Config configures a Client.
type Config struct {
	SecretKey string
	BaseURL   string // optional; empty -> DefaultBaseURL
}

// Client is the Paystack HTTP client (transfer endpoints only).
type Client struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	return &Client{cfg: cfg, http: &http.Client{Timeout: 30 * time.Second}}
}

// ErrMissingSecretKey mirrors the provider's null-return when
// PAYSTACK_SECRET_KEY is unset (paystack.provider.ts:210, :253).
var ErrMissingSecretKey = errors.New("paystack: secret key not configured")

// APIError is a typed Paystack failure: non-2xx HTTP status or a false
// envelope status field. Message carries Paystack's own message string.
type APIError struct {
	Endpoint   string // e.g. "/transfer"
	HTTPStatus int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("paystack: %s failed: http=%d message=%q", e.Endpoint, e.HTTPStatus, e.Message)
}

// CreateRecipientRequest carries the momo recipient fields; Type defaults to
// "momo" when empty.
type CreateRecipientRequest struct {
	Name          string
	AccountNumber string
	BankCode      string
	Currency      string
	Type          string
}

type wireCreateRecipient struct {
	Type          string `json:"type"`
	Name          string `json:"name"`
	AccountNumber string `json:"account_number"`
	BankCode      string `json:"bank_code"`
	Currency      string `json:"currency"`
}

// CreateTransferRecipient POSTs /transferrecipient and returns
// data.recipient_code. An empty code with a success envelope is reported as
// an error (the source's falsy-code check at payouts.service.ts:168).
func (c *Client) CreateTransferRecipient(ctx context.Context, req CreateRecipientRequest) (string, error) {
	typ := req.Type
	if typ == "" {
		typ = defaultRecipientType
	}
	var out struct {
		RecipientCode string `json:"recipient_code"`
	}
	if err := c.do(ctx, "/transferrecipient", wireCreateRecipient{
		Type:          typ,
		Name:          req.Name,
		AccountNumber: req.AccountNumber,
		BankCode:      req.BankCode,
		Currency:      req.Currency,
	}, &out); err != nil {
		return "", err
	}
	return out.RecipientCode, nil
}

// TransferRequest is an initiated payout transfer. AmountMinor is already in
// the minor currency unit (pesewas for GHS); callers convert major -> minor
// with exact decimal math (internal/money.ToMinorUnits), never floats.
type TransferRequest struct {
	AmountMinor int64
	Recipient   string
	Reason      string
	Currency    string // reserved; NOT sent — see package comment
	Reference   string
}

type wireTransfer struct {
	Source    string `json:"source"`
	Amount    int64  `json:"amount"`
	Recipient string `json:"recipient"`
	Reason    string `json:"reason"`
	Reference string `json:"reference"`
}

// TransferResult maps {data.status, data.transfer_code}; status is the
// provider-side transfer state ("pending"/"success"/...), which the handler
// folds into the Payout row's PaymentStatus.
type TransferResult struct {
	Status       string
	TransferCode string
}

// InitiateTransfer POSTs /transfer with source "balance".
func (c *Client) InitiateTransfer(ctx context.Context, req TransferRequest) (TransferResult, error) {
	var out struct {
		Status       string `json:"status"`
		TransferCode string `json:"transfer_code"`
	}
	if err := c.do(ctx, "/transfer", wireTransfer{
		Source:    "balance",
		Amount:    req.AmountMinor,
		Recipient: req.Recipient,
		Reason:    req.Reason,
		Reference: req.Reference,
	}, &out); err != nil {
		return TransferResult{}, err
	}
	return TransferResult{Status: out.Status, TransferCode: out.TransferCode}, nil
}

// envelope is the universal Paystack response shape {status, message, data}.
type envelope struct {
	Status  bool            `json:"status"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (c *Client) do(ctx context.Context, endpoint string, reqBody any, out any) error {
	if strings.TrimSpace(c.cfg.SecretKey) == "" {
		return ErrMissingSecretKey
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(reqBody); err != nil {
		return fmt.Errorf("paystack: encode %s request: %w", endpoint, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.cfg.BaseURL, "/")+endpoint, &buf)
	if err != nil {
		return fmt.Errorf("paystack: build %s request: %w", endpoint, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.SecretKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("paystack: %s transport: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("paystack: read %s response: %w", endpoint, err)
	}

	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return &APIError{
			Endpoint:   endpoint,
			HTTPStatus: resp.StatusCode,
			Message:    fmt.Sprintf("invalid JSON response (%d bytes)", len(body)),
		}
	}

	// The provider's success gate: !response.ok || !data.status -> failure
	// (paystack.provider.ts:229, :272).
	if resp.StatusCode < 200 || resp.StatusCode > 299 || !env.Status {
		return &APIError{Endpoint: endpoint, HTTPStatus: resp.StatusCode, Message: env.Message}
	}

	if len(env.Data) > 0 && out != nil {
		if err := json.Unmarshal(env.Data, out); err != nil {
			return fmt.Errorf("paystack: decode %s data: %w", endpoint, err)
		}
	}
	return nil
}
