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
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/money"
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

// VerifyWebhookSignature ports PaystackProvider.verifyWebhookSignature
// (paystack.provider.ts:41-61): HMAC-SHA512 hex digest of the RAW body under
// the secret key, compared constant-time against the x-paystack-signature
// header value. An empty secret always fails (the source logs
// "PAYSTACK_SECRET_KEY is not configured — cannot verify webhook" and returns
// false — logging stays at the adapter layer so this stays pure).
func VerifyWebhookSignature(secret string, body []byte, signature string) bool {
	if strings.TrimSpace(secret) == "" {
		return false
	}
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	// timingSafeEqual requires equal-length buffers; the source pre-checks
	// (paystack.provider.ts:57-59) and so do we.
	if len(expected) != len(signature) {
		return false
	}
	return hmac.Equal([]byte(expected), []byte(signature))
}

// InitiatePaymentRequest carries the /transaction/initialize parameters,
// mirroring InitiatePaymentParams. AmountMajor is MAJOR units as an exact
// decimal; the single minor-unit conversion happens below via
// money.ToMinorUnits (never a float), replacing the source's
// Math.round(params.amount * 100).
type InitiatePaymentRequest struct {
	AmountMajor   decimal.Decimal
	Currency      string
	CustomerEmail string // optional; falls back to <digits(customerPhone)>@customers.novoapex.com
	CustomerPhone string
	Reference     string
	BusinessID    string
	CallbackURL   string
}

// InitiatePaymentResult mirrors InitiatePaymentResult: failures are reported
// as Status "failed", NOT as errors (the source never throws out of
// initiatePayment — paystack.provider.ts:144-196).
type InitiatePaymentResult struct {
	Status            string // "initiated" | "failed"
	PaymentURL        string // data.authorization_url
	ProviderReference string // data.reference ?? params.reference
}

const customerEmailFallbackDomain = "@customers.novoapex.com"

// InitiatePayment POSTs ${BaseURL}/transaction/initialize to create a pending
// transaction and returns the authorization URL the customer completes
// payment at (paystack.provider.ts:144-196). Wire contract:
//
//	body {amount:<minor units>, currency, email, reference, callback_url,
//	      metadata:{customer_phone, businessId}}
//	ok  -> data{authorization_url, access_code, reference}, status "initiated"
//	any API/transport/secret failure -> {status:"failed"}, nil error
func (c *Client) InitiatePayment(ctx context.Context, req InitiatePaymentRequest) (InitiatePaymentResult, error) {
	if strings.TrimSpace(c.cfg.SecretKey) == "" {
		slog.Error("PAYSTACK_SECRET_KEY is not configured — cannot initiate payment")
		return InitiatePaymentResult{Status: "failed"}, nil
	}

	email := req.CustomerEmail
	if email == "" {
		// Paystack REQUIRES an email: non-personal per-customer placeholder on
		// a non-delivering subdomain (paystack.provider.ts:164-168).
		digits := strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' {
				return r
			}
			return -1
		}, req.CustomerPhone)
		if digits == "" {
			digits = "unknown"
		}
		email = digits + customerEmailFallbackDomain
	}

	var out struct {
		AuthorizationURL string `json:"authorization_url"`
		AccessCode       string `json:"access_code"`
		Reference        string `json:"reference"`
	}
	err := c.do(ctx, "/transaction/initialize", wireInitialize{
		Amount:      money.ToMinorUnits(req.AmountMajor),
		Currency:    req.Currency,
		Email:       email,
		Reference:   req.Reference,
		CallbackURL: req.CallbackURL,
		Metadata: wireInitMetadata{
			CustomerPhone: req.CustomerPhone,
			BusinessID:    req.BusinessID,
		},
	}, &out)
	if err != nil {
		slog.Error(fmt.Sprintf("Paystack initiation failed: %v", err))
		return InitiatePaymentResult{Status: "failed"}, nil
	}

	ref := out.Reference
	if ref == "" {
		ref = req.Reference
	}
	return InitiatePaymentResult{
		Status:            "initiated",
		PaymentURL:        out.AuthorizationURL,
		ProviderReference: ref,
	}, nil
}

// wireInitialize preserves the source body shape exactly; callback_url and
// both metadata keys are ALWAYS serialised (even empty), matching
// JSON.stringify of paystack.provider.ts:160-177.
type wireInitialize struct {
	Amount      int64            `json:"amount"`
	Currency    string           `json:"currency"`
	Email       string           `json:"email"`
	Reference   string           `json:"reference"`
	CallbackURL string           `json:"callback_url"`
	Metadata    wireInitMetadata `json:"metadata"`
}

type wireInitMetadata struct {
	CustomerPhone string `json:"customer_phone"`
	BusinessID    string `json:"businessId"`
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
