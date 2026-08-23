package paystack

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// NormalisedPaymentEvent mirrors PaystackProvider.parseWebhookEvent output
// (apps/api/src/payments): the provider-agnostic shape consumed by the
// payment-events worker (Stage 7 T7.1–T7.4).
//
// Field-mapping deltas vs the Node object are documented on each field and
// in ParseWebhookEvent; the declared Go contract is authoritative here:
//   - AmountMinor keeps provider NATIVE minor units — the /100 major-unit
//     division of paystack.provider.ts:100 happens once, in the worker, via
//     money.FromMinorUnits (no float path anywhere).
//   - customerPhone is NOT part of the declared contract; the worker extracts
//     data.customer.phone from RawJSON verbatim (workers.customerPhoneFromRaw).
type NormalisedPaymentEvent struct {
	Provider          string    // "paystack"
	Event             string    // raw event name, e.g. charge.success
	Reference         string    // data.reference
	AmountMinor       int64     // data.amount — provider native minor units
	Currency          string    // data.currency
	Status            string    // normalised from EVENT NAME (success/failed/pending) — data.status is ignored, exactly like the source
	PaidAt            time.Time // data.paid_at (RFC3339); zero when absent/unparseable
	BusinessID        string    // data.metadata.businessId (may be empty)
	CustomerEmail     string
	AuthorizationBank string // data.authorization.bank -> Payment.network
	RawJSON           []byte // full original body, preserved verbatim
}

// ErrMissingDataField reproduces the source error thrown when the payload
// has no "data" object (paystack.provider.ts:79):
//
//	new Error('Paystack webhook payload missing "data" field')
var ErrMissingDataField = errors.New(`Paystack webhook payload missing "data" field`) //nolint:staticcheck // verbatim paystack.provider.ts:79 error text

// ErrParseNotImplemented is retained for the Stage 4 declaration surface
// (workers tests reference it as the pre-Stage-5 sentinel). The parser below
// is implemented and never returns it.
var ErrParseNotImplemented = errors.New("paystack: ParseWebhookEvent pending Stage 5 implementation")

// webhookWire is the envelope Paystack signs: {event, data{...}}.
type webhookWire struct {
	Event string       `json:"event"`
	Data  *webhookData `json:"data"`
}

type webhookData struct {
	Reference string      `json:"reference"`
	Amount    json.Number `json:"amount"`
	Currency  string      `json:"currency"`
	Status    string      `json:"status"` // present on the wire but NOT used for mapping (source derives status from the event name)
	PaidAt    string      `json:"paid_at"`

	Customer struct {
		Email string `json:"email"`
	} `json:"customer"`

	Authorization struct {
		Bank string `json:"bank"`
	} `json:"authorization"`

	Metadata struct {
		BusinessID string `json:"businessId"`
	} `json:"metadata"`
}

// ParseWebhookEvent validates and maps a raw Paystack webhook body into
// NormalisedPaymentEvent, porting parseWebhookEvent
// (libs/common/src/payments/providers/paystack.provider.ts:74-134):
//
//   - missing "data" -> ErrMissingDataField with the source's exact message;
//   - status is derived from the EVENT NAME only:
//     charge.success -> "success", charge.failed -> "failed", else "pending";
//   - amount stays in provider MINOR units (see struct comment);
//   - currency defaults to "GHS" and is upper-cased;
//   - paid_at parses as RFC3339 ("2026-08-22T09:30:00.000Z"); an absent or
//     unparseable value leaves PaidAt zero (the JS Invalid-Date case);
//   - RawJSON carries the original bytes VERBATIM — never re-marshalled.
func ParseWebhookEvent(body []byte) (NormalisedPaymentEvent, error) {
	var wire webhookWire
	if err := json.Unmarshal(body, &wire); err != nil {
		return NormalisedPaymentEvent{RawJSON: body}, fmt.Errorf("paystack: decode webhook body: %w", err)
	}
	if wire.Data == nil {
		return NormalisedPaymentEvent{RawJSON: body}, ErrMissingDataField
	}

	status := "pending" // default branch of the source switch
	switch wire.Event {
	case "charge.success":
		status = "success"
	case "charge.failed":
		status = "failed"
	}

	minor := int64(0)
	if s := wire.Data.Amount.String(); s != "" {
		if d, err := decimal.NewFromString(s); err == nil {
			// Paystack minor-unit amounts are integers per API spec;
			// IntPart truncates any exotic fractional payload.
			minor = d.IntPart()
		}
	}

	currency := strings.ToUpper(wire.Data.Currency)
	if currency == "" {
		currency = "GHS" // ((data.currency as string) ?? 'GHS').toUpperCase()
	}

	var paidAt time.Time
	if s := wire.Data.PaidAt; s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			paidAt = t
		}
	}

	return NormalisedPaymentEvent{
		Provider:          "paystack",
		Event:             wire.Event,
		Reference:         wire.Data.Reference,
		AmountMinor:       minor,
		Currency:          currency,
		Status:            status,
		PaidAt:            paidAt,
		BusinessID:        wire.Data.Metadata.BusinessID,
		CustomerEmail:     wire.Data.Customer.Email,
		AuthorizationBank: wire.Data.Authorization.Bank,
		RawJSON:           body,
	}, nil
}
