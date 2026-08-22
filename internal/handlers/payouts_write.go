package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

// Payout request ports PayoutsService.requestPayout
// (apps/mobile-api/src/payouts/payouts.service.ts:111-221) — T4.21, the MONEY
// PATH. Every amount is shopspring decimal end-to-end; the single minor-unit
// conversion goes through money.ToMinorUnits, never a float.
//
// Mount: chi r.Route("/payouts", func(r chi.Router) {
// MountPayoutsWrite(r, deps); r.Mount("/", handlers.NewPayouts(pool)) }).

// PaystackMover is the slice of the Paystack client the payout flow needs; a
// consumer-side interface so tests stub it without touching the integration
// package.
type PaystackMover interface {
	CreateTransferRecipient(ctx context.Context, req paystack.CreateRecipientRequest) (string, error)
	InitiateTransfer(ctx context.Context, req paystack.TransferRequest) (paystack.TransferResult, error)
}

// PayoutsWriteDeps carries the write endpoint's collaborators.
type PayoutsWriteDeps struct {
	Pool     *pgxpool.Pool
	Paystack PaystackMover
}

// MountPayoutsWrite registers POST /payouts/request on r.
func MountPayoutsWrite(r chi.Router, d PayoutsWriteDeps) {
	r.Post("/request", payoutsRequest(d.Pool, d.Paystack))
}

// wo_moneyEpsilon ports MONEY_EPSILON (payouts.service.ts:27): money
// comparison tolerance as an exact decimal (0.001).
var wo_moneyEpsilon = decimal.New(1, -3)

// wo_transferReason is initiateTransfer's default reason
// (paystack.provider.ts:248).
const wo_transferReason = "Vendor Payout"

// wo_newID mints a UUID v4 — the Go stand-in for Prisma @default(uuid()).
func wo_newID() string { return uuid.NewString() }

// wo_newReference ports reference generation (payouts.service.ts:120):
// `payout_${randomUUID()}`.
func wo_newReference() string { return "payout_" + uuid.NewString() }

// payoutsRequest implements the two-phase source flow:
//
//	Phase 1 (payouts.service.ts:129-152): pg_advisory_xact_lock(hashtext(
//	'payout:<businessId>')) serialises concurrent requests for the business;
//	inside the lock the balance is recomputed (SUCCESS payments - SUCCESS+
//	PENDING payouts, floored 0) and, only if it covers dto.amount (+0.001
//	epsilon), a PENDING payout reserves those funds.
//
//	Phase 2 (:154-220): recipient code from the businesses.paystack_recipient_
//	code cache or createTransferRecipient(type momo) + cache write-back; then
//	initiateTransfer with MINOR units; payout becomes SUCCESS iff provider
//	status === 'success' else stays PENDING; any failure marks the payout
//	FAILED (releasing the reservation) and answers BadRequestException with
//	the source's exact messages.
func payoutsRequest(pool *pgxpool.Pool, mover PaystackMover) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		dto, ok := wo_decodePayoutDTO(w, r)
		if !ok {
			return
		}

		business, err := q.GetBusinessForPayout(r.Context(), bizID)
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest, "Business not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		payout, err := wo_reservePayout(r.Context(), pool, bizID, business.Currency, dto.amount)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		final, err := wo_executeTransfer(r.Context(), q, mover, bizID,
			business.PaystackRecipientCode.String, business.Currency, dto, payout)
		if err != nil {
			// Release the reservation: FAILED stops counting against balance
			// (payouts.service.ts:199-210); update failures are logged and
			// swallowed like the source's .catch(logger.error).
			if _, updErr := q.MarkPayoutFailed(r.Context(), payout.ID); updErr != nil {
				slog.Error(fmt.Sprintf("Failed to mark payout %s as FAILED after transfer error: %v", payout.ID, updErr))
			}
			slog.Error(fmt.Sprintf("Payout %s (%s) failed for business %s: %v",
				payout.ID, payout.Reference.String, bizID, err))

			var he *httpx.HTTPException
			if errors.As(err, &he) {
				httpx.WriteError(w, r, err)
				return
			}
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest,
				"Payout could not be completed. Please try again."))
			return
		}

		_ = httpx.WriteJSON(w, http.StatusCreated, wo_payoutJSON(final))
	}
}

// wo_payoutDTO is RequestPayoutSchema (dto/request-payout.dto.ts): amount a
// positive number, accountNumber/bankCode/accountName strings min(1).
type wo_payoutDTO struct {
	amount        decimal.Decimal
	accountNumber string
	bankCode      string
	accountName   string
}

// wo_decodePayoutDTO validates the body against RequestPayoutSchema and emits
// zod issues in schema field order (amount, accountNumber, bankCode,
// accountName). amount is carried as json.Number -> decimal: no float ever
// touches the value.
func wo_decodePayoutDTO(w http.ResponseWriter, r *http.Request) (wo_payoutDTO, bool) {
	raw := map[string]json.RawMessage{}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		httpx.WriteZodValidationError(w, []httpx.FieldIssue{{
			Code: "invalid_type", Path: "", Message: "Expected object",
		}})
		return wo_payoutDTO{}, false
	}

	var dto wo_payoutDTO
	var issues []httpx.FieldIssue

	if v, present := raw["amount"]; !present {
		issues = append(issues, httpx.FieldIssue{Code: "invalid_type", Path: "amount", Message: "Required"})
	} else if !wo_isJSONNumber(v) {
		issues = append(issues, httpx.FieldIssue{Code: "invalid_type", Path: "amount",
			Message: "Expected number, received " + wo_jsonTypeName(v)})
	} else {
		d, err := decimal.NewFromString(string(v))
		switch {
		case err != nil:
			issues = append(issues, httpx.FieldIssue{Code: "invalid_type", Path: "amount",
				Message: "Expected number, received number"})
		case !d.IsPositive():
			issues = append(issues, httpx.FieldIssue{Code: "too_small", Path: "amount",
				Message: "Number must be greater than 0"})
		default:
			dto.amount = d
		}
	}

	for _, f := range []struct {
		key string
		dst *string
	}{
		{"accountNumber", &dto.accountNumber},
		{"bankCode", &dto.bankCode},
		{"accountName", &dto.accountName},
	} {
		v, present := raw[f.key]
		if !present {
			issues = append(issues, httpx.FieldIssue{Code: "invalid_type", Path: f.key, Message: "Required"})
			continue
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			issues = append(issues, httpx.FieldIssue{Code: "invalid_type", Path: f.key,
				Message: "Expected string, received " + wo_jsonTypeName(v)})
			continue
		}
		if len(s) < 1 {
			issues = append(issues, httpx.FieldIssue{Code: "too_small", Path: f.key,
				Message: "String must contain at least 1 character(s)"})
			continue
		}
		*f.dst = s
	}

	if len(issues) > 0 {
		httpx.WriteZodValidationError(w, issues)
		return wo_payoutDTO{}, false
	}
	return dto, true
}

// wo_isJSONNumber reports whether the raw JSON value is a number token
// (json.Decoder never produces NaN/Infinity, so this mirrors z.number()).
func wo_isJSONNumber(v json.RawMessage) bool { return wo_jsonTypeName(v) == "number" }

// wo_jsonTypeName names a raw JSON value's type the way zod words received
// types ("string"/"number"/"boolean"/"object"/"array"/"null"; absent keys are
// handled before this point).
func wo_jsonTypeName(v json.RawMessage) string {
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case ' ', '\t', '\n', '\r':
			continue
		case '"':
			return "string"
		case '{':
			return "object"
		case '[':
			return "array"
		case 't', 'f':
			return "boolean"
		case 'n':
			return "null"
		default:
			return "number"
		}
	}
	return "undefined"
}

// wo_reservePayout runs phase 1 inside one transaction: advisory lock ->
// balance read -> guard -> PENDING insert. Insufficient balance answers the
// exact BadRequestException message of payouts.service.ts:138-140.
func wo_reservePayout(ctx context.Context, pool *pgxpool.Pool, bizID, currency string, amount decimal.Decimal) (gen.Payout, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return gen.Payout{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := gen.New(tx)
	if err := qtx.LockPayoutBalanceKey(ctx, "payout:"+bizID); err != nil {
		return gen.Payout{}, err
	}

	revenue, err := qtx.SumSuccessfulPaymentAmounts(ctx, bizID)
	if err != nil {
		return gen.Payout{}, err
	}
	committed, err := qtx.SumCommittedPayoutAmounts(ctx, bizID)
	if err != nil {
		return gen.Payout{}, err
	}
	available := revenue.Sub(committed)
	if available.IsNegative() {
		available = decimal.Zero // Math.max(0, ...)
	}

	if amount.GreaterThan(available.Add(wo_moneyEpsilon)) {
		return gen.Payout{}, httpx.NewHTTPException(http.StatusBadRequest,
			fmt.Sprintf("Insufficient balance. Available: %s %s, requested: %s %s",
				currency, available.StringFixed(2), currency, amount.StringFixed(2)))
	}

	payout, err := qtx.InsertPendingPayout(ctx, gen.InsertPendingPayoutParams{
		ID:         wo_newID(),
		BusinessID: bizID,
		Amount:     amount,
		Currency:   currency,
		Reference:  pgtype.Text{String: wo_newReference(), Valid: true},
	})
	if err != nil {
		return gen.Payout{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return gen.Payout{}, err
	}
	return payout, nil
}

// wo_executeTransfer runs phase 2 outside the reservation transaction and
// returns the final payout row. Errors are either *httpx.HTTPException (the
// source's two explicit BadRequestExceptions) or generic failures that the
// caller folds into 'Payout could not be completed. Please try again.'.
func wo_executeTransfer(ctx context.Context, q *gen.Queries, mover PaystackMover,
	bizID, cachedRecipientCode, currency string, dto wo_payoutDTO, payout gen.Payout,
) (gen.Payout, error) {
	recipientCode := cachedRecipientCode
	if recipientCode == "" {
		// momo recipient fields straight from the request + business currency
		// (payouts.service.ts:161-166).
		code, err := mover.CreateTransferRecipient(ctx, paystack.CreateRecipientRequest{
			Name:          dto.accountName,
			AccountNumber: dto.accountNumber,
			BankCode:      dto.bankCode,
			Currency:      currency,
		})
		if err != nil || code == "" {
			return payout, httpx.NewHTTPException(http.StatusBadRequest,
				"Failed to create transfer recipient with provided bank details")
		}
		recipientCode = code

		if _, err := q.SetPaystackRecipientCode(ctx, gen.SetPaystackRecipientCodeParams{
			PaystackRecipientCode: pgtype.Text{String: recipientCode, Valid: true},
			ID:                    bizID,
		}); err != nil {
			return payout, err
		}
	}

	result, err := mover.InitiateTransfer(ctx, paystack.TransferRequest{
		AmountMinor: money.ToMinorUnits(dto.amount), // 80 GHS -> 8000 pesewas
		Recipient:   recipientCode,
		Reason:      wo_transferReason,
		Currency:    currency,
		Reference:   payout.Reference.String,
	})
	if err != nil {
		return payout, httpx.NewHTTPException(http.StatusBadRequest,
			"Failed to initiate transfer via Paystack")
	}

	status := gen.PaymentStatusPENDING
	if result.Status == "success" {
		status = gen.PaymentStatusSUCCESS
	}
	return q.CompletePayout(ctx, gen.CompletePayoutParams{
		PaystackTransferID: pgtype.Text{String: result.TransferCode, Valid: true},
		Status:             status,
		ID:                 payout.ID,
	})
}

// wo_payoutJSON renders the Prisma Payout payload key-for-key (same shape as
// GET /payouts/history rows).
func wo_payoutJSON(p gen.Payout) payoutJSON {
	return payoutJSON{
		ID:                 p.ID,
		BusinessID:         p.BusinessID,
		Amount:             ep_num(p.Amount),
		Currency:           p.Currency,
		Status:             string(p.Status),
		Reference:          ep_text(p.Reference),
		PaystackTransferID: ep_text(p.PaystackTransferID),
		CreatedAt:          epISO(p.CreatedAt.Time),
		UpdatedAt:          epISO(p.UpdatedAt.Time),
	}
}
