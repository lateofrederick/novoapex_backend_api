// Package workers hosts the asynq task handlers. This file is the Stage 7B
// CRM materialiser (T7.9): the port of
// libs/queue/src/processors/crm-materialiser.processor.ts — a deterministic,
// LLM-free consumer that turns per-turn CRM signals into durable records.
//
// Flow (processor comment, crm-materialiser.processor.ts:19-23):
//  1. always update profile (accumulate preferences, sentiment, delivery area)
//  2. if order confirmed → create Order + OrderItems (+ invoice + follow-ups)
//  3. if customer name detected → update Customer name
//
// Core logic lives in pure funcs (HandleCRMSignals and the crm* handlers in
// crm_handlers.go) so tests exercise them directly without asynq.
package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// CRMDeps carries everything the materialiser touches. Pool is the real
// database; Publisher fans out to the outbound queue; Paystack initiates
// checkout links. All three are interface/fake-friendly for tests.
type CRMDeps struct {
	Pool      *pgxpool.Pool
	Publisher queue.Publisher
	Paystack  PaystackInitiator
}

// PaystackInitiator mirrors PaymentProvider.initiatePayment
// (libs/common/src/payments/payment-provider.interface.ts:122) — the single
// capability the order ledger needs from the payment factory.
type PaystackInitiator interface {
	InitiatePayment(ctx context.Context, req PaymentRequest) (PaymentLink, error)
}

// PaymentRequest mirrors InitiatePaymentParams
// (payment-provider.interface.ts:45-69). Amount stays in MAJOR units here;
// converting to provider minor units (Math.round(amount * 100)) belongs to the
// Paystack adapter, exactly like the source.
type PaymentRequest struct {
	Amount        decimal.Decimal // major currency units
	Currency      string
	CustomerPhone string
	Reference     string
	CallbackURL   string
	BusinessID    string
}

// PaymentLink mirrors InitiatePaymentResult
// (payment-provider.interface.ts:74-83).
type PaymentLink struct {
	ProviderReference string
	PaymentURL        string
	Status            string // "initiated" | "failed"
}

// CRMSignalJob mirrors CrmSignalJobData
// (libs/queue/src/schemas/crm-signal.schema.ts) with the same wire keys:
// camelCase at the top level, snake_case inside crmSignals.
type CRMSignalJob struct {
	BusinessID           string     `json:"businessId"`
	CustomerID           string     `json:"customerId"`
	ConversationID       string     `json:"conversationId"`
	CustomerPhone        string     `json:"customerPhone,omitempty"`
	SourceMessageID      string     `json:"sourceMessageId,omitempty"` // deterministic idempotency key
	CRMSignals           CrmSignals `json:"crmSignals"`
	LLMIntent            string     `json:"llmIntent"`
	ReferencedProductIDs []string   `json:"referencedProductIds"`
	Timestamp            string     `json:"timestamp"`
}

// DetectedItem is one crm_signals.detected_items entry.
type DetectedItem struct {
	ProductID string `json:"product_id"`
	Quantity  int32  `json:"quantity"`
}

// CrmSignals mirrors CrmSignalsSchema
// (libs/orchestrator/src/llm/structured-output.ts:3-23).
type CrmSignals struct {
	OrderConfirmed      bool           `json:"order_confirmed"`
	DetectedItems       []DetectedItem `json:"detected_items"`
	DeliveryArea        *string        `json:"delivery_area"`
	CustomerName        *string        `json:"customer_name"`
	DetectedPreferences []string       `json:"detected_preferences"`
	Sentiment           *string        `json:"sentiment"`
}

// RegisterCRM attaches the crm-materialiser handler to the worker registrar
// (T7.9). The wrapper only decodes; HandleCRMSignals carries the logic so
// tests bypass asynq entirely.
func RegisterCRM(reg queue.Registrar, deps CRMDeps) {
	reg.Register(queue.TaskCRMProcess, func(ctx context.Context, payload []byte) error {
		var job CRMSignalJob
		if err := json.Unmarshal(payload, &job); err != nil {
			return fmt.Errorf("crm materialiser: decode job payload: %w", err)
		}
		return HandleCRMSignals(ctx, deps, job)
	})
}

// HandleCRMSignals runs one CRM signal turn through the three stages in the
// processor's order, naming the failed stage exactly like runStage
// (crm-materialiser.processor.ts:59-77) before propagating.
func HandleCRMSignals(ctx context.Context, deps CRMDeps, job CRMSignalJob) error {
	slog.Info("Processing CRM signals",
		"event", "crm_materialiser_started",
		"customerId", job.CustomerID,
		"conversationId", job.ConversationID,
		"orderConfirmed", job.CRMSignals.OrderConfirmed,
		"itemCount", len(job.CRMSignals.DetectedItems),
		"preferencesCount", len(job.CRMSignals.DetectedPreferences),
	)

	// 1. Always update profile (preferences, delivery area, sentiment)
	if err := runCRMStage("profile_builder", func() error {
		return crmHandleProfileBuilder(ctx, deps, job)
	}); err != nil {
		return err
	}

	// 2. If order confirmed with items, create order
	if job.CRMSignals.OrderConfirmed && len(job.CRMSignals.DetectedItems) > 0 {
		if err := runCRMStage("order_ledger", func() error {
			return crmHandleOrderLedger(ctx, deps, job)
		}); err != nil {
			return err
		}
	}

	// 3. If customer name detected, update it
	if job.CRMSignals.CustomerName != nil {
		if err := runCRMStage("customer_capture", func() error {
			return crmHandleCustomerCapture(ctx, deps, job.CustomerID, *job.CRMSignals.CustomerName)
		}); err != nil {
			return err
		}
	}

	slog.Info("CRM signals processed",
		"event", "crm_materialiser_completed", "customerId", job.CustomerID)
	return nil
}

// runCRMStage wraps a stage so a thrown error names the stage that broke
// before it propagating (crm-materialiser.processor.ts:56-77). Without this,
// a failure anywhere in the chain surfaced only as an opaque job failure.
func runCRMStage(stage string, fn func() error) error {
	if err := fn(); err != nil {
		slog.Error(fmt.Sprintf("CRM materialiser stage failed: %s", stage),
			"err", err,
			"event", "crm_materialiser_stage_failed",
			"stage", stage,
		)
		return fmt.Errorf("crm materialiser: stage %s failed: %w", stage, err)
	}
	return nil
}
