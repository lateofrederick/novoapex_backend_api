// Package queue defines the asynq-backed job substrate contracts shared by
// producers (api process) and consumers (worker process). Implementation
// lands with Stage 6 (T6.2–T6.7); producers code against Publisher only.
package queue

import (
	"context"
	"time"
)

// Queue names — must match the BullMQ queues they replace (plan §B.2).
const (
	QWebhookProcessing = "webhook-processing"
	QOrchestrator      = "orchestrator-queue"
	QOutbound          = "outbound-queue"
	QCRMMaterialiser   = "crm-materialiser"
	QPaymentEvents     = "payment-events"
	QFollowUp          = "follow-up"
	QEmbedding         = "embedding"
)

// Task types — mirror the Node job name for every enqueue site.
const (
	TaskWebhookProcess       = "webhook-processing:process"
	TaskOrchestratorDebounce = "orchestrator-queue:debounced"
	TaskOutboundSend         = "outbound-queue:send-message"
	TaskCRMProcess           = "crm-materialiser:process-crm-signals"
	TaskPaymentProcess       = "payment-events:process"
	TaskEmbedProduct         = "embedding:embed-product"
	TaskEmbedProductImage    = "embedding:embed-product-image"
	TaskFollowUpRun          = "follow-up:run" // payload.job_type switches abandoned-cart/unpaid-invoice-first/unpaid-invoice-second/delivery-confirmation/re-engagement
)

// EnqueueOpts mirrors the per-site BullMQ jobOptions (plan §B.2 table).
type EnqueueOpts struct {
	MaxRetry  int
	ProcessIn time.Duration // delay (debounce 3s, follow-up +2h/+24h …)
	ProcessAt time.Time
	TaskID    string        // fixed-id dedup (webhook idempotency, orchestrator debounce key)
	UniqueTTL time.Duration // asynq Unique semantics
	RetainTTL time.Duration // retention after completion
}

// Publisher enqueues jobs. Producers (handlers) depend on this interface;
// asynq implementation arrives with T6.2. Null implementations are valid in
// tests. Implementations MUST be safe for concurrent use.
type Publisher interface {
	Enqueue(ctx context.Context, queue, taskType string, payload any, opts *EnqueueOpts) error
}

// Handler processes one task. Payload is the raw JSON body; decode into the
// stage's own DTO. Return error to trigger retry per queue policy.
type Handler func(ctx context.Context, payload []byte) error

// Registrar is what the worker main uses to attach handlers; implemented by
// the asynq mux wrapper in T6.2.
type Registrar interface {
	Register(taskType string, h Handler)
}
