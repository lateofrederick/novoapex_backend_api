// Package handlers — Stage 5 payments ingest (T5.11–T5.14, T5.17).
//
// webhooks_payments.go ports PaymentsController
// (apps/api/src/payments/payments.controller.ts):
//
//	POST /webhooks/payments/{provider}
//
// Source contract (payments.controller.ts:38-127), quoted for parity:
//
//  1. unknown provider  -> 400 {"error":"Unknown provider"}  (a plain body,
//     NOT the Nest exceptions envelope; the brief's "404" was wrong —
//     SOURCE wins)
//  2. no raw body       -> 500 {"error":"Server configuration error"}
//  3. bad signature     -> 400 {"error":"Invalid signature"}
//  4. enqueue {providerName, rawPayload, receivedAt} onto payment-events
//     with jobId/dedup id `payment:${providerName}:${externalReference}`
//     and a 300_000 ms dedup window
//  5. ALWAYS res.status(200).json({status:'ok'}) — enqueue failures are
//     logged ('payment_webhook_enqueue_failed') and swallowed.
//
// externalReference = data.reference ?? `${providerName}:${Date.now()}`.
// Signature header selection is x-paystack-signature ?? x-hubtel-signature.
//
// DELTA vs the Stage-5 brief: the published payload is the job envelope the
// Node controller enqueues AND the landed Go worker decodes
// (workers.PaymentEventJob), NOT a bare NormalisedPaymentEvent — publishing
// the normalised form would break RegisterPaymentEvents, which parses
// job.RawPayload itself. Parse coverage is proven at the handler boundary in
// tests by decoding the captured payload through paystack.ParseWebhookEvent.
package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// PaymentsIngestDeps carries the payments-ingest collaborators.
type PaymentsIngestDeps struct {
	Publisher      queue.Publisher
	PaystackSecret string // PAYSTACK_SECRET_KEY
}

// MountPaymentsIngest registers POST /webhooks/payments/{provider} on r.
func MountPaymentsIngest(r chi.Router, d PaymentsIngestDeps) {
	factory := wp_newProviderFactory(d.PaystackSecret)
	r.Post("/webhooks/payments/{provider}", wp_handle(factory, d.Publisher))
}

// PaymentProvider ports PaymentProvider
// (libs/common/src/payments/payment-provider.interface.ts) narrowed to what
// ingest needs: identity + signature verification.
type PaymentProvider interface {
	ProviderName() string
	VerifyWebhookSignature(body []byte, signature string) bool
}

// wp_paystackProvider adapts the paystack package onto PaymentProvider.
type wp_paystackProvider struct {
	secret string
}

func (p *wp_paystackProvider) ProviderName() string { return "paystack" }

func (p *wp_paystackProvider) VerifyWebhookSignature(body []byte, signature string) bool {
	if p.secret == "" {
		slog.Error("PAYSTACK_SECRET_KEY is not configured — cannot verify webhook")
		return false
	}
	return paystack.VerifyWebhookSignature(p.secret, body, signature)
}

// wp_providerFactory ports PaymentProviderFactory
// (payment-provider.factory.ts): getProvider/getDefaultProvider/
// getAvailableProviders over a registered map. Adding hubtel = one adapter +
// one registration.
type wp_providerFactory struct {
	providers map[string]PaymentProvider
	def       PaymentProvider // getDefaultProvider() -> Paystack
}

func wp_newProviderFactory(paystackSecret string) *wp_providerFactory {
	ps := &wp_paystackProvider{secret: paystackSecret}
	return &wp_providerFactory{
		providers: map[string]PaymentProvider{
			ps.ProviderName(): ps,
			// "hubtel": register here when the provider exists
		},
		def: ps,
	}
}

// GetProvider throws like the factory; callers translate the miss into the
// controller's 400 {"error":"Unknown provider"} branch.
func (f *wp_providerFactory) GetProvider(name string) (PaymentProvider, error) {
	if p, ok := f.providers[name]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("Unknown payment provider: %q. Available providers: %s", //nolint:staticcheck // verbatim payment-provider.factory.ts message
		name, fmt.Sprint(f.GetAvailableProviders()))
}

func (f *wp_providerFactory) GetDefault() PaymentProvider { return f.def }

func (f *wp_providerFactory) GetAvailableProviders() []string {
	names := make([]string, 0, len(f.providers))
	for n := range f.providers {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// wp_paymentEventJob mirrors PaymentEventJobData
// (schemas/payment-event.schema.ts == workers.PaymentEventJob): exactly what
// both the BullMQ consumer and the asynq worker decode.
type wp_paymentEventJob struct {
	ProviderName string          `json:"providerName"`
	RawPayload   json.RawMessage `json:"rawPayload"`
	ReceivedAt   string          `json:"receivedAt"`
}

// wp_handle implements handleWebhook; every rejection path answers with the
// source's exact status + PLAIN body (never the Nest error envelope).
func wp_handle(factory *wp_providerFactory, publisher queue.Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		providerName := chi.URLParam(r, "provider")

		// 1. Resolve the provider.
		provider, err := factory.GetProvider(providerName)
		if err != nil {
			slog.Warn("Unknown payment provider in webhook URL",
				"event", "payment_webhook_unknown_provider",
				"providerName", providerName)
			wp_plainJSON(w, http.StatusBadRequest, map[string]string{"error": "Unknown provider"})
			return
		}

		// 2. Raw body for signature verification (T5.2 raw-body capture).
		rawBody, err := io.ReadAll(r.Body)
		if err != nil || len(rawBody) == 0 {
			slog.Error("Raw body not available for signature verification",
				"event", "payment_webhook_no_raw_body",
				"providerName", providerName)
			wp_plainJSON(w, http.StatusInternalServerError, map[string]string{"error": "Server configuration error"})
			return
		}

		// 3. Verify webhook signature (header fallback chain preserved).
		signature := r.Header.Get("X-Paystack-Signature")
		if signature == "" {
			signature = r.Header.Get("X-Hubtel-Signature")
		}
		if !provider.VerifyWebhookSignature(rawBody, signature) {
			slog.Warn("Invalid webhook signature",
				"event", "payment_webhook_invalid_signature",
				"providerName", providerName)
			wp_plainJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid signature"})
			return
		}

		// 4. Parse body and enqueue for async processing.
		var parsed struct {
			Data struct {
				Reference string `json:"reference"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rawBody, &parsed); err != nil {
			// JSON.parse(rawBody.toString()) throws here on Node -> 500 via
			// the exceptions filter; mirror that envelope.
			httpx.WriteError(w, r, fmt.Errorf("parse webhook body: %w", err))
			return
		}

		externalReference := parsed.Data.Reference
		if externalReference == "" {
			externalReference = fmt.Sprintf("%s:%d", providerName, time.Now().UnixMilli())
		}

		err = publisher.Enqueue(ctx, queue.QPaymentEvents, queue.TaskPaymentProcess,
			wp_paymentEventJob{
				ProviderName: providerName,
				RawPayload:   json.RawMessage(rawBody), // verbatim bytes
				ReceivedAt:   time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"),
			},
			&queue.EnqueueOpts{
				TaskID:    "payment:" + providerName + ":" + externalReference,
				MaxRetry:  4,                 // §B.2 payment-events policy
				UniqueTTL: 300 * time.Second, // deduplication ttl 300_000 ms
			})
		if err != nil {
			slog.Error("Failed to enqueue payment webhook",
				"event", "payment_webhook_enqueue_failed",
				"providerName", providerName,
				"error", err.Error())
		} else {
			slog.Info("Payment webhook enqueued",
				"event", "payment_webhook_enqueued",
				"providerName", providerName,
				"externalReference", externalReference)
		}

		// 5. Always return 200 immediately.
		wp_plainJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// wp_plainJSON writes Express res.json(...)-style bodies for this controller
// (plain objects without the Nest exceptions wrapper).
func wp_plainJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
