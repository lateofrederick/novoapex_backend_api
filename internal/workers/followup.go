package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// FollowUpPayload mirrors FollowUpJobData (schemas/follow-up.schema.ts).
type FollowUpPayload struct {
	JobType    string `json:"jobType"`
	BusinessID string `json:"businessId"`
	CustomerID string `json:"customerId"`
	OrderID    string `json:"orderId"`
}

// RegisterFollowUp wires the follow-up handler onto reg.
func RegisterFollowUp(reg queue.Registrar, deps Deps) {
	reg.Register(queue.TaskFollowUpRun, func(ctx context.Context, payload []byte) error {
		var fp FollowUpPayload
		if err := json.Unmarshal(payload, &fp); err != nil {
			return fmt.Errorf("follow-up: decode job: %w", err)
		}
		return HandleFollowUp(ctx, deps, fp)
	})
}

// HandleFollowUp ports FollowUpProcessor.process
// (follow-up.processor.ts:34-144): relevance re-check against the live order
// status per job type, then exact-copy message enqueued to outbound-queue.
func HandleFollowUp(ctx context.Context, deps Deps, fp FollowUpPayload) error {
	pool := deps.Pool
	if pool == nil {
		return errors.New("follow-up: nil pool")
	}

	slog.InfoContext(ctx, "Processing follow-up job",
		"event", "follow_up_started",
		"jobType", fp.JobType,
		"orderId", fp.OrderID)

	var status, customerName, customerPhone, currency string
	var total decimal.Decimal
	err := pool.QueryRow(ctx, `
		SELECT o.status::text, o.total_amount, c.name, c.phone, b.currency
		  FROM orders o
		  JOIN customers c ON c.id = o.customer_id
		  JOIN businesses b ON b.id = o.business_id
		 WHERE o.id = $1`, fp.OrderID).Scan(&status, &total, &customerName, &customerPhone, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		slog.WarnContext(ctx, "Order or customer not found, skipping follow-up",
			"orderId", fp.OrderID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("follow-up: order lookup: %w", err)
	}

	currencySymbol := symbolForCurrency(currency)
	name := customerName
	if name == "" {
		name = "there"
	}
	formattedTotal := total.StringFixed(2)

	shouldSend := false
	messageText := ""
	isLatePaymentReminder := false
	switch fp.JobType {
	case "abandoned-cart":
		if status == "CONFIRMED" {
			shouldSend = true
			messageText = fmt.Sprintf("Hi %s, looks like you left some items in your cart. Let us know if you'd like to complete your order!", name)
		}
	case "unpaid-invoice-first":
		if status == "PAYMENT_PENDING" || status == "CONFIRMED" {
			shouldSend = true
			isLatePaymentReminder = true
			messageText = fmt.Sprintf("Hi %s, just a friendly reminder about your recent order for %s%s. Let us know if you need help with payment.", name, currencySymbol, formattedTotal)
		}
	case "unpaid-invoice-second":
		if status == "PAYMENT_PENDING" || status == "CONFIRMED" {
			shouldSend = true
			isLatePaymentReminder = true
			messageText = fmt.Sprintf("Hi %s, this is a final reminder for your order of %s%s. Your order will be cancelled soon if payment is not received.", name, currencySymbol, formattedTotal)
		}
	case "delivery-confirmation":
		if status == "DELIVERED" {
			shouldSend = true
			messageText = fmt.Sprintf("Hi %s, your order has been marked as delivered. Did everything arrive safely?", name)
		}
	case "re-engagement":
		shouldSend = true
		messageText = fmt.Sprintf("Hi %s, it's been a while since your last order! We miss you and wanted to share our latest catalog with you.", name)
	default:
		slog.WarnContext(ctx, "Unknown follow-up job type", "jobType", fp.JobType)
	}

	if !shouldSend {
		slog.InfoContext(ctx, "Follow-up job no longer relevant",
			"jobType", fp.JobType,
			"orderStatus", status)
		return nil
	}

	// Payment-behaviour signal: a reminder firing means the order was still
	// unpaid at the scheduled deadline — a proxy for "needs a nudge to pay".
	// Non-blocking: a failed CRM write must never stop the reminder itself.
	if isLatePaymentReminder {
		if _, ierr := pool.Exec(ctx,
			`UPDATE customer_profiles SET late_payment_count = late_payment_count + 1, updated_at = CURRENT_TIMESTAMP WHERE customer_id = $1`,
			fp.CustomerID); ierr != nil {
			slog.WarnContext(ctx, "Failed to record late-payment signal (non-blocking)",
				"customerId", fp.CustomerID, "error", ierr.Error())
		}
	}

	var conversationID string
	err = pool.QueryRow(ctx,
		`SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
		fp.BusinessID, customerPhone).Scan(&conversationID)
	if errors.Is(err, pgx.ErrNoRows) {
		slog.WarnContext(ctx, "Conversation not found for follow-up",
			"customerId", fp.CustomerID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("follow-up: conversation lookup: %w", err)
	}

	// Persist the OutboundMessage row BEFORE enqueueing — OutboundProcessor
	// (the sole worker on outbound-queue) reads job.data.outboundMessageId and
	// looks up the row; it does not accept a raw text payload. The previous
	// version of this handler enqueued {businessId, conversationId,
	// recipientPhone, text} directly, which nothing consumes — every
	// follow-up message silently failed after retries despite this log line
	// below claiming success. Fixed to match the pattern every other sender
	// in this codebase already uses (payment_events.go sendPaymentConfirmation,
	// orchestrator_context.go orchEnqueueOutboundText).
	rawPayload, merr := json.Marshal(map[string]any{
		"messaging_product": "whatsapp",
		"type":              "text",
		"text":              map[string]string{"body": messageText},
	})
	if merr != nil {
		return fmt.Errorf("follow-up: marshal outbound payload: %w", merr)
	}
	outboundID := uuid.NewString()
	if err := orchInsertOutboundRow(ctx, pool, outboundRow{
		id: outboundID, businessID: fp.BusinessID, conversationID: conversationID,
		messageType: "text", textContent: &messageText,
		rawPayload: rawPayload, status: "pending",
	}); err != nil {
		return fmt.Errorf("follow-up: persist outbound row: %w", err)
	}

	if deps.Publisher == nil {
		slog.WarnContext(ctx, "outbound publish skipped: no queue bridge wired (ADR 0001)",
			"jobType", fp.JobType)
		return nil
	}
	if err := orchPublishOutbound(ctx, deps.Publisher, outboundID); err != nil {
		return fmt.Errorf("follow-up: enqueue outbound: %w", err)
	}

	slog.InfoContext(ctx, "Follow-up message enqueued",
		"event", "follow_up_sent",
		"jobType", fp.JobType,
		"orderId", fp.OrderID)
	return nil
}

// SweepFollowUps ports FollowUpScannerProcessor.sweepDueFollowUps
// (follow-up-scanner.processor.ts:32-90): claim due rows with an executed_at
// compare-and-set BEFORE enqueueing, so overlapping sweeps can never
// double-send. The advisory xact lock (T7.28) makes one cron run a singleton.
func SweepFollowUps(ctx context.Context, deps Deps) error {
	pool := deps.Pool
	if pool == nil {
		return errors.New("follow-up: nil pool")
	}

	now := time.Now().UTC()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("follow-up: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('follow-up-scanner'))`); err != nil {
		return fmt.Errorf("follow-up: advisory lock: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT id, job_type, business_id, customer_id, order_id
		  FROM scheduled_follow_ups
		 WHERE scheduled_at <= $1 AND executed_at IS NULL AND cancelled_at IS NULL
		 ORDER BY scheduled_at ASC
		 LIMIT 50`, now)
	if err != nil {
		return fmt.Errorf("follow-up: due query: %w", err)
	}
	type dueRow struct {
		id      string
		payload FollowUpPayload
	}
	var due []dueRow
	for rows.Next() {
		var d dueRow
		if err := rows.Scan(&d.id, &d.payload.JobType, &d.payload.BusinessID, &d.payload.CustomerID, &d.payload.OrderID); err != nil {
			rows.Close()
			return fmt.Errorf("follow-up: due scan: %w", err)
		}
		due = append(due, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("follow-up: due rows: %w", err)
	}
	rows.Close()

	enqueued := 0
	for _, d := range due {
		tag, err := tx.Exec(ctx, `
			UPDATE scheduled_follow_ups
			   SET executed_at = $2
			 WHERE id = $1 AND executed_at IS NULL AND cancelled_at IS NULL`,
			d.id, now)
		if err != nil {
			return fmt.Errorf("follow-up: claim row: %w", err)
		}
		if tag.RowsAffected() == 0 {
			continue
		}

		opts := &queue.EnqueueOpts{TaskID: "scheduled-follow-up:" + d.id}
		if deps.Publisher == nil {
			slog.WarnContext(ctx, "outbound publish skipped: no queue bridge wired (ADR 0001)",
				"scheduledFollowUpId", d.id)
			continue
		}
		if err := deps.Publisher.Enqueue(ctx, queue.QFollowUp, queue.TaskFollowUpRun, d.payload, opts); err != nil {
			slog.ErrorContext(ctx, "Failed to enqueue claimed follow-up (will not retry — row already marked executed)",
				"scheduledFollowUpId", d.id,
				"orderId", d.payload.OrderID,
				"error", err.Error())
			continue
		}
		enqueued++
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("follow-up: commit: %w", err)
	}

	slog.InfoContext(ctx, fmt.Sprintf("Follow-up sweep complete: %d/%d enqueued", enqueued, len(due)))
	return nil
}

// symbolForCurrency ports CURRENCY_CONFIG symbols (currency.config.ts):
// GHS GH₵, NGN ₦, USD $, unknown falls back to GHS.
func symbolForCurrency(currency string) string {
	switch currency {
	case "NGN":
		return "₦"
	case "USD":
		return "$"
	default:
		return "GH₵"
	}
}

// s7a_requiresEscalation duplicates safety.guard.requiresEscalation
// (libs/orchestrator/src/guards/safety.guard.ts) locally because
// internal/domain (sibling-owned this wave) does not exist yet. Dedupe onto
// domain.RequiresEscalation once it lands.
var s7a_escalationPatterns = regexp.MustCompile(`(?i)\b(refund|police|scam|angry|not helpful|dispute)\b`)

func s7a_requiresEscalation(textContent string) bool {
	if textContent == "" {
		return false
	}
	return s7a_escalationPatterns.MatchString(textContent)
}
