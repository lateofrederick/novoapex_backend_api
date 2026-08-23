package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// CronEntry describes one periodic job for the worker scheduler.
type CronEntry struct {
	Spec     string
	Cron     string
	TaskType string
	Payload  any
}

// CronSpecs returns the three Stage 7 cron definitions
// (follow-up-scanner.processor.ts:31, outbox-sweep.processor.ts:31,
// retention-scanner.processor.ts:24). Singleton semantics per run come from
// pg_advisory_xact_lock(hashtext(spec)) inside each sweep (T7.28).
func CronSpecs() []CronEntry {
	return []CronEntry{
		{Spec: "follow-up-scanner", Cron: "*/1 * * * *", TaskType: "cron:follow-up-scanner"},
		{Spec: "outbox-sweep", Cron: "*/2 * * * *", TaskType: "cron:outbox-sweep"},
		{Spec: "retention-scanner", Cron: "0 9 * * *", TaskType: "cron:retention-scanner"},
	}
}

// SweepOutbox ports OutboxSweepProcessor.sweepStrandedMessages
// (outbox-sweep.processor.ts:32-95): re-enqueue outbound rows stuck 'pending'
// older than two minutes (persist-then-publish gap recovery — survives
// because asynq has no transactional enqueue); when a newer message in the
// same conversation already went out, mark the stranded row 'failed_stale'
// instead to avoid out-of-order delivery.
func SweepOutbox(ctx context.Context, deps Deps) error {
	pool := deps.Pool
	if pool == nil {
		return errors.New("outbox-sweep: nil pool")
	}

	now := time.Now().UTC()
	cutoff := now.Add(-2 * time.Minute)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("outbox-sweep: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('outbox-sweep'))`); err != nil {
		return fmt.Errorf("outbox-sweep: advisory lock: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT id, conversation_id
		  FROM outbound_messages
		 WHERE status = 'pending' AND created_at < $1
		 ORDER BY created_at ASC
		 LIMIT 100`, cutoff)
	if err != nil {
		return fmt.Errorf("outbox-sweep: stranded query: %w", err)
	}
	type strandedRow struct {
		id             string
		conversationID string
	}
	var stranded []strandedRow
	for rows.Next() {
		var m strandedRow
		var conv pgtype.Text
		if err := rows.Scan(&m.id, &conv); err != nil {
			rows.Close()
			return fmt.Errorf("outbox-sweep: stranded scan: %w", err)
		}
		m.conversationID = conv.String
		stranded = append(stranded, m)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("outbox-sweep: stranded rows: %w", err)
	}
	rows.Close()

	if len(stranded) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("outbox-sweep: commit: %w", err)
		}
		return nil
	}

	slog.InfoContext(ctx, fmt.Sprintf("Outbox sweep found %d stranded message(s)", len(stranded)))

	requeued, stale := 0, 0
	for _, msg := range stranded {
		if msg.conversationID != "" {
			var newerSent string
			err := tx.QueryRow(ctx, `
				SELECT id FROM outbound_messages
				 WHERE conversation_id = $1 AND status = 'sent' AND created_at > (
				       SELECT created_at FROM outbound_messages WHERE id = $2)
				 LIMIT 1`, msg.conversationID, msg.id).Scan(&newerSent)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
			case err != nil:
				return fmt.Errorf("outbox-sweep: staleness probe: %w", err)
			default:
				if _, err := tx.Exec(ctx,
					`UPDATE outbound_messages SET status = 'failed_stale' WHERE id = $1`,
					msg.id); err != nil {
					return fmt.Errorf("outbox-sweep: mark failed_stale: %w", err)
				}
				stale++
				continue
			}
		}

		if deps.Publisher == nil {
			slog.WarnContext(ctx, "outbound publish skipped: no queue bridge wired (ADR 0001)",
				"outboundMessageId", msg.id)
			continue
		}
		opts := &queue.EnqueueOpts{TaskID: "outbox-sweep:" + msg.id}
		if err := deps.Publisher.Enqueue(ctx, queue.QOutbound, queue.TaskOutboundSend,
			outboundSendJob{OutboundMessageID: msg.id}, opts); err != nil {
			slog.ErrorContext(ctx, "Failed to re-enqueue stranded message",
				"outboundMessageId", msg.id,
				"error", err.Error())
			continue
		}
		requeued++
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("outbox-sweep: commit: %w", err)
	}

	slog.InfoContext(ctx, fmt.Sprintf("Outbox sweep complete: %d re-enqueued, %d marked stale", requeued, stale))
	return nil
}

// SweepRetention ports RetentionScannerProcessor.scanForReengagement
// (retention-scanner.processor.ts:24-88): profiles whose daysSinceLastOrder
// exceeds orderFrequencyDays x thresholdMultiplier get a re-engagement
// follow-up enqueued, rate-limited to one per cooldown window by
// lastReengagementAt, which is stamped after each send.
func SweepRetention(ctx context.Context, deps Deps) error {
	pool := deps.Pool
	if pool == nil {
		return errors.New("retention-scanner: nil pool")
	}

	thresholdMultiplier := deps.ReengagementThresholdMultiplier
	if thresholdMultiplier == 0 {
		thresholdMultiplier = 1.5
	}
	cooldownDays := deps.ReengagementCooldownDays
	if cooldownDays == 0 {
		cooldownDays = 30
	}

	slog.InfoContext(ctx, "Starting daily retention scan...")

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("retention-scanner: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('retention-scanner'))`); err != nil {
		return fmt.Errorf("retention-scanner: advisory lock: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT p.customer_id, p.last_order_at, p.order_frequency_days, p.last_reengagement_at, c.business_id
		  FROM customer_profiles p
		  JOIN customers c ON c.id = p.customer_id
		 WHERE p.last_order_at IS NOT NULL AND p.order_frequency_days IS NOT NULL`)
	if err != nil {
		return fmt.Errorf("retention-scanner: profiles query: %w", err)
	}
	type profileRow struct {
		customerID       string
		businessID       string
		lastOrderAt      time.Time
		frequencyDays    float64
		lastReengagement pgtype.Timestamp
	}
	var profiles []profileRow
	for rows.Next() {
		var p profileRow
		var lastOrder pgtype.Timestamp
		if err := rows.Scan(&p.customerID, &lastOrder, &p.frequencyDays, &p.lastReengagement, &p.businessID); err != nil {
			rows.Close()
			return fmt.Errorf("retention-scanner: profile scan: %w", err)
		}
		p.lastOrderAt = lastOrder.Time
		profiles = append(profiles, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("retention-scanner: profile rows: %w", err)
	}
	rows.Close()

	now := time.Now().UTC()
	reengaged := 0

	for _, p := range profiles {
		daysSinceLastOrder := now.Sub(p.lastOrderAt).Hours() / 24
		if daysSinceLastOrder <= p.frequencyDays*thresholdMultiplier {
			continue
		}
		if p.lastReengagement.Valid &&
			now.Sub(p.lastReengagement.Time).Hours()/24 < float64(cooldownDays) {
			continue
		}

		var lastOrderID string
		err := tx.QueryRow(ctx, `
			SELECT id FROM orders WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 1`,
			p.customerID).Scan(&lastOrderID)
		if errors.Is(err, pgx.ErrNoRows) {
			lastOrderID = ""
		} else if err != nil {
			return fmt.Errorf("retention-scanner: last order lookup: %w", err)
		}

		if deps.Publisher == nil {
			slog.WarnContext(ctx, "outbound publish skipped: no queue bridge wired (ADR 0001)",
				"customerId", p.customerID)
			continue
		}
		payload := FollowUpPayload{
			JobType:    "re-engagement",
			BusinessID: p.businessID,
			CustomerID: p.customerID,
			OrderID:    lastOrderID,
		}
		if err := deps.Publisher.Enqueue(ctx, queue.QFollowUp, queue.TaskFollowUpRun, payload, nil); err != nil {
			return fmt.Errorf("retention-scanner: enqueue re-engagement: %w", err)
		}

		if _, err := tx.Exec(ctx,
			`UPDATE customer_profiles SET last_reengagement_at = $2 WHERE customer_id = $1`,
			p.customerID, now); err != nil {
			return fmt.Errorf("retention-scanner: stamp cooldown: %w", err)
		}

		reengaged++
		slog.InfoContext(ctx, fmt.Sprintf("Enqueued re-engagement for customer %s", p.customerID))
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("retention-scanner: commit: %w", err)
	}

	slog.InfoContext(ctx, fmt.Sprintf("Retention scan completed. Enqueued %d re-engagements.", reengaged))
	return nil
}
