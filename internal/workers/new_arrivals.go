package workers

// new_arrivals.go ports the new-arrivals digest scanner from this session's
// Node work (apps/mobile-api's business.newArrivalsTemplateName +
// libs/queue/src/processors/new-arrivals-scanner.processor.ts): a periodic
// WhatsApp marketing-template send to opted-in customers when a business has
// new products since its last digest. Structured exactly like SweepRetention
// in crons.go (advisory lock, raw SQL throughout — no sqlc, matching every
// other cron-sweep file in this package).
//
// Two hard WhatsApp platform constraints, not design choices: (1) a business-
// initiated send outside the 24h customer-service window requires an
// approved Meta template — hence gating on business.new_arrivals_template_name
// being set, and sending message_type='template' rather than free text; (2) a
// marketing-category template additionally requires documented opt-in —
// hence gating on customers.marketing_opt_in = true. Templates carry no
// dynamic variables (see internal/integrations/whatsapp), so the content
// stays a static teaser; preference-matching below is a SEND FILTER (who
// gets it), never content personalisation (what it says).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// SweepNewArrivals ports NewArrivalsScannerProcessor.scanForNewArrivals: per
// business with an approved template, checks the configured interval has
// elapsed and at least one product was created since the last digest, then
// sends to opted-in customers whose recorded preferences overlap a new
// product's category (or who have no preferences recorded, or when none of
// the new products carry a category — never withheld for lack of data).
func SweepNewArrivals(ctx context.Context, deps Deps) error {
	pool := deps.Pool
	if pool == nil {
		return errors.New("new-arrivals-scanner: nil pool")
	}

	intervalDays := deps.NewArrivalsIntervalDays
	if intervalDays == 0 {
		intervalDays = 14
	}

	slog.InfoContext(ctx, "Starting daily new-arrivals scan...")

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("new-arrivals-scanner: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('new-arrivals-scanner'))`); err != nil {
		return fmt.Errorf("new-arrivals-scanner: advisory lock: %w", err)
	}

	type businessRow struct {
		id                string
		templateName      string
		lastNotifiedAt    *time.Time
		hasLastNotifiedAt bool
	}

	rows, err := tx.Query(ctx, `
		SELECT id, new_arrivals_template_name, last_new_arrivals_notified_at
		  FROM businesses
		 WHERE new_arrivals_template_name IS NOT NULL`)
	if err != nil {
		return fmt.Errorf("new-arrivals-scanner: businesses query: %w", err)
	}
	var businesses []businessRow
	for rows.Next() {
		var b businessRow
		var lastNotified *time.Time
		if err := rows.Scan(&b.id, &b.templateName, &lastNotified); err != nil {
			rows.Close()
			return fmt.Errorf("new-arrivals-scanner: business scan: %w", err)
		}
		b.lastNotifiedAt = lastNotified
		b.hasLastNotifiedAt = lastNotified != nil
		businesses = append(businesses, b)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("new-arrivals-scanner: business rows: %w", err)
	}
	rows.Close()

	now := time.Now().UTC()
	digestsSent := 0
	var toPublish []string

	for _, b := range businesses {
		if b.hasLastNotifiedAt && now.Sub(*b.lastNotifiedAt) < time.Duration(intervalDays)*24*time.Hour {
			continue // not due yet
		}

		since := time.Unix(0, 0).UTC()
		if b.hasLastNotifiedAt {
			since = *b.lastNotifiedAt
		}

		catRows, err := tx.Query(ctx, `
			SELECT category FROM products
			 WHERE business_id = $1 AND is_available = true AND created_at > $2`,
			b.id, since)
		if err != nil {
			return fmt.Errorf("new-arrivals-scanner: new products query: %w", err)
		}
		var newCategories []string
		newProductCount := 0
		for catRows.Next() {
			var cat *string
			if err := catRows.Scan(&cat); err != nil {
				catRows.Close()
				return fmt.Errorf("new-arrivals-scanner: category scan: %w", err)
			}
			newProductCount++
			if cat != nil && *cat != "" {
				newCategories = append(newCategories, strings.ToLower(*cat))
			}
		}
		if err := catRows.Err(); err != nil {
			catRows.Close()
			return fmt.Errorf("new-arrivals-scanner: category rows: %w", err)
		}
		catRows.Close()

		// Nothing new since last time — skip silently rather than send an
		// empty "check out what's new" digest.
		if newProductCount == 0 {
			continue
		}

		custRows, err := tx.Query(ctx, `
			SELECT cu.id, cu.phone, COALESCE(p.preferences, '[]'::jsonb)
			  FROM customers cu
			  LEFT JOIN customer_profiles p ON p.customer_id = cu.id
			 WHERE cu.business_id = $1 AND cu.marketing_opt_in = true`, b.id)
		if err != nil {
			return fmt.Errorf("new-arrivals-scanner: opted-in customers query: %w", err)
		}
		type custRow struct {
			id          string
			phone       string
			preferences []string
		}
		var customers []custRow
		for custRows.Next() {
			var c custRow
			var prefsJSON []byte
			if err := custRows.Scan(&c.id, &c.phone, &prefsJSON); err != nil {
				custRows.Close()
				return fmt.Errorf("new-arrivals-scanner: customer scan: %w", err)
			}
			if len(prefsJSON) > 0 {
				_ = json.Unmarshal(prefsJSON, &c.preferences) // non-array jsonb -> empty, same fallback as profile-builder.handler.ts
			}
			customers = append(customers, c)
		}
		if err := custRows.Err(); err != nil {
			custRows.Close()
			return fmt.Errorf("new-arrivals-scanner: customer rows: %w", err)
		}
		custRows.Close()

		sentCount := 0
		for _, c := range customers {
			if !matchesNewArrivalsInterest(c.preferences, newCategories) {
				continue
			}

			var conversationID string
			err := tx.QueryRow(ctx,
				`SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
				b.id, c.phone).Scan(&conversationID)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return fmt.Errorf("new-arrivals-scanner: conversation lookup: %w", err)
			}

			rawPayload := []byte(fmt.Sprintf(
				`{"messaging_product":"whatsapp","to":%q,"type":"template","template":{"name":%q,"language":{"code":"en_US"}}}`,
				c.phone, b.templateName))

			outboundID := uuid.NewString()
			if _, err := tx.Exec(ctx, `
				INSERT INTO outbound_messages
					(id, business_id, conversation_id, recipient_phone, message_type,
					 template_name, raw_payload, status)
				VALUES ($1, $2, $3, $4, 'template', $5, $6, 'pending')`,
				outboundID, b.id, conversationID, c.phone, b.templateName, rawPayload); err != nil {
				return fmt.Errorf("new-arrivals-scanner: persist digest outbound: %w", err)
			}

			// Published only after COMMIT (below): the outbound consumer must
			// be able to see the row when it picks the task up.
			toPublish = append(toPublish, outboundID)
			sentCount++
		}

		if _, err := tx.Exec(ctx,
			`UPDATE businesses SET last_new_arrivals_notified_at = $2 WHERE id = $1`,
			b.id, now); err != nil {
			return fmt.Errorf("new-arrivals-scanner: stamp notified: %w", err)
		}

		digestsSent++
		slog.InfoContext(ctx, fmt.Sprintf(
			"New-arrivals digest sent to %d/%d opted-in customer(s) for business %s.",
			sentCount, len(customers), b.id))
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("new-arrivals-scanner: commit: %w", err)
	}

	for _, outboundID := range toPublish {
		if deps.Publisher == nil {
			slog.WarnContext(ctx, "outbound publish skipped: no queue publisher wired",
				"outboundMessageId", outboundID)
			continue
		}
		// A failed publish leaves the committed 'pending' row for the outbox
		// sweep to recover.
		if err := queue.PublishOutbound(ctx, deps.Publisher, outboundID); err != nil {
			slog.ErrorContext(ctx, "Failed to enqueue new-arrivals digest",
				"outboundMessageId", outboundID, "error", err.Error())
		}
	}

	slog.InfoContext(ctx, fmt.Sprintf("New-arrivals scan complete. %d business(es) notified.", digestsSent))
	return nil
}

// matchesNewArrivalsInterest is a SEND FILTER, not content personalisation.
// A customer with no recorded preference tags is included rather than
// withheld — there is simply no basis to exclude them, and this is the one
// accumulated field (see profile-builder.handler.ts) that otherwise never
// gets read for anything. Uncategorised new products can't be matched
// against either, so every opted-in customer qualifies in that case too.
func matchesNewArrivalsInterest(preferences, newCategories []string) bool {
	if len(newCategories) == 0 {
		return true
	}
	if len(preferences) == 0 {
		return true
	}
	prefs := make(map[string]bool, len(preferences))
	for _, p := range preferences {
		prefs[strings.ToLower(p)] = true
	}
	for _, cat := range newCategories {
		if prefs[cat] {
			return true
		}
	}
	return false
}
