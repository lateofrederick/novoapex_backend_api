// Stage 7B CRM handler ports (T7.10–T7.22, T7.29, T7.31): profile-builder,
// customer-capture and the currency config — the CRM-enrichment half of the
// checkout/CRM split. Order creation, payment initiation, profile-stats and
// follow-up scheduling now live in checkout.go (and its order.created
// consumers), not here.
//
// SOURCES (quoted inline at each step):
//   - libs/queue/src/handlers/profile-builder.handler.ts
//   - libs/queue/src/handlers/customer-capture.handler.ts
//   - libs/common/src/currency/currency.config.ts
package workers

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
)

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

// crmValidShortID mirrors VALID_SHORT_ID (order-ledger.handler.ts:22): the
// first 8 hex characters of a UUID — defence against LLM output containing
// LIKE wildcards or other garbage.
var crmValidShortID = regexp.MustCompile(`^[0-9a-f]{8}$`)

// crmEscapeLikePattern mirrors escapeLikePattern (order-ledger.handler.ts:28-30):
// backslash-escape LIKE wildcards so LLM output can't silently widen a match.
func crmEscapeLikePattern(value string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(value)
}

// crmNewID returns a random RFC-4122 v4 UUID string; the source relies on the
// Prisma uuid() column default, which does not exist for explicit inserts.
func crmNewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crm materialiser: entropy unavailable: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func crmText(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }

// ---------------------------------------------------------------------------
// profile-builder.handler.ts
// ---------------------------------------------------------------------------

// crmMaxPreferences mirrors MAX_PREFERENCES (profile-builder.handler.ts:7).
const crmMaxPreferences = 50

// crmHandleProfileBuilder ports ProfileBuilderHandler.handle
// (profile-builder.handler.ts:29-88): preference accumulation (deduplicated,
// case-insensitive, capped), delivery area latest-wins, sentiment change-only.
func crmHandleProfileBuilder(ctx context.Context, deps CRMDeps, job CRMSignalJob) error {
	q := gen.New(deps.Pool)

	profile, err := q.GetCustomerProfileByCustomerID(ctx, job.CustomerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Defensive — profile should exist from customer capture (Phase 3).
		// The source creates a bare profile and RETURNS without applying this
		// turn's updates (profile-builder.handler.ts:36-43).
		slog.Warn("CustomerProfile not found, creating", "customerId", job.CustomerID)
		_, cerr := q.CreateCustomerProfileIfMissing(ctx, gen.CreateCustomerProfileIfMissingParams{
			ID:         crmNewID(),
			CustomerID: job.CustomerID,
		})
		return cerr
	case err != nil:
		return err
	}

	signals := job.CRMSignals

	// 1. Accumulate preferences (deduplicate, case-insensitive, cap at
	// MAX_PREFERENCES) — profile-builder.handler.ts:47-68.
	if len(signals.DetectedPreferences) > 0 {
		var existing []string
		if len(profile.Preferences) > 0 {
			_ = json.Unmarshal(profile.Preferences, &existing) // non-array jsonb behaves like the source's Array.isArray fallback: empty
		}
		existingLower := make(map[string]struct{}, len(existing))
		for _, p := range existing {
			existingLower[strings.ToLower(p)] = struct{}{}
		}

		newPrefs := make([]string, 0, len(signals.DetectedPreferences))
		for _, p := range signals.DetectedPreferences {
			trimmed := strings.TrimSpace(p)
			if trimmed == "" {
				continue
			}
			if _, dup := existingLower[strings.ToLower(trimmed)]; dup {
				continue
			}
			newPrefs = append(newPrefs, trimmed)
			existingLower[strings.ToLower(trimmed)] = struct{}{}
		}

		if len(newPrefs) > 0 {
			merged := append(existing, newPrefs...)
			if len(merged) > crmMaxPreferences {
				merged = merged[:crmMaxPreferences]
			}
			blob, merr := json.Marshal(merged)
			if merr != nil {
				return merr
			}
			if _, uerr := q.UpdateProfilePreferences(ctx, gen.UpdateProfilePreferencesParams{
				Preferences: blob,
				CustomerID:  job.CustomerID,
			}); uerr != nil {
				return uerr
			}
		}
	}

	// 2. Update delivery area (latest value wins). The source guards with a
	// truthy check, so "" never overwrites (profile-builder.handler.ts:70-73).
	if signals.DeliveryArea != nil && *signals.DeliveryArea != "" {
		if _, uerr := q.UpdateProfileDeliveryArea(ctx, gen.UpdateProfileDeliveryAreaParams{
			DeliveryArea: pgtype.Text{String: strings.TrimSpace(*signals.DeliveryArea), Valid: true},
			CustomerID:   job.CustomerID,
		}); uerr != nil {
			return uerr
		}
	}

	// 3. Sentiment latest value — only when it actually changes, to avoid a
	// redundant write on every unchanged turn
	// (profile-builder.handler.ts:75-79).
	currentSentiment := ""
	if profile.Sentiment.Valid {
		currentSentiment = profile.Sentiment.String
	}
	if signals.Sentiment != nil && *signals.Sentiment != currentSentiment {
		if _, uerr := q.UpdateProfileSentiment(ctx, gen.UpdateProfileSentimentParams{
			Sentiment:  pgtype.Text{String: *signals.Sentiment, Valid: true},
			CustomerID: job.CustomerID,
		}); uerr != nil {
			return uerr
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// customer-capture.handler.ts
// ---------------------------------------------------------------------------

// crmHandleCustomerCapture ports CustomerCaptureHandler.updateCustomerName
// (customer-capture.handler.ts:27-56): longest-wins naming strategy.
func crmHandleCustomerCapture(ctx context.Context, deps CRMDeps, customerID, newName string) error {
	if strings.TrimSpace(newName) == "" {
		return nil // !newName || trim().length === 0 -> no-op
	}
	trimmedName := strings.TrimSpace(newName)

	q := gen.New(deps.Pool)
	currentRaw, err := q.GetCustomerNameByID(ctx, customerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		slog.Warn("Customer not found for name update", "customerId", customerID)
		return nil
	case err != nil:
		return err
	}

	currentName := ""
	if currentRaw.Valid {
		currentName = strings.TrimSpace(currentRaw.String)
	}
	// Only update if current name is null/empty or new name is longer. Length
	// is counted in runes, the closest Go analogue of JS string length here.
	if currentName == "" || utf8.RuneCountInString(trimmedName) > utf8.RuneCountInString(currentName) {
		if _, uerr := q.UpdateCustomerName(ctx, gen.UpdateCustomerNameParams{
			Name: pgtype.Text{String: trimmedName, Valid: true},
			ID:   customerID,
		}); uerr != nil {
			return uerr
		}
		slog.Info("Customer name updated",
			"customerId", customerID,
			"previousName", currentName,
			"newName", trimmedName,
		)
	}
	return nil
}

// crmUpdateAcquisitionChannel ports CustomerCaptureHandler.updateAcquisitionChannel
// (customer-capture.handler.ts): record where the customer came from, write-
// once — only a NULL or 'organic' channel is replaced, so the first real
// attribution sticks. Returns whether the channel changed.
func crmUpdateAcquisitionChannel(ctx context.Context, pool *pgxpool.Pool, customerID, channel string) (bool, error) {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return false, nil
	}
	tag, err := pool.Exec(ctx, `
		UPDATE customers
		   SET acquisition_channel = $2, updated_at = CURRENT_TIMESTAMP
		 WHERE id = $1 AND (acquisition_channel IS NULL OR acquisition_channel = 'organic')`,
		customerID, channel)
	if err != nil {
		return false, fmt.Errorf("crm: update acquisition channel: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	slog.InfoContext(ctx, "Acquisition channel set", "customerId", customerID, "channel", channel)
	return true, nil
}

// AcquisitionChannelFromReferral derives the channel recorded for a customer
// from WhatsApp referral data (click-to-WhatsApp ads and posts, and QR/deep
// links that carry a referral): "<source_type>:<source_id>", e.g.
// "ad:120212345678" — or just the source type when Meta sends no id.
func AcquisitionChannelFromReferral(referral map[string]any) string {
	sourceType, _ := referral["source_type"].(string)
	sourceID, _ := referral["source_id"].(string)
	sourceType, sourceID = strings.TrimSpace(sourceType), strings.TrimSpace(sourceID)
	switch {
	case sourceType == "" && sourceID == "":
		return "referral"
	case sourceType == "":
		return "referral:" + sourceID
	case sourceID == "":
		return sourceType
	default:
		return sourceType + ":" + sourceID
	}
}

// crmHandleMarketingOptIn ports CustomerCaptureHandler.updateMarketingOptIn:
// records explicit marketing consent (or its withdrawal) from the LLM's
// wants_updates signal. This is the ONLY thing that gates a new-arrivals
// digest send — WhatsApp policy requires documented opt-in for marketing
// template messages, so a stale/wrong value here has real platform-policy
// consequences, not just a UX one.
func crmHandleMarketingOptIn(ctx context.Context, deps CRMDeps, customerID string, optedIn bool) error {
	q := gen.New(deps.Pool)
	if _, err := q.UpdateCustomerMarketingOptIn(ctx, gen.UpdateCustomerMarketingOptInParams{
		MarketingOptIn: optedIn,
		ID:             customerID,
	}); err != nil {
		return err
	}
	slog.Info("Marketing opt-in updated", "customerId", customerID, "optedIn", optedIn)
	return nil
}

// ---------------------------------------------------------------------------
// currency.config.ts
// ---------------------------------------------------------------------------

// CurrencyDisplayConfig mirrors CurrencyDisplayConfig
// (currency.config.ts:6-16): the human-readable payment-method list must be
// market-specific because Paystack's channels are — promising Mobile Money in
// a card-only market would be a broken promise at exactly the moment the
// customer is trying to pay.
type CurrencyDisplayConfig struct {
	Symbol         string
	Locale         string
	PaymentMethods string
}

// CRMCurrencyConfig mirrors CURRENCY_CONFIG (currency.config.ts:18-34).
var CRMCurrencyConfig = map[string]CurrencyDisplayConfig{
	"GHS": {Symbol: "GH₵", Locale: "en-GH", PaymentMethods: "Mobile Money, card, or bank transfer"},
	"NGN": {Symbol: "₦", Locale: "en-NG", PaymentMethods: "card, bank transfer, or USSD"},
	"USD": {Symbol: "$", Locale: "en-US", PaymentMethods: "card"},
}

// GetCurrencyConfig resolves a currency's display config, falling back to GHS
// for unknowns (currency.config.ts:37-38). Brand names never appear in prose.
func GetCurrencyConfig(currency string) CurrencyDisplayConfig {
	if cfg, ok := CRMCurrencyConfig[currency]; ok {
		return cfg
	}
	return CRMCurrencyConfig["GHS"]
}
