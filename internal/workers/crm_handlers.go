// Stage 7B CRM handler ports (T7.10–T7.22, T7.29, T7.31): profile-builder,
// order-ledger and customer-capture, one file per source handler group.
//
// SOURCES (quoted inline at each step):
//   - libs/queue/src/handlers/profile-builder.handler.ts
//   - libs/queue/src/handlers/order-ledger.handler.ts
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
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/domain"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
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

// crmDecimalToNumeric converts shopspring decimals into pgtype.Numeric so
// exact values reach NUMERIC(14,2) columns without a float64 hop.
func crmDecimalToNumeric(d decimal.Decimal) pgtype.Numeric {
	return pgtype.Numeric{
		Int:   d.Coefficient(),
		Exp:   d.Exponent(),
		Valid: true,
	}
}

func crmText(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }

// errCRMInsufficientStock mirrors InsufficientStockError
// (order-ledger.handler.ts:13-19), thrown inside the order transaction to
// roll it back. Message kept byte-identical to the source class.
var errCRMInsufficientStock = errors.New("Insufficient stock for one or more order items") //nolint:staticcheck // ST1005: verbatim port of the TS error message

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
// order-ledger.handler.ts
// ---------------------------------------------------------------------------

// resolvedItem is the post-resolution ledger entry
// (order-ledger.handler.ts:72-77).
type resolvedItem struct {
	productID   string
	productName string
	unitPrice   decimal.Decimal
	quantity    int32
}

type crmOrderItemRow struct { // orderItem.createMany payload row
	ID          string `json:"id"`
	OrderID     string `json:"order_id"`
	ProductID   string `json:"product_id"`
	ProductName string `json:"product_name"`
	Quantity    int32  `json:"quantity"`
	UnitPrice   string `json:"unit_price"` // exact decimal string, cast SQL-side
}

type crmStockEntry struct { // aggregated decrement entry
	ID  string `json:"id"`
	Qty int32  `json:"qty"`
}

// crmResolveFulfillment ports resolveFulfillment/resolvePickupLocation
// (order-ledger.handler.ts). Defaults to DELIVERY whenever the customer
// hasn't stated a pickup choice, and never blocks order creation: a pickup
// choice whose location fails to resolve still returns PICKUP with a nil
// locationId, logged as a warning rather than rejected.
func crmResolveFulfillment(ctx context.Context, q *gen.Queries, businessID string, sig CrmSignals) (gen.FulfillmentType, pgtype.Text, error) {
	if sig.FulfillmentChoice == nil || *sig.FulfillmentChoice != "pickup" {
		return gen.FulfillmentTypeDELIVERY, pgtype.Text{}, nil
	}

	shortID := ""
	if sig.PickupLocationID != nil {
		shortID = *sig.PickupLocationID
	}
	if shortID == "" || !crmValidShortID.MatchString(shortID) {
		slog.Warn("Pickup chosen but no valid location ID was provided — order created without a location",
			"businessId", businessID)
		return gen.FulfillmentTypePICKUP, pgtype.Text{}, nil
	}

	locID, err := q.ResolvePickupLocationByShortID(ctx, gen.ResolvePickupLocationByShortIDParams{
		BusinessID: businessID,
		ID:         crmEscapeLikePattern(shortID) + "%",
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		slog.Warn("Pickup location could not be resolved — order created without a location",
			"businessId", businessID, "shortLocationId", shortID)
		return gen.FulfillmentTypePICKUP, pgtype.Text{}, nil
	case err != nil:
		return "", pgtype.Text{}, err
	}

	return gen.FulfillmentTypePICKUP, pgtype.Text{String: locID, Valid: true}, nil
}

// crmHandleOrderLedger ports OrderLedgerHandler.handleOrderConfirmation
// (order-ledger.handler.ts:60-368). Only called when order_confirmed is true
// AND detected_items has at least one entry.
func crmHandleOrderLedger(ctx context.Context, deps CRMDeps, job CRMSignalJob) error {
	detectedItems := job.CRMSignals.DetectedItems
	if len(detectedItems) == 0 {
		slog.Warn("Order confirmed but no items detected", "customerId", job.CustomerID)
		return nil
	}

	q := gen.New(deps.Pool)

	// Idempotency is handled deterministically via the database unique
	// constraint on idempotencyKey (= sourceMessageId)
	// (order-ledger.handler.ts:69).

	// 1. Resolve short product IDs to full products (:79-108).
	resolved := make([]resolvedItem, 0, len(detectedItems))
	for _, item := range detectedItems {
		if !crmValidShortID.MatchString(item.ProductID) {
			slog.Warn("Invalid product short ID rejected",
				"msg", "Invalid product short ID rejected",
				"event", "invalid_short_id",
				"shortId", item.ProductID,
				"businessId", job.BusinessID,
			)
			continue
		}

		product, err := q.ResolveProductByShortID(ctx, gen.ResolveProductByShortIDParams{
			BusinessID: job.BusinessID,
			ID:         crmEscapeLikePattern(item.ProductID) + "%",
		})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			slog.Warn("Product not found or unavailable, skipping",
				"shortId", item.ProductID, "businessId", job.BusinessID)
			continue
		case err != nil:
			return err
		}

		quantity := item.Quantity
		if quantity == 0 {
			quantity = 1 // TS `item.quantity || 1`
		}
		resolved = append(resolved, resolvedItem{
			productID:   product.ID,
			productName: product.Name,
			unitPrice:   product.Price,
			quantity:    quantity,
		})
	}

	if len(resolved) == 0 {
		slog.Error("All detected items failed resolution — no order created",
			"customerId", job.CustomerID)
		return nil
	}

	// 2. Calculate total using Decimal arithmetic (:119-123).
	totalAmount := decimal.Zero
	for _, item := range resolved {
		totalAmount = totalAmount.Add(item.unitPrice.Mul(decimal.NewFromInt32(item.quantity)))
	}

	// Tenant config up-front: the order is stamped with the business's own
	// currency and the payment link uses its callback URL (:125-137).
	businessRow, bizErr := q.GetBusinessForOrder(ctx, job.BusinessID)
	var business *gen.GetBusinessForOrderRow
	switch {
	case bizErr == nil:
		b := businessRow
		business = &b
	case errors.Is(bizErr, pgx.ErrNoRows):
		// Order still gets created; only invoicing is skipped (:336-338).
	default:
		return bizErr
	}
	orderCurrency := "GHS"
	if business != nil && business.Currency != "" {
		orderCurrency = business.Currency
	}

	// 2.5. Resolve fulfillment (order-ledger.handler.ts resolveFulfillment).
	// Pickup with an unresolvable location still creates the order (as
	// PICKUP, locationId nil) rather than blocking — consistent with how a
	// partially-unresolvable item list still orders what it can, above.
	fulfillmentType, locationID, fulErr := crmResolveFulfillment(ctx, q, job.BusinessID, job.CRMSignals)
	if fulErr != nil {
		return fulErr
	}

	// 3. Create Order + OrderItems + conditional stock decrement in ONE
	// transaction (:139-207). The decrement is the sqlc-generated
	// DecrementStockIfAvailable statement (T7.15 mandate): per-product
	// quantities are aggregated caller-side, then a single UPDATE ... FROM
	// decrements only rows holding enough stock; affected != entries aborts
	// everything before commit.
	orderID := crmNewID()
	txErr := pgx.BeginFunc(ctx, deps.Pool, func(tx pgx.Tx) error {
		tq := gen.New(tx)

		conversationID := crmText(job.ConversationID)
		idempotencyKey := crmText(job.SourceMessageID)
		if _, err := tq.InsertOrder(ctx, gen.InsertOrderParams{
			ID:              orderID,
			BusinessID:      job.BusinessID,
			CustomerID:      job.CustomerID,
			ConversationID:  conversationID,
			IdempotencyKey:  idempotencyKey,
			TotalAmount:     totalAmount,
			Currency:        orderCurrency,
			FulfillmentType: fulfillmentType,
			LocationID:      locationID,
		}); err != nil {
			return err
		}

		items := make([]crmOrderItemRow, len(resolved))
		for i, item := range resolved {
			items[i] = crmOrderItemRow{
				ID:          crmNewID(),
				OrderID:     orderID,
				ProductID:   item.productID,
				ProductName: item.productName,
				Quantity:    item.quantity,
				UnitPrice:   item.unitPrice.String(),
			}
		}
		itemsJSON, err := json.Marshal(items)
		if err != nil {
			return err
		}
		if err := tq.InsertOrderItems(ctx, itemsJSON); err != nil {
			return err
		}

		// Aggregate quantities per product first so duplicate line-items for
		// the same product are summed (:170-178).
		qtyByProduct := make(map[string]int64, len(resolved))
		var entries []crmStockEntry
		for _, item := range resolved {
			if _, seen := qtyByProduct[item.productID]; !seen {
				entries = append(entries, crmStockEntry{ID: item.productID})
			}
			qtyByProduct[item.productID] += int64(item.quantity)
		}
		for i := range entries {
			entries[i].Qty = int32(qtyByProduct[entries[i].ID])
		}
		entriesJSON, err := json.Marshal(entries)
		if err != nil {
			return err
		}

		affected, err := tq.DecrementStockIfAvailable(ctx, entriesJSON)
		if err != nil {
			return err
		}
		if affected != int64(len(entries)) {
			return errCRMInsufficientStock
		}
		return nil
	})

	switch {
	case txErr == nil:
		// committed
	case isCRMDuplicateIdempotency(txErr):
		// P2002 + meta.target includes 'idempotency_key'
		// (order-ledger.handler.ts:209-217).
		slog.Warn("Order creation skipped — duplicate idempotencyKey detected",
			"msg", "Order creation skipped — duplicate idempotencyKey detected",
			"event", "order_duplicate_skipped",
			"sourceMessageId", job.SourceMessageID,
			"customerId", job.CustomerID,
		)
		return nil
	case errors.Is(txErr, errCRMInsufficientStock):
		// Warn + swallow: the job completes, nothing was written
		// (order-ledger.handler.ts:218-227).
		slog.Warn("Order rolled back — insufficient stock for one or more items",
			"event", "order_insufficient_stock",
			"customerId", job.CustomerID,
		)
		return nil
	default:
		return txErr
	}

	slog.Info("Order created",
		"orderId", orderID,
		"customerId", job.CustomerID,
		"totalAmount", totalAmount.StringFixed(2),
		"itemCount", len(resolved),
	)

	// 4. Update CustomerProfile stats (:239-240).
	if err := crmUpdateProfileStats(ctx, deps.Pool, job.CustomerID, totalAmount); err != nil {
		return err
	}

	// 5. Generate Payment Link and Send Invoice (:242-338).
	if business != nil {
		paymentURL, payErr := crmInitiatePaymentLink(ctx, deps.Paystack, *business, job, orderID, totalAmount)
		if payErr != nil {
			return payErr
		}

		if paymentURL != "" && job.CustomerPhone != "" && business.WhatsappPhoneNumberID != "" {
			methods := crmCurrencyConfig(business.Currency).PaymentMethods
			paymentText := fmt.Sprintf(
				"Thank you for confirming your order!\n\nYour total is *%s %s*.\n\nTap the link below to pay securely with %s:\n%s",
				business.Currency, totalAmount.StringFixed(2), methods, paymentURL,
			)

			rawPayload, mErr := json.Marshal(map[string]any{
				"messaging_product": "whatsapp",
				"to":                job.CustomerPhone,
				"type":              "text",
				"text":              map[string]string{"body": paymentText},
			})
			if mErr != nil {
				return mErr
			}

			outboundMsg, err := q.InsertOutboundMessage(ctx, gen.InsertOutboundMessageParams{
				ID:             crmNewID(),
				RecipientPhone: job.CustomerPhone,
				MessageType:    "text",
				TextContent:    pgtype.Text{String: paymentText, Valid: true},
				RawPayload:     rawPayload,
				Status:         "pending",
				BusinessID:     crmText(job.BusinessID),
				ConversationID: conversationIDText(job.ConversationID),
			})
			if err != nil {
				return err
			}

			// Route through outbound-queue to guarantee ordering after the LLM
			// reply (:279-299).
			if deps.Publisher != nil {
				if err := queue.PublishOutbound(ctx, deps.Publisher, outboundMsg.ID); err != nil {
					return err
				}
			}

			// Update conversation state to INVOICING via the state machine.
			// Fetch current state and use CAS to prevent stomping a concurrent
			// transition (:301-326).
			if job.ConversationID != "" {
				conv, err := q.GetConversationStateByID(ctx, job.ConversationID)
				switch {
				case errors.Is(err, pgx.ErrNoRows):
					// Conversation vanished between turns; nothing to transition.
				case err != nil:
					return err
				default:
					currentState := domain.ConversationState(conv)
					if currentState != domain.StateInvoicing && currentState != domain.StateEscalated {
						ok, terr := domain.Transition(ctx, deps.Pool, job.ConversationID, currentState, domain.StateInvoicing)
						if terr != nil {
							return terr
						}
						if !ok {
							slog.Warn("State transition to INVOICING rejected (non-blocking)",
								"conversationId", job.ConversationID)
						}
					}
				}
			}

			slog.Info("Payment link enqueued to outbound queue",
				"orderId", orderID, "outboundMessageId", outboundMsg.ID)
		} else if paymentURL == "" {
			slog.Error("Skipped sending payment link — URL was empty", "orderId", orderID)
		} else if job.CustomerPhone == "" {
			slog.Error("Skipped sending payment link — customerPhone missing", "orderId", orderID)
		} else {
			slog.Error("Skipped sending payment link — whatsappPhoneNumberId missing", "orderId", orderID)
		}
	} else {
		slog.Error("Business not found — cannot generate payment link",
			"businessId", job.BusinessID)
	}

	// 6. Schedule follow-ups durably in the database (:340-367): abandoned-cart
	// after 2h, first unpaid-invoice reminder after 24h, second unpaid-invoice
	// reminder after 48h. Errors are logged and swallowed — the order itself
	// is already durable.
	now := time.Now().UTC()
	err := q.InsertScheduledFollowUpPair(ctx, gen.InsertScheduledFollowUpPairParams{
		ID: crmNewID(), OrderID: orderID, BusinessID: job.BusinessID,
		CustomerID: job.CustomerID, JobType: "abandoned-cart",
		ScheduledAt: pgtype.Timestamp{Time: now.Add(2 * time.Hour), Valid: true},

		ID_2:          crmNewID(),
		OrderID_2:     orderID,
		BusinessID_2:  job.BusinessID,
		CustomerID_2:  job.CustomerID,
		JobType_2:     "unpaid-invoice-first",
		ScheduledAt_2: pgtype.Timestamp{Time: now.Add(24 * time.Hour), Valid: true},
	})
	if err != nil {
		slog.Error("Failed to schedule follow-ups", "error", err.Error())
	} else if err := q.InsertScheduledFollowUp(ctx, gen.InsertScheduledFollowUpParams{
		ID: crmNewID(), OrderID: orderID, BusinessID: job.BusinessID,
		CustomerID: job.CustomerID, JobType: "unpaid-invoice-second",
		ScheduledAt: pgtype.Timestamp{Time: now.Add(48 * time.Hour), Valid: true},
	}); err != nil {
		slog.Error("Failed to schedule follow-ups", "error", err.Error())
	}

	return nil
}

// conversationIDText exists purely to keep InsertOutboundMessage call sites
// readable; it widens an optional string into pgtype.Text.
func conversationIDText(id string) pgtype.Text { return crmText(id) }

// isCRMDuplicateIdempotency maps Prisma's P2002-on-idempotency_key branch onto
// Postgres: unique_violation raised by orders_idempotency_key_key
// (order-ledger.handler.ts:209).
func isCRMDuplicateIdempotency(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23505" && strings.Contains(pgErr.ConstraintName, "idempotency_key")
}

// crmInitiatePaymentLink ports the provider block
// (order-ledger.handler.ts:244-270): initiate with major-unit amount,
// fall back to placeholder phone/callback like the source, and treat any
// failure as "no payment link generated".
func crmInitiatePaymentLink(ctx context.Context, initiator PaystackInitiator, business gen.GetBusinessForOrderRow, job CRMSignalJob, orderID string, totalAmount decimal.Decimal) (string, error) {
	if initiator == nil {
		slog.Error("Payment provider does not support initiatePayment", "orderId", orderID)
		return "", nil
	}

	phone := job.CustomerPhone
	if phone == "" {
		phone = "0000000000"
	}
	callbackURL := "https://novoapex.com/success"
	if business.PaymentCallbackUrl.Valid && business.PaymentCallbackUrl.String != "" {
		callbackURL = business.PaymentCallbackUrl.String
	}
	currency := business.Currency
	if currency == "" {
		currency = "GHS"
	}

	result, err := initiator.InitiatePayment(ctx, PaymentRequest{
		Amount:        totalAmount,
		Currency:      currency,
		CustomerPhone: phone,
		Reference:     orderID,
		CallbackURL:   callbackURL,
		BusinessID:    job.BusinessID,
	})
	if err != nil {
		// The Node provider catches transport errors and returns status
		// "failed"; mirror that instead of failing the whole job.
		slog.Error("Paystack initiation failed", "orderId", orderID, "error", err.Error())
		return "", nil
	}
	if result.Status == "initiated" && result.PaymentURL != "" {
		slog.Info("Payment link generated", "orderId", orderID, "paymentUrl", result.PaymentURL)
		return result.PaymentURL, nil
	}
	slog.Error("Payment initiation failed — no payment link generated",
		"orderId", orderID,
		"paymentStatus", result.Status,
		"providerReference", result.ProviderReference,
	)
	return "", nil
}

// crmUpdateProfileStats ports updateProfileStats
// (order-ledger.handler.ts:407-449):
//
//	newTotalOrders      = profile.totalOrders + 1
//	newTotalSpent       = profile.totalSpent + orderAmount        (exact decimal)
//	newAverageOrderValue = newTotalSpent / newTotalOrders         (DECIMAL div)
//	orderFrequencyDays  = daysSinceFirstContact / (newTotalOrders - 1)
//	                       — only when totalOrders > 1 and the customer row
//	                       carries firstContactAt; NULL otherwise
//	lastOrderAt         = now
//
// A missing profile is silently skipped (`if (!profile) return`).
func crmUpdateProfileStats(ctx context.Context, pool *pgxpool.Pool, customerID string, orderAmount decimal.Decimal) error {
	q := gen.New(pool)

	profile, err := q.GetCustomerProfileByCustomerID(ctx, customerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		return err
	}

	newTotalOrders := profile.TotalOrders + 1
	newTotalSpent := profile.TotalSpent.Add(orderAmount)
	newAverageOrderValue := newTotalSpent.Div(decimal.NewFromInt(int64(newTotalOrders)))

	// Calculate order frequency: days between first contact and now / total
	// orders (:418-429).
	var orderFrequencyDays pgtype.Float8
	if newTotalOrders > 1 {
		firstContactAt, cerr := q.GetCustomerFirstContactAt(ctx, customerID)
		switch {
		case errors.Is(cerr, pgx.ErrNoRows):
			// leave NULL, matching the source's `customer &&` guard
		case cerr != nil:
			return cerr
		default:
			if firstContactAt.Valid {
				daysSinceFirstContact := time.Since(firstContactAt.Time).Hours() / 24
				orderFrequencyDays = pgtype.Float8{
					Float64: daysSinceFirstContact / float64(newTotalOrders-1),
					Valid:   true,
				}
			}
		}
	}

	_, uerr := q.UpdateProfileOrderStats(ctx, gen.UpdateProfileOrderStatsParams{
		TotalOrders:        newTotalOrders,
		TotalSpent:         newTotalSpent,
		AverageOrderValue:  crmDecimalToNumeric(newAverageOrderValue),
		OrderFrequencyDays: orderFrequencyDays,
		CustomerID:         customerID,
	})
	if uerr != nil {
		return uerr
	}

	slog.Info("Customer profile stats updated",
		"customerId", customerID,
		"totalOrders", newTotalOrders,
		"totalSpent", newTotalSpent.StringFixed(2),
		"averageOrderValue", newAverageOrderValue.StringFixed(2),
	)
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

// crmCurrencyConfig is the internal alias used by the invoice copy path.
func crmCurrencyConfig(currency string) CurrencyDisplayConfig { return GetCurrencyConfig(currency) }
