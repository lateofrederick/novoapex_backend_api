// Stage 8b orchestrator response-side helpers (T8.21–T8.25), the 24h-window
// outbound persistence shared by every reply path, currency display config,
// and the HTTP-less transcript driver glue for the golden-transcript harness.
// Source references point at
// libs/orchestrator/src/conversation-orchestrator.service.ts.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// ---------------------------------------------------------------------------
// shown-images context + image gating (T8.21)
// ---------------------------------------------------------------------------

// orchAlreadyShownProducts ports getAlreadyShownProducts (:150-172):
// window-blocked and failed sends are excluded — the customer never received
// those, so they must not suppress a later legitimate send.
func orchAlreadyShownProducts(ctx context.Context, pool *pgxpool.Pool, conversationID string) (map[string]string, error) {
	rows, err := pool.Query(ctx,
		`SELECT product_id, COALESCE(text_content,'') FROM outbound_messages
		 WHERE conversation_id = $1 AND message_type = 'image'
		   AND product_id IS NOT NULL
		   AND status NOT IN ('failed','failed_24h_window_closed')
		 ORDER BY created_at ASC`, conversationID)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: shown products: %w", err)
	}
	defer rows.Close()
	shown := map[string]string{}
	for rows.Next() {
		var productID, caption string
		if err := rows.Scan(&productID, &caption); err != nil {
			return nil, fmt.Errorf("orchestrator: scan shown products: %w", err)
		}
		name := strings.TrimSpace(strings.SplitN(caption, "—", 2)[0]) // caption "Name — GH₵45.00"
		if len(productID) >= 8 {
			shown[productID[:8]] = name
		}
	}
	return shown, rows.Err()
}

// orchBuildShownImagesContext ports buildShownImagesContext (:181-195).
func orchBuildShownImagesContext(shown map[string]string) string {
	if len(shown) == 0 {
		return ""
	}
	shortIDs := make([]string, 0, len(shown))
	for id := range shown {
		shortIDs = append(shortIDs, id)
	}
	sort.Strings(shortIDs)
	lines := make([]string, 0, len(shortIDs))
	for _, id := range shortIDs {
		line := "[ID: " + id + "]"
		if name := shown[id]; name != "" {
			line += " " + name
		}
		lines = append(lines, line)
	}
	return "\n=== IMAGES ALREADY SENT ===\n" +
		"The customer has already been sent photos of these products:\n" +
		strings.Join(lines, "\n") +
		"\nDo NOT send these images again. Refer to them in words instead " +
		"(\"the one you saw\"), unless the customer explicitly asks to see one again.\n"
}

// orchGateProductImageIDs ports gateProductImageIds (:217-240): the single
// rule is EXPLICIT REQUEST THIS TURN — send history only informs the prompt,
// never the gate; repeat sends on request are allowed.
func orchGateProductImageIDs(requested []string, customerRequestedImages bool, alreadyShown map[string]string, conversationID string) []string {
	if len(requested) == 0 {
		return nil
	}
	if !customerRequestedImages {
		slog.Info("Suppressed unsolicited product image(s)",
			"conversationId", conversationID,
			"requested", strings.Join(requested, ", "))
		return nil
	}
	repeats := make([]string, 0, len(requested))
	for _, id := range requested {
		if _, seen := alreadyShown[id]; seen {
			repeats = append(repeats, id)
		}
	}
	if len(repeats) > 0 {
		slog.Info("Re-sending already-seen product image(s)",
			"conversationId", conversationID, "repeats", strings.Join(repeats, ", "))
	}
	return requested
}

// ---------------------------------------------------------------------------
// product-image sends (T8.22)
// ---------------------------------------------------------------------------

// orchSendProductImage ports sendProductImages (:242-291) for one short ID:
// validate, resolve primary image, format the price caption, persist the
// image row with productId.
func orchSendProductImage(ctx context.Context, deps OrchestratorDeps, oc orchContext, shortID string) error {
	if !orchValidShortID.MatchString(shortID) { // :250-256 LLM output untrusted
		slog.Warn("Invalid product short ID rejected in sendProductImages.", "shortId", shortID)
		return nil
	}
	var productID, name, priceText string
	err := deps.Pool.QueryRow(ctx,
		`SELECT id, name, price::text FROM products
		 WHERE id LIKE $1 || '%' AND business_id = $2 ORDER BY id LIMIT 1`,
		shortID, oc.businessID).Scan(&productID, &name, &priceText)
	if err != nil {
		slog.Warn("Product not found or has no image. Skipping image send.", "shortId", shortID)
		return nil // non-fatal skip (:270-273)
	}
	price, err := decimal.NewFromString(priceText)
	if err != nil {
		return fmt.Errorf("orchestrator: parse price %s: %w", priceText, err)
	}
	var imageURL *string
	err = deps.Pool.QueryRow(ctx,
		`SELECT url FROM product_images WHERE product_id = $1 ORDER BY position ASC LIMIT 1`,
		productID).Scan(&imageURL)
	if err != nil || imageURL == nil {
		slog.Warn("Product not found or has no image. Skipping image send.", "productId", productID)
		return nil
	}
	cfg := orchCurrencyFor(oc.currency)
	caption := fmt.Sprintf("%s — %s%s", name, cfg.Symbol, price.StringFixed(2)) // :277 toFixed(2)
	return orchEnqueueOutboundImage(ctx, deps, oc.businessID, oc.conversationID, *imageURL, caption, productID)
}

// ---------------------------------------------------------------------------
// 24h-window outbound persistence (T8.23)
// ---------------------------------------------------------------------------

type outboundRow struct {
	id             string
	businessID     string
	conversationID string
	messageType    string
	textContent    *string
	imageURL       *string
	productID      *string
	rawPayload     []byte
	status         string
}

// orchEnqueueOutboundText ports enqueueOutboundMessage (:28-81); the 24-hour
// rule keys off the NEWEST inbound row of the conversation. Blocked sends
// still persist a failed_24h_window_closed row with rawPayload {}.
func orchEnqueueOutboundText(ctx context.Context, deps OrchestratorDeps, businessID, conversationID, textContent string) error {
	outID := uuid.NewString()
	within, err := orchWithin24hWindow(ctx, deps.Pool, conversationID)
	if err != nil {
		return err
	}
	if !within {
		slog.Warn("Compliance Block: Cannot send free-form message - 24h window closed.",
			"conversationId", conversationID)
		return orchInsertOutboundRow(ctx, deps.Pool, outboundRow{
			id: outID, businessID: businessID, conversationID: conversationID,
			messageType: "text", textContent: ptr(textContent),
			rawPayload: []byte("{}"), status: "failed_24h_window_closed",
		})
	}
	raw, err := json.Marshal(map[string]any{
		"messaging_product": "whatsapp",
		"type":              "text",
		"text":              map[string]string{"body": textContent},
	})
	if err != nil {
		return fmt.Errorf("orchestrator: marshal text payload: %w", err)
	}
	if err := orchInsertOutboundRow(ctx, deps.Pool, outboundRow{
		id: outID, businessID: businessID, conversationID: conversationID,
		messageType: "text", textContent: ptr(textContent),
		rawPayload: raw, status: "pending",
	}); err != nil {
		return err
	}
	return orchPublishOutbound(ctx, deps.Publisher, outID)
}

// orchEnqueueOutboundImage ports enqueueOutboundImageMessage (:83-141).
func orchEnqueueOutboundImage(ctx context.Context, deps OrchestratorDeps, businessID, conversationID, imageUrl, caption, productID string) error {
	outID := uuid.NewString()
	within, err := orchWithin24hWindow(ctx, deps.Pool, conversationID)
	if err != nil {
		return err
	}
	if !within {
		slog.Warn("Compliance Block: Cannot send image message - 24h window closed.",
			"conversationId", conversationID)
		return orchInsertOutboundRow(ctx, deps.Pool, outboundRow{
			id: outID, businessID: businessID, conversationID: conversationID,
			messageType: "image", textContent: ptr(caption),
			imageURL: ptr(imageUrl), productID: ptr(productID),
			rawPayload: []byte("{}"), status: "failed_24h_window_closed",
		})
	}
	raw, err := json.Marshal(map[string]any{
		"messaging_product": "whatsapp",
		"type":              "image",
		"image":             map[string]string{"link": imageUrl, "caption": caption},
	})
	if err != nil {
		return fmt.Errorf("orchestrator: marshal image payload: %w", err)
	}
	if err := orchInsertOutboundRow(ctx, deps.Pool, outboundRow{
		id: outID, businessID: businessID, conversationID: conversationID,
		messageType: "image", textContent: ptr(caption),
		imageURL: ptr(imageUrl), productID: ptr(productID),
		rawPayload: raw, status: "pending",
	}); err != nil {
		return err
	}
	return orchPublishOutbound(ctx, deps.Publisher, outID)
}

func orchInsertOutboundRow(ctx context.Context, pool *pgxpool.Pool, r outboundRow) error {
	if _, err := pool.Exec(ctx,
		`INSERT INTO outbound_messages
		   (id, recipient_phone, message_type, text_content, image_url, product_id,
		    raw_payload, status, business_id, conversation_id, created_at)
		 VALUES ($1,
		         (SELECT customer_phone FROM conversations WHERE id = $2),
		         $3, $4, $5, $6, $7, $8, $9, $2, CURRENT_TIMESTAMP)`,
		r.id, r.conversationID, r.messageType, r.textContent, r.imageURL, r.productID,
		r.rawPayload, r.status, r.businessID); err != nil {
		return fmt.Errorf("orchestrator: insert outbound row: %w", err)
	}
	return nil
}

func orchPublishOutbound(ctx context.Context, pub queue.Publisher, outboundMessageID string) error {
	return queue.PublishOutbound(ctx, pub, outboundMessageID)
}

func orchWithin24hWindow(ctx context.Context, pool *pgxpool.Pool, conversationID string) (bool, error) {
	var latest *time.Time
	err := pool.QueryRow(ctx,
		`SELECT max("timestamp") FROM inbound_messages WHERE conversation_id = $1`,
		conversationID).Scan(&latest)
	if err != nil {
		return false, fmt.Errorf("orchestrator: window check %s: %w", conversationID, err)
	}
	return latest != nil && time.Since(*latest) < orch24hWindow, nil
}

// ---------------------------------------------------------------------------
// escalation + CRM emission (T8.19/T8.20/T8.25)
// ---------------------------------------------------------------------------

func orchSetEscalated(ctx context.Context, pool *pgxpool.Pool, conversationID, reason string) error {
	_, err := pool.Exec(ctx,
		`UPDATE conversations SET is_escalated_to_human = true, escalation_reason = $2,
		 updated_at = CURRENT_TIMESTAMP WHERE id = $1`,
		conversationID, reason)
	if err != nil {
		return fmt.Errorf("orchestrator: escalate %s (%s): %w", conversationID, reason, err)
	}
	return nil
}

// orchEscalate sets flag+reason then enqueues the notice through the normal
// 24h-window path (source: enforceSafetyGuardrails :459-470).
func orchEscalate(ctx context.Context, deps OrchestratorDeps, conversationID, reason, notice string) error {
	if err := orchSetEscalated(ctx, deps.Pool, conversationID, reason); err != nil {
		return err
	}
	var businessID string
	if err := deps.Pool.QueryRow(ctx,
		`SELECT business_id FROM conversations WHERE id = $1`, conversationID).Scan(&businessID); err != nil {
		return fmt.Errorf("orchestrator: escalate conversation lookup %s: %w", conversationID, err)
	}
	return orchEnqueueOutboundText(ctx, deps, businessID, conversationID, notice)
}

// orchCRMSignalJob builds the S7B wire shape (crm_materialiser.go CRMSignalJob:
// camelCase top level, snake_case signals) with sourceMessageId as the
// deterministic order-idempotency key.
func orchCRMSignalJob(oc orchContext, latest *orchLatest, resp orchestrator.LlmResponse) CRMSignalJob {
	items := make([]DetectedItem, len(resp.CrmSignals.DetectedItems))
	for i, it := range resp.CrmSignals.DetectedItems {
		items[i] = DetectedItem{ProductID: it.ProductID, Quantity: int32(it.Quantity)}
	}
	source := ""
	if latest != nil {
		source = latest.whatsappID
	}
	deliveryArea := resp.CrmSignals.DeliveryArea
	customerName := resp.CrmSignals.CustomerName
	sentiment := resp.CrmSignals.Sentiment
	return CRMSignalJob{
		BusinessID:      oc.businessID,
		CustomerID:      oc.customerID,
		ConversationID:  oc.conversationID,
		CustomerPhone:   oc.customerPhone,
		SourceMessageID: source,
		CRMSignals: CrmSignals{
			OrderConfirmed:      resp.CrmSignals.OrderConfirmed,
			DetectedItems:       items,
			DeliveryArea:        deliveryArea,
			FulfillmentChoice:   resp.CrmSignals.FulfillmentChoice,
			PickupLocationID:    resp.CrmSignals.PickupLocationID,
			CustomerName:        customerName,
			DetectedPreferences: resp.CrmSignals.DetectedPreferences,
			WantsUpdates:        resp.CrmSignals.WantsUpdates,
			Sentiment:           &sentiment,
		},
		LLMIntent:            resp.Intent,
		ReferencedProductIDs: resp.ReferencedProductIDs,
		Timestamp:            time.Now().UTC().Format(time.RFC3339),
	}
}

// orchCheckoutJob builds the checkout payload from a confirmed turn: only the
// order-relevant fields. The CRM enrichment signal travels separately.
func orchCheckoutJob(oc orchContext, latest *orchLatest, resp orchestrator.LlmResponse) CheckoutJob {
	items := make([]DetectedItem, len(resp.CrmSignals.DetectedItems))
	for i, it := range resp.CrmSignals.DetectedItems {
		items[i] = DetectedItem{ProductID: it.ProductID, Quantity: int32(it.Quantity)}
	}
	source := ""
	if latest != nil {
		source = latest.whatsappID
	}
	return CheckoutJob{
		BusinessID:        oc.businessID,
		CustomerID:        oc.customerID,
		ConversationID:    oc.conversationID,
		CustomerPhone:     oc.customerPhone,
		SourceMessageID:   source,
		DetectedItems:     items,
		FulfillmentChoice: resp.CrmSignals.FulfillmentChoice,
		PickupLocationID:  resp.CrmSignals.PickupLocationID,
	}
}

// ---------------------------------------------------------------------------
// currency display config (currency.config.ts port for message copy)
// ---------------------------------------------------------------------------

// ResolvedConversation is the exported view of resolveConversationContext for
// callers that only need identity linkage (harness drivers, tests).
type ResolvedConversation struct {
	BusinessID     string
	Currency       string
	CustomerID     string
	ConversationID string
}

// ResolveConversationContext exposes the T8.18 upsert phase.
func ResolveConversationContext(ctx context.Context, pool *pgxpool.Pool, recipientPhone, senderPhone string) (ResolvedConversation, bool, error) {
	oc, ok, err := orchResolveConversationContext(ctx, pool, recipientPhone, senderPhone)
	return ResolvedConversation{
		BusinessID:     oc.businessID,
		Currency:       oc.currency,
		CustomerID:     oc.customerID,
		ConversationID: oc.conversationID,
	}, ok, err
}

type orchCurrencyDisplay struct {
	Symbol         string
	PaymentMethods string
}

var orchCurrencyConfigs = map[string]orchCurrencyDisplay{
	"GHS": {Symbol: "GH₵", PaymentMethods: "Mobile Money, card, or bank transfer"},
	"NGN": {Symbol: "₦", PaymentMethods: "card, bank transfer, or USSD"},
	"USD": {Symbol: "$", PaymentMethods: "card"},
}

func orchCurrencyFor(code string) orchCurrencyDisplay {
	if c, ok := orchCurrencyConfigs[code]; ok {
		return c
	}
	return orchCurrencyConfigs["GHS"] // getCurrencyConfig fallback (:38)
}

// orchFavoriteProduct ports getFavoriteProduct
// (conversation-orchestrator.service.ts): the product this customer has
// bought the most of, by total quantity across their order history. Uses the
// immutable order_items.product_name snapshot rather than joining to the
// live product row, so a since-renamed or deleted product still resolves to
// what the customer actually bought. Only worth calling for a returning
// customer — callers gate this on profileTotalOrders > 0.
func orchFavoriteProduct(ctx context.Context, pool *pgxpool.Pool, customerID string) (string, error) {
	var name string
	err := pool.QueryRow(ctx, `
		SELECT oi.product_name
		FROM order_items oi
		JOIN orders o ON o.id = oi.order_id
		WHERE o.customer_id = $1
		GROUP BY oi.product_name
		ORDER BY SUM(oi.quantity) DESC
		LIMIT 1;`, customerID).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("orchestrator: favorite product for customer %s: %w", customerID, err)
	}
	return name, nil
}

// orchBuildCustomerContext ports buildCustomerContext
// (conversation-orchestrator.service.ts): renders accumulated CRM profile
// data as context for the LLM, so the "stealth CRM" actually personalises
// replies instead of just recording data nobody reads.
//
// The name line is independent of order history — a lead who introduced
// themselves but hasn't ordered yet should still be greeted by name. The
// order-history lines (order count, favorite product, delivery area) stay
// gated on profileTotalOrders > 0, since there is nothing there to report
// otherwise. Empty only when neither a name nor any order history is known.
func orchBuildCustomerContext(oc orchContext, favoriteProduct string) string {
	isReturning := oc.profileTotalOrders > 0
	if oc.customerName == "" && !isReturning {
		return ""
	}

	var lines []string
	if oc.customerName != "" {
		lines = append(lines, "Name: "+oc.customerName+".")
	}
	if isReturning {
		lines = append(lines, fmt.Sprintf("Returning customer — %d previous order(s).", oc.profileTotalOrders))
		if favoriteProduct != "" {
			lines = append(lines, "Most frequently ordered: "+favoriteProduct+".")
		}
		if oc.profileDeliveryArea != "" {
			lines = append(lines, "Known delivery area: "+oc.profileDeliveryArea+".")
		}
	}

	return "\n=== CUSTOMER CONTEXT ===\n" + strings.Join(lines, "\n") + "\n"
}

func ptr[T any](v T) *T { return &v }

func orchDeref(s *string, fallback string) string {
	if s == nil || *s == "" {
		return fallback
	}
	return *s
}

// ---------------------------------------------------------------------------
// transcript driver glue (used by internal/harness golden-transcript runner)
// ---------------------------------------------------------------------------

// TranscriptWebhookDriver returns an HTTP-less Go-stack driver for the golden
// transcript harness: it persists the scripted webhook payload as an inbound
// row (the webhook-processing stage's persistence contract), runs one
// debounced orchestrator pass including the completion re-check, then drains
// emitted CRM jobs synchronously so durable effects settle immediately.
func TranscriptWebhookDriver(orch OrchestratorDeps, crm CRMDeps) func(ctx context.Context, payload []byte) error {
	return func(ctx context.Context, payload []byte) error {
		msg, recipient, err := orchParseWebhookPayload(payload)
		if err != nil {
			return err
		}
		if _, err := poolExec(ctx, orch.Pool,
			`INSERT INTO inbound_messages
			   (id, whatsapp_message_id, sender_phone, recipient_phone, message_type,
			    text_content, raw_payload, timestamp, created_at)
			 VALUES ($1, $2, $3, $4, $5, NULLIF($6,''), $7::jsonb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
			uuid.NewString(), msg.id, msg.from, recipient, msg.msgType, msg.text, msg.raw); err != nil {
			return err
		}

		capture := newCapturingPublisher(orch.Publisher)
		deps := orch
		deps.Publisher = capture
		job := OrchestratorJob{RecipientPhone: recipient, SenderPhone: msg.from}
		mark, err := inboundMark(ctx, deps.Pool, job)
		if err != nil {
			return err
		}
		if err := HandleDebouncedConversation(ctx, deps, job); err != nil {
			return err
		}
		if err := reenqueueIfNewerInbound(ctx, deps, job, mark); err != nil {
			return err
		}
		for _, j := range capture.drain() {
			if j.taskType != queue.TaskCRMProcess {
				continue
			}
			var cj CRMSignalJob
			if err := json.Unmarshal(j.payload, &cj); err != nil {
				return fmt.Errorf("transcript driver: decode crm job: %w", err)
			}
			if err := HandleCRMSignals(ctx, crm, cj); err != nil {
				return fmt.Errorf("transcript driver: crm materialise: %w", err)
			}
		}
		return nil
	}
}

// capturingPublisher wraps a Publisher recording every enqueue so test
// drivers can drain downstream stages synchronously.
type capturingPublisher struct {
	next queue.Publisher
	jobs chan capturedJob
}

type capturedJob struct {
	queue    string
	taskType string
	payload  []byte
	opts     *queue.EnqueueOpts
}

func newCapturingPublisher(next queue.Publisher) *capturingPublisher {
	return &capturingPublisher{next: next, jobs: make(chan capturedJob, 128)}
}

func (c *capturingPublisher) Enqueue(ctx context.Context, q, taskType string, payload any, opts *queue.EnqueueOpts) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("capturing publisher: marshal %s payload: %w", taskType, err)
	}
	select {
	case c.jobs <- capturedJob{queue: q, taskType: taskType, payload: body, opts: opts}:
	default:
		return fmt.Errorf("capturing publisher: buffer overflow")
	}
	return c.next.Enqueue(ctx, q, taskType, payload, opts)
}

func (c *capturingPublisher) drain() []capturedJob {
	var out []capturedJob
	for {
		select {
		case j := <-c.jobs:
			out = append(out, j)
		default:
			return out
		}
	}
}

var orchWaEnvelope = regexp.MustCompile(`(?s)^\s*\{`)

type orchWebhookMessage struct {
	id      string
	from    string
	msgType string
	text    string
	raw     string
}

func orchParseWebhookPayload(payload []byte) (orchWebhookMessage, string, error) {
	var doc struct {
		Entry []struct {
			Changes []struct {
				Value struct {
					Metadata struct {
						PhoneNumberID string `json:"phone_number_id"`
					} `json:"metadata"`
					Messages []struct {
						ID   string          `json:"id"`
						From string          `json:"from"`
						Type string          `json:"type"`
						Text json.RawMessage `json:"text,omitempty"`
					} `json:"messages"`
				} `json:"value"`
			} `json:"changes"`
		} `json:"entry"`
	}
	if !orchWaEnvelope.Match(payload) {
		return orchWebhookMessage{}, "", fmt.Errorf("transcript driver: not a webhook JSON payload")
	}
	if err := json.Unmarshal(payload, &doc); err != nil {
		return orchWebhookMessage{}, "", fmt.Errorf("transcript driver: decode webhook: %w", err)
	}
	if len(doc.Entry) == 0 || len(doc.Entry[0].Changes) == 0 {
		return orchWebhookMessage{}, "", fmt.Errorf("transcript driver: empty webhook payload")
	}
	value := doc.Entry[0].Changes[0].Value
	if len(value.Messages) == 0 {
		return orchWebhookMessage{}, "", fmt.Errorf("transcript driver: no messages in webhook payload")
	}
	m := value.Messages[0]
	rawMsg := map[string]any{}
	_ = json.Unmarshal(payload, &rawMsg)
	msgObj := map[string]any{"id": m.ID, "from": m.From, "type": m.Type}
	if len(m.Text) > 0 {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.Unmarshal(m.Text, &body)
		msgObj["text"] = map[string]string{"body": body.Body}
	}
	raw, _ := json.Marshal(msgObj)
	text := ""
	if m.Type == "text" && len(m.Text) > 0 {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.Unmarshal(m.Text, &body)
		text = body.Body
	}
	return orchWebhookMessage{id: m.ID, from: m.From, msgType: m.Type, text: text, raw: string(raw)},
		value.Metadata.PhoneNumberID, nil
}

func poolExec(ctx context.Context, pool *pgxpool.Pool, query string, args ...any) (any, error) {
	tag, err := pool.Exec(ctx, query, args...)
	return tag, err
}
