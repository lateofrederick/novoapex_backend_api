// Stage 8b conversation pipeline worker (T8.14, T8.17–T8.28): the port of
// libs/orchestrator/src/conversation-orchestrator.service.ts plus the
// debounce wrapper from libs/queue/src/processors/orchestrator.processor.ts.
//
// Phase order (handleConversation, conversation-orchestrator.service.ts:293-324):
//  1. resolveConversationContext (:326-384) — business lookup, customer
//     upsert w/ nested profile create, conversation upsert, inbound backfill
//  2. latest-message fetch (:301-304)
//  3. processMediaPayload (:386-444) — audio/image handling; media
//     collaborators are injectable (T8.15/T8.16 own the real services)
//  4. enforceSafetyGuardrails (:446-475) — already-escalated pause +
//     keyword fast-path (BEFORE history/LLM)
//  5. fetchConversationHistory (:477-509) — last 10+10 merged/sorted/
//     sliced/filtered
//  6. catalog context + already-shown products + LLM call (:511-537)
//  7. response handling (:539-644): low-confidence flag-only escalation →
//     image gating → image sends → text reply → intent/state transitions →
//     CRM signal emission
//
// Delta vs brief: no WA sender / MediaURLResolver dep exists here because the
// source never touches WhatsApp directly — enqueueOutboundMessage only writes
// an outbound_messages row and hands {outboundMessageId} to the outbound
// queue (:59-81); delivery belongs to Stage 8c's outbound worker.
package workers

import (
	"context"
	"encoding/base64"
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

	"github.com/novoapex/novoapex-backend-api/internal/domain"
	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

const (
	// OrchestratorDebounceDelay mirrors `delay: 3000`
	// (webhook.processor.ts:158, orchestrator.processor.ts:85).
	OrchestratorDebounceDelay = 3 * time.Second

	orch24hWindow       = 24 * time.Hour
	orchMaxAudioBytes   = 10 * 1024 * 1024 // :408
	orchConfidenceFloor = 0.7              // :539
	orchHistoryPerSide  = 10               // :481/:487

	orchKeywordReason   = "KEYWORD_MATCH"           // :461
	orchLowConfReason   = "LOW_CONFIDENCE"          // :543
	orchAudioLongReason = "AUDIO_TOO_LONG"          // :412
	orchMediaFailReason = "MEDIA_PROCESSING_FAILED" // :431

	orchConnectingMsg = "You are being connected to a human agent. Please wait."           // :468/:549
	orchAudioLongMsg  = "Voice note too long, escalating..."                               // :418
	orchMediaFailMsg  = "I had trouble processing your media. Let me get a human to help." // :437
)

// orchValidShortID mirrors VALID_SHORT_ID (:250) — LLM output is untrusted.
var orchValidShortID = regexp.MustCompile(`^[0-9a-f]{8}$`)

// ---------------------------------------------------------------------------
// deps + job shapes
// ---------------------------------------------------------------------------

// CatalogFunc mirrors CatalogRetrieverService.getContextForBusiness
// (catalog-retriever.service.ts:46). The inbound image, when present,
// travels as a data URL so sibling retrieval code can embed it without
// importing bytes-level media concerns.
type CatalogFunc func(ctx context.Context, businessID, userMessage, currency, latestImageDataURL string) (string, error)

// MediaProcessor bundles the two capabilities processMediaPayload needs
// (T8.15 WhatsAppMediaService.downloadMedia, T8.16 AudioTranscriptionService).
// Optional: nil Media escalates any media turn exactly like the source's
// catch-branch (:427-440).
type MediaProcessor interface {
	Download(ctx context.Context, mediaID string) ([]byte, error)
	Transcribe(ctx context.Context, media []byte) (string, error)
}

// OrchestratorDeps carries everything one debounced turn touches.
type OrchestratorDeps struct {
	Pool      *pgxpool.Pool
	Publisher queue.Publisher
	LLM       orchestrator.LLMClient
	Catalog   CatalogFunc // nil ⇒ empty catalog context (retrieval-failure path)
	Media     MediaProcessor
}

// OrchestratorJob mirrors OrchestratorJobData (orchestrator.processor.ts:10-13);
// businessId/messageId are optional trace fields for producers.
type OrchestratorJob struct {
	BusinessID     string `json:"businessId,omitempty"`
	RecipientPhone string `json:"recipientPhone"`
	SenderPhone    string `json:"senderPhone"`
	MessageID      string `json:"messageId,omitempty"`
}

// OrchestratorDebounceTaskID is the fixed BullMQ jobId debounce key
// (webhook.processor.ts:151). See ORCHESTRATOR_DEBOUNCE.md for the design.
func OrchestratorDebounceTaskID(recipientPhone, senderPhone string) string {
	return "orchestrator:" + recipientPhone + ":" + senderPhone
}

// PublishOrchestratorDebounce is THE producer entrypoint for webhook ingest:
// fixed TaskID dedup collapses a rapid burst into one pending job, ProcessIn
// carries the 3 s delay. A duplicate TaskID is silently dropped by the queue
// client (client.go ErrTaskIDConflict swallow) — drop-newest, exactly like
// BullMQ's duplicate-jobId rejection while the old job is pending/active.
func PublishOrchestratorDebounce(ctx context.Context, pub queue.Publisher, job OrchestratorJob) error {
	return pub.Enqueue(ctx, queue.QOrchestrator, queue.TaskOrchestratorDebounce, job, &queue.EnqueueOpts{
		TaskID:    OrchestratorDebounceTaskID(job.RecipientPhone, job.SenderPhone),
		ProcessIn: OrchestratorDebounceDelay,
	})
}

// RegisterOrchestrator attaches the debounced-conversation handler. The
// wrapper owns processor-level concerns (decode, startedAt capture,
// completion re-check); HandleDebouncedConversation holds the pipeline.
func RegisterOrchestrator(reg queue.Registrar, deps OrchestratorDeps) {
	reg.Register(queue.TaskOrchestratorDebounce, func(ctx context.Context, payload []byte) error {
		var job OrchestratorJob
		if err := json.Unmarshal(payload, &job); err != nil {
			return fmt.Errorf("orchestrator: decode job payload: %w", err)
		}
		// startedAt BEFORE processing (processor.ts:29-32): anything persisted
		// after this instant arrived while we were busy.
		startedAt := time.Now()
		if err := HandleDebouncedConversation(ctx, deps, job); err != nil {
			slog.Error("Orchestrator failed to handle debounced conversation",
				"event", "orchestrator_job_failed",
				"recipientPhone", job.RecipientPhone,
				"senderPhone", job.SenderPhone,
				"err", err)
			return err // terminal — §B.2 QOrchestrator policy MaxRetry:0
		}
		return reenqueueIfNewerInbound(ctx, deps, job, startedAt)
	})
}

// reenqueueIfNewerInbound ports reenqueueIfNewerMessages
// (orchestrator.processor.ts:63-106): close the debounce drop-window by
// scheduling one more pass under a FRESH timestamped task id when messages
// landed during processing. Errors are swallowed non-fatally (:97-105).
func reenqueueIfNewerInbound(ctx context.Context, deps OrchestratorDeps, job OrchestratorJob, startedAt time.Time) error {
	var newer bool
	// Epoch-millis comparison: pgx encodes untyped time.Time params as naive
	// local wall time, which skews against the server's UTC-naive created_at;
	// to_timestamp($3/1000.0) is timezone-immune.
	err := deps.Pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM inbound_messages
		 WHERE sender_phone = $1 AND recipient_phone = $2 AND created_at > to_timestamp($3 / 1000.0))`,
		job.SenderPhone, job.RecipientPhone, startedAt.UnixMilli()).Scan(&newer)
	if err != nil {
		slog.Warn("Failed to re-check for messages arriving during processing (non-blocking)",
			"event", "orchestrator_reenqueue_check_failed",
			"recipientPhone", job.RecipientPhone,
			"senderPhone", job.SenderPhone,
			"err", err)
		return nil
	}
	if !newer {
		return nil
	}
	err = deps.Publisher.Enqueue(ctx, queue.QOrchestrator, queue.TaskOrchestratorDebounce, job, &queue.EnqueueOpts{
		// Fresh jobId required (:59-61) — the fixed key still points at this
		// active run, so reusing it would recreate the very drop we close.
		TaskID:    fmt.Sprintf("%s:%d", OrchestratorDebounceTaskID(job.RecipientPhone, job.SenderPhone), time.Now().UnixMilli()),
		ProcessIn: OrchestratorDebounceDelay,
	})
	if err != nil {
		slog.Warn("Failed to re-enqueue orchestrator after newer inbound (non-blocking)", "err", err)
		return nil
	}
	slog.Info("Re-enqueued orchestrator for messages that arrived during processing",
		"event", "orchestrator_reenqueue",
		"recipientPhone", job.RecipientPhone,
		"senderPhone", job.SenderPhone)
	return nil
}

// ---------------------------------------------------------------------------
// pipeline core
// ---------------------------------------------------------------------------

type orchContext struct {
	businessID     string
	currency       string
	customerID     string
	customerPhone  string
	conversationID string
	state          domain.ConversationState
	escalated      bool
}

// HandleDebouncedConversation runs one debounced turn through every phase of
// conversation-orchestrator.service.ts:293-324 in source order.
func HandleDebouncedConversation(ctx context.Context, deps OrchestratorDeps, job OrchestratorJob) error {
	oc, ok, err := orchResolveConversationContext(ctx, deps.Pool, job.RecipientPhone, job.SenderPhone)
	if err != nil || !ok {
		return err // unknown recipientPhone → warn+ignore (:331-334)
	}

	latest, err := orchLatestMessage(ctx, deps.Pool, oc.conversationID)
	if err != nil {
		return err
	}

	textContent, imageURL, escalated, err := orchProcessMediaPayload(ctx, deps, oc.conversationID, latest)
	if err != nil || escalated {
		return err
	}

	proceed, err := orchEnforceSafetyGuardrails(ctx, deps, oc, textContent)
	if err != nil || !proceed {
		return err
	}

	history, err := orchFetchConversationHistory(ctx, deps.Pool, oc.conversationID)
	if err != nil {
		return err
	}

	return orchGenerateAndHandleLlmResponse(ctx, deps, oc, latest, textContent, imageURL, history)
}

// orchResolveConversationContext ports resolveConversationContext
// (:326-384): find business by whatsapp phone id; upsert customer with
// nested profile create on first contact and lastContactAt bump; upsert
// conversation keyed (businessId, customerPhone); backfill orphan inbounds.
func orchResolveConversationContext(ctx context.Context, pool *pgxpool.Pool, recipientPhone, senderPhone string) (orchContext, bool, error) {
	var oc orchContext
	var currency *string
	err := pool.QueryRow(ctx,
		`SELECT id, currency FROM businesses WHERE whatsapp_phone_number_id = $1`, recipientPhone).
		Scan(&oc.businessID, &currency)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		slog.Warn("No business found for recipientPhone. Ignoring.", "recipientPhone", recipientPhone)
		return oc, false, nil
	case err != nil:
		return oc, false, fmt.Errorf("orchestrator: lookup business %s: %w", recipientPhone, err)
	}
	oc.currency = orchDeref(currency, "GHS")
	oc.customerPhone = senderPhone

	// Stealth CRM customer capture (:338-356): create-with-profile on first
	// contact, bump lastContactAt on repeats. ON CONFLICT DO UPDATE is the
	// upsert-race shield the source cites (:340-342). CURRENT_TIMESTAMP keeps
	// the three stamps on the DB clock (same statement ⇒ same instant).
	oc.customerID = uuid.NewString()
	err = pool.QueryRow(ctx,
		`INSERT INTO customers
		   (id, business_id, phone, acquisition_channel, first_contact_at, last_contact_at, updated_at)
		 VALUES ($1, $2, $3, 'organic', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		 ON CONFLICT (business_id, phone)
		 DO UPDATE SET last_contact_at = EXCLUDED.last_contact_at, updated_at = CURRENT_TIMESTAMP
		 RETURNING id`,
		oc.customerID, oc.businessID, senderPhone).Scan(&oc.customerID)
	if err != nil {
		return oc, false, fmt.Errorf("orchestrator: upsert customer: %w", err)
	}
	// Nested profile create only runs on first contact (:353).
	if _, err := pool.Exec(ctx,
		`INSERT INTO customer_profiles (id, customer_id, updated_at)
		 VALUES ($1, $2, CURRENT_TIMESTAMP)
		 ON CONFLICT (customer_id) DO NOTHING`,
		uuid.NewString(), oc.customerID); err != nil {
		return oc, false, fmt.Errorf("orchestrator: ensure customer profile: %w", err)
	}

	// Conversation upsert keyed [businessId,customerPhone] (:359-374).
	err = pool.QueryRow(ctx,
		`INSERT INTO conversations
		   (id, business_id, customer_id, customer_phone, state, is_escalated_to_human, updated_at)
		 VALUES ($1, $2, $3, $4, 'LEAD', false, CURRENT_TIMESTAMP)
		 ON CONFLICT (business_id, customer_phone)
		 DO UPDATE SET customer_id = EXCLUDED.customer_id, updated_at = CURRENT_TIMESTAMP
		 RETURNING id, state, is_escalated_to_human`,
		uuid.NewString(), oc.businessID, oc.customerID, senderPhone).
		Scan(&oc.conversationID, &oc.state, &oc.escalated)
	if err != nil {
		return oc, false, fmt.Errorf("orchestrator: upsert conversation: %w", err)
	}

	// Backfill messages captured before the business was resolved (:376-381).
	if _, err := pool.Exec(ctx,
		`UPDATE inbound_messages SET business_id = $3, conversation_id = $4
		 WHERE recipient_phone = $1 AND sender_phone = $2 AND business_id IS NULL`,
		recipientPhone, senderPhone, oc.businessID, oc.conversationID); err != nil {
		return oc, false, fmt.Errorf("orchestrator: backfill inbound links: %w", err)
	}
	return oc, true, nil
}

type orchLatest struct {
	whatsappID  string
	messageType string // raw_payload.type ("text"/"audio"/"image"/…)
	textContent string
	rawPayload  map[string]any
}

func orchLatestMessage(ctx context.Context, pool *pgxpool.Pool, conversationID string) (*orchLatest, error) {
	var (
		m    orchLatest
		raw  []byte
		kind *string
	)
	err := pool.QueryRow(ctx,
		`SELECT COALESCE(whatsapp_message_id,''), COALESCE(text_content,''),
		        raw_payload::text, raw_payload->>'type'
		 FROM inbound_messages WHERE conversation_id = $1
		 ORDER BY "timestamp" DESC LIMIT 1`, conversationID).
		Scan(&m.whatsappID, &m.textContent, &raw, &kind)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil // nothing yet → empty-text turn, like the source
	case err != nil:
		return nil, fmt.Errorf("orchestrator: latest inbound for %s: %w", conversationID, err)
	}
	if kind != nil {
		m.messageType = *kind
		m.rawPayload = map[string]any{}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &m.rawPayload)
		}
	}
	return &m, nil
}

// orchProcessMediaPayload ports processMediaPayload (:386-444). Media
// collaborators are optional; without them a media turn follows the source's
// MEDIA_PROCESSING_FAILED escalation branch verbatim.
func orchProcessMediaPayload(ctx context.Context, deps OrchestratorDeps, conversationID string, latest *orchLatest) (textContent, imageURL string, escalated bool, err error) {
	if latest == nil {
		return "", "", false, nil
	}
	textContent = latest.textContent
	mediaType := latest.messageType
	if mediaType != "audio" && mediaType != "image" {
		return textContent, "", false, nil
	}

	mediaID := ""
	if body, ok := latest.rawPayload[mediaType].(map[string]any); ok {
		mediaID, _ = body["id"].(string)
	}

	escalateBranch := func(reason, notice string) (string, string, bool, error) {
		if err := orchEscalate(ctx, deps, conversationID, reason, notice); err != nil {
			return "", "", false, err
		}
		return "", "", true, nil
	}

	if deps.Media == nil || mediaID == "" { // :400-403 mediaId guard / no service
		slog.Error("Failed to process media", "conversationId", conversationID)
		return escalateBranch(orchMediaFailReason, orchMediaFailMsg)
	}

	buffer, dlErr := deps.Media.Download(ctx, mediaID)
	if dlErr == nil && mediaType == "audio" && len(buffer) > orchMaxAudioBytes { // :408-421
		slog.Info("Audio voice note too long. Escalating...", "conversationId", conversationID)
		return escalateBranch(orchAudioLongReason, orchAudioLongMsg)
	}
	if dlErr == nil && mediaType == "audio" { // :423
		textContent, dlErr = deps.Media.Transcribe(ctx, buffer)
	} else if dlErr == nil && mediaType == "image" { // :424-426
		imageURL = "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(buffer)
	}
	if dlErr != nil { // :427-440
		slog.Error("Failed to process media", "conversationId", conversationID, "err", dlErr)
		return escalateBranch(orchMediaFailReason, orchMediaFailMsg)
	}
	return textContent, imageURL, false, nil
}

// orchEnforceSafetyGuardrails ports enforceSafetyGuardrails (:446-475):
// an already-escalated conversation pauses automation entirely; a keyword hit
// sets isEscalatedToHuman=true + escalationReason=KEYWORD_MATCH and enqueues
// the connecting message — the STATE column is untouched (flag-only, exactly
// like low-confidence), then automation stops before history/LLM.
func orchEnforceSafetyGuardrails(ctx context.Context, deps OrchestratorDeps, oc orchContext, textContent string) (bool, error) {
	if oc.escalated {
		slog.Info("Conversation is paused for human agent. Skipping automation.",
			"conversationId", oc.conversationID)
		return false, nil
	}
	if domain.RequiresEscalation(textContent) {
		slog.Info("Escalation triggered by keyword", "conversationId", oc.conversationID)
		if err := orchSetEscalated(ctx, deps.Pool, oc.conversationID, orchKeywordReason); err != nil {
			return false, err
		}
		// :464-469 — connecting message goes through enqueueOutboundMessage,
		// i.e. subject to the 24h window like every free-form reply.
		if err := orchEnqueueOutboundText(ctx, deps, oc.businessID, oc.conversationID, orchConnectingMsg); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// orchFetchConversationHistory ports fetchConversationHistory (:477-509):
// last 10 inbound (by timestamp desc) + last 10 outbound (by createdAt desc),
// merged sorted ascending, sliced to the last 10, THEN empty-filtered (an
// empty message inside the newest ten still burns its budget slot), mapped to
// single-text-part chat messages.
func orchFetchConversationHistory(ctx context.Context, pool *pgxpool.Pool, conversationID string) ([]orchestrator.ChatMessage, error) {
	type item struct {
		role    string
		content string
		at      int64
	}

	inRows, err := pool.Query(ctx,
		`SELECT COALESCE(text_content,''), "timestamp" FROM inbound_messages
		 WHERE conversation_id = $1 ORDER BY "timestamp" DESC LIMIT $2`,
		conversationID, orchHistoryPerSide)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: inbound history: %w", err)
	}
	var history []item
	for inRows.Next() {
		var content string
		var ts time.Time
		if err := inRows.Scan(&content, &ts); err != nil {
			inRows.Close()
			return nil, fmt.Errorf("orchestrator: scan inbound history: %w", err)
		}
		history = append(history, item{role: "user", content: content, at: ts.UnixMilli()})
	}
	inRows.Close()
	if err := inRows.Err(); err != nil {
		return nil, fmt.Errorf("orchestrator: inbound history rows: %w", err)
	}

	outRows, err := pool.Query(ctx,
		`SELECT COALESCE(text_content,''), created_at FROM outbound_messages
		 WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT $2`,
		conversationID, orchHistoryPerSide)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: outbound history: %w", err)
	}
	for outRows.Next() {
		var content string
		var ts time.Time
		if err := outRows.Scan(&content, &ts); err != nil {
			outRows.Close()
			return nil, fmt.Errorf("orchestrator: scan outbound history: %w", err)
		}
		history = append(history, item{role: "assistant", content: content, at: ts.UnixMilli()})
	}
	outRows.Close()
	if err := outRows.Err(); err != nil {
		return nil, fmt.Errorf("orchestrator: outbound history rows: %w", err)
	}

	sort.Slice(history, func(i, j int) bool { return history[i].at < history[j].at })
	if len(history) > orchHistoryPerSide {
		history = history[len(history)-orchHistoryPerSide:] // :505-506 slice(-10)
	}

	messages := make([]orchestrator.ChatMessage, 0, len(history))
	for _, h := range history {
		if strings.TrimSpace(h.content) == "" { // :507 filter AFTER slice
			continue
		}
		messages = append(messages, orchestrator.ChatMessage{
			Role:  h.role,
			Parts: []orchestrator.MessagePart{{Type: "text", Text: h.content}},
		})
	}
	return messages, nil
}

// orchGenerateAndHandleLlmResponse ports generateAndHandleLlmResponse
// (:511-644): catalog context, already-shown context, LLM call, then the
// response-handling phases in source order.
func orchGenerateAndHandleLlmResponse(ctx context.Context, deps OrchestratorDeps, oc orchContext, latest *orchLatest, textContent, imageURL string, history []orchestrator.ChatMessage) error {
	catalogContext := ""
	if deps.Catalog != nil {
		cc, err := deps.Catalog(ctx, oc.businessID, textContent, oc.currency, imageURL)
		if err != nil {
			slog.Error("Failed to retrieve context", "err", err)
			catalogContext = "Catalog retrieval failed.\n" // retriever catch-branch
		} else {
			catalogContext = cc
		}
	}

	alreadyShown, err := orchAlreadyShownProducts(ctx, deps.Pool, oc.conversationID)
	if err != nil {
		return err
	}
	shownImagesCtx := orchBuildShownImagesContext(alreadyShown)

	paymentMethods := orchCurrencyFor(oc.currency).PaymentMethods
	req := orchestrator.GenerateRequest{
		System: strings.Join([]string{
			"Current Conversation State: " + string(oc.state),
			"Accepted payment methods: " + paymentMethods,
			catalogContext + shownImagesCtx,
		}, "\n\n"),
		Messages: history,
	}
	if imageURL != "" {
		attachImageToLastUserTurn(req.Messages, imageURL) // T8.14 multimodal assembly
	}

	resp, err := deps.LLM.GenerateObject(ctx, req)
	if err != nil {
		return fmt.Errorf("orchestrator: llm generate: %w", err)
	}
	slog.Info("LLM decision",
		"intent", resp.Intent,
		"internal_confidence", resp.InternalConfidence,
		"send_product_image_ids", strings.Join(resp.SendProductImageIDs, ","))

	// Low confidence → FLAG-ONLY escalation (:539-552): flag + reason +
	// connecting message + early return. No transition, no images, no reply,
	// no CRM emission.
	if resp.InternalConfidence < orchConfidenceFloor {
		slog.Info("Low confidence. Escalating...", "confidence", resp.InternalConfidence)
		if err := orchSetEscalated(ctx, deps.Pool, oc.conversationID, orchLowConfReason); err != nil {
			return err
		}
		return orchEnqueueOutboundText(ctx, deps, oc.businessID, oc.conversationID, orchConnectingMsg)
	}

	// Images go out BEFORE the text reply (:554-575), under the deterministic
	// explicit-request-only gate (:217-240).
	imageIDsToSend := orchGateProductImageIDs(resp.SendProductImageIDs, resp.CustomerRequestedImages, alreadyShown, oc.conversationID)
	for _, shortID := range imageIDsToSend {
		if err := orchSendProductImage(ctx, deps, oc, shortID); err != nil {
			return err
		}
	}

	if err := orchEnqueueOutboundText(ctx, deps, oc.businessID, oc.conversationID, resp.ReplyText); err != nil {
		return err
	}

	current := oc.state
	newState := domain.ConversationState("")
	hasNew := false
	sig := resp.CrmSignals
	switch {
	case sig.OrderConfirmed && len(sig.DetectedItems) > 0:
		// Order confirmed → INVOICING (from any active state) (:589-596).
		if current != domain.StateInvoicing && current != domain.StateEscalated {
			newState, hasNew = domain.StateInvoicing, true
		}
	case resp.Intent == "checkout_request": // (:597-601)
		if current == domain.StateLead || current == domain.StateBrowsing {
			newState, hasNew = domain.StateCheckout, true
		}
	case resp.Intent == "product_inquiry" || resp.Intent == "image_match": // (:602-605)
		if current == domain.StateLead {
			newState, hasNew = domain.StateBrowsing, true
		}
	case resp.Intent == "support_faq" || resp.Intent == "complaint": // (:606-610)
		if current == domain.StateLead || current == domain.StateBrowsing {
			newState, hasNew = domain.StateSupport, true
		}
	}
	if hasNew && newState != current { // (:613-624) WITH SOURCE GUARDS
		ok, err := domain.Transition(ctx, deps.Pool, oc.conversationID, current, newState)
		if err != nil {
			return err
		}
		if !ok {
			slog.Warn("State transition rejected",
				"from", current, "to", newState, "conversationId", oc.conversationID)
		}
	}

	// Stealth CRM emission (:626-643) — MUST NOT block the reply; failures
	// logged and swallowed.
	crmJob := orchCRMSignalJob(oc, latest, resp)
	if err := deps.Publisher.Enqueue(ctx, queue.QCRMMaterialiser, queue.TaskCRMProcess, crmJob, nil); err != nil {
		slog.Warn("Failed to emit CRM signals (non-blocking)", "err", err)
	}
	return nil
}

// attachImageToLastUserTurn ports llm-client.service.ts:96-108: the inbound
// photo rides on the LAST user message as a second content part.
func attachImageToLastUserTurn(messages []orchestrator.ChatMessage, dataURL string) {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "user" {
			continue
		}
		text := ""
		for _, p := range messages[i].Parts {
			if p.Type == "text" {
				text = p.Text
				break
			}
		}
		messages[i].Parts = []orchestrator.MessagePart{
			{Type: "text", Text: text},
			{Type: "image", ImageURL: dataURL},
		}
		return
	}
}
