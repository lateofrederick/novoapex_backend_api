// Package orchestrator ports libs/orchestrator (Stage 8). Shared types live
// here so the catalog, LLM, and pipeline streams compile independently.
package orchestrator

import "context"

// CrmSignals mirrors CrmSignalsSchema (libs/orchestrator/src/llm/structured-output.ts).
type CrmSignals struct {
	OrderConfirmed      bool           `json:"order_confirmed"`
	DetectedItems       []DetectedItem `json:"detected_items"`
	DeliveryArea        *string        `json:"delivery_area"`
	FulfillmentChoice   *string        `json:"fulfillment_choice"` // "delivery" | "pickup" | nil
	PickupLocationID    *string        `json:"pickup_location_id"`
	CustomerName        *string        `json:"customer_name"`
	DetectedPreferences []string       `json:"detected_preferences"`
	WantsUpdates        *bool          `json:"wants_updates"`
	Sentiment           string         `json:"sentiment"`
}

type DetectedItem struct {
	ProductID string  `json:"product_id"`
	Quantity  float64 `json:"quantity"`
}

// LlmResponse mirrors LlmResponseSchema — every field required.
type LlmResponse struct {
	ReplyText               string     `json:"reply_text"`
	InternalConfidence      float64    `json:"internal_confidence"`
	Intent                  string     `json:"intent"` // product_inquiry|greeting|support_faq|checkout_request|complaint|image_match|unknown
	CustomerRequestedImages bool       `json:"customer_requested_images"`
	SendProductImageIDs     []string   `json:"send_product_image_ids"`
	ReferencedProductIDs    []string   `json:"referenced_product_ids"`
	CrmSignals              CrmSignals `json:"crm_signals"`
}

// MessagePart is one content part of a chat message (text or image).
type MessagePart struct {
	Type     string `json:"type"` // "text" | "image"
	Text     string `json:"-"`
	ImageURL string `json:"-"` // data URL or https URL
}

// ChatMessage is one conversation-turn message for the LLM call.
type ChatMessage struct {
	Role  string        // "user" | "assistant"
	Parts []MessagePart // >=1 part; text-only messages have exactly one text part
}

// GenerateRequest is everything the structured-output call needs.
type GenerateRequest struct {
	System   string
	Messages []ChatMessage
}

// LLMClient produces schema-constrained structured output. Implemented over
// OpenAI Responses API (T8.13); fakes satisfy it in tests.
type LLMClient interface {
	GenerateObject(ctx context.Context, req GenerateRequest) (LlmResponse, error)
}

// TokenUsage reports prompt/completion token counts for cost tracking (T8.30).
type TokenUsage struct {
	PromptTokens     int
	CompletionTokens int
}
