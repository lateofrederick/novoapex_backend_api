// Structured-output engine over the OpenAI Responses API (T8.13): schema
// injection, native responses-mode call, output_text parsing and zod-mirroring
// validation with one corrective retry on malformed output.
package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
)

// DefaultLLMModel mirrors openai('gpt-4o-mini') in llm-client.service.ts.
const DefaultLLMModel = "gpt-4o-mini"

// schemaName is the json_schema name sent in text.format — the AI SDK default
// for generateObject without an explicit schemaName.
const schemaName = "response"

// correctiveRetryInstruction is appended to the system prompt when the first
// attempt comes back malformed. The source service does not retry at all (it
// logs and rethrows); the retry-once design is specified for the port.
const correctiveRetryInstruction = "Your previous response could not be used because it did not satisfy the required JSON schema (%s). Respond again with ONLY a single JSON object that satisfies the schema — no prose, no markdown fences, no trailing commas."

// LLMConfig configures a ResponsesLLM.
type LLMConfig struct {
	APIKey     string
	BaseURL    string // optional override; default openai.DefaultBaseURL
	Model      string // optional; default DefaultLLMModel
	HTTPClient *http.Client
}

// ResponsesLLM implements orchestrator.LLMClient over POST {base}/responses
// with text.format json_schema (strict).
type ResponsesLLM struct {
	client *openai.Client
	model  string

	mu       sync.Mutex
	lastUsed TokenUsage // usage accumulated by the most recent GenerateObject call
}

// NewResponsesLLM builds an engine on top of the shared openai transport.
func NewResponsesLLM(cfg LLMConfig) *ResponsesLLM {
	model := cfg.Model
	if model == "" {
		model = DefaultLLMModel
	}
	return &ResponsesLLM{
		client: openai.New(openai.Config{
			APIKey:  cfg.APIKey,
			BaseURL: cfg.BaseURL,
			Client:  cfg.HTTPClient,
		}),
		model: model,
	}
}

// LastUsage returns the prompt/completion tokens consumed by the most recent
// GenerateObject call (summed across retry attempts) — T8.30 groundwork.
func (l *ResponsesLLM) LastUsage() TokenUsage {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastUsed
}

// MalformedResponseError is returned when every attempt produced output that
// failed to parse or validate against LlmResponseSchema.
type MalformedResponseError struct {
	Attempts int      // number of attempts made (2)
	Issues   []string // validation findings from the final attempt
	Body     string   // raw model text of the final attempt
}

func (e *MalformedResponseError) Error() string {
	return fmt.Sprintf("orchestrator: malformed LLM response after %d attempt(s): %v (last body: %.256q)",
		e.Attempts, e.Issues, e.Body)
}

// responses wire shapes (request side only; reply decoding lives in openai).

type llmContentPart struct {
	Type     string `json:"type"` // input_text | input_image
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
}

type llmInputMessage struct {
	Role    string           `json:"role"` // system | user | assistant
	Content []llmContentPart `json:"content"`
}

type llmFormat struct {
	Type   string          `json:"type"` // json_schema
	Name   string          `json:"name"`
	Strict bool            `json:"strict"`
	Schema json.RawMessage `json:"schema"`
}

type llmTextOptions struct {
	Format llmFormat `json:"format"`
}

type llmResponsesRequest struct {
	Model string            `json:"model"`
	Input []llmInputMessage `json:"input"`
	Text  llmTextOptions    `json:"text"`
}

// buildInput converts GenerateRequest into /responses input messages.
// Text parts become input_text, image parts become input_image carrying their
// data/https URL verbatim; part order is preserved so a customer photo stays
// attached to its turn (T8.14).
func buildInput(req GenerateRequest) []llmInputMessage {
	input := make([]llmInputMessage, 0, len(req.Messages)+1)
	if req.System != "" {
		input = append(input, llmInputMessage{
			Role: "system",
			Content: []llmContentPart{
				{Type: "input_text", Text: req.System},
			},
		})
	}
	for _, msg := range req.Messages {
		parts := make([]llmContentPart, 0, len(msg.Parts))
		for _, p := range msg.Parts {
			if p.Type == "image" && p.ImageURL != "" {
				parts = append(parts, llmContentPart{Type: "input_image", ImageURL: p.ImageURL})
				continue
			}
			parts = append(parts, llmContentPart{Type: "input_text", Text: p.Text})
		}
		if len(parts) == 0 {
			parts = append(parts, llmContentPart{Type: "input_text"})
		}
		input = append(input, llmInputMessage{Role: msg.Role, Content: parts})
	}
	return input
}

func (l *ResponsesLLM) requestBody(system string, req GenerateRequest) ([]byte, error) {
	messages := buildInput(req)
	if system != "" {
		if len(messages) > 0 && messages[0].Role == "system" {
			messages[0].Content[0].Text = system
		} else {
			messages = append([]llmInputMessage{{
				Role:    "system",
				Content: []llmContentPart{{Type: "input_text", Text: system}},
			}}, messages...)
		}
	}
	body, err := json.Marshal(llmResponsesRequest{
		Model: l.model,
		Input: messages,
		Text: llmTextOptions{
			Format: llmFormat{
				Type:   "json_schema",
				Name:   schemaName,
				Strict: true,
				Schema: json.RawMessage(LLMSchemaJSON()),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("orchestrator: encode /responses request: %w", err)
	}
	return body, nil
}

// GenerateObject calls /responses, parses output_text into LlmResponse and
// validates it against hand-written checks mirroring the zod constraints.
// On malformed output (JSON parse failure OR validation failure) it retries
// once with a corrective instruction appended to the system prompt, then fails
// with *MalformedResponseError. Transport errors fail fast, like the source's
// log-and-rethrow.
func (l *ResponsesLLM) GenerateObject(ctx context.Context, req GenerateRequest) (LlmResponse, error) {
	system := req.System
	var (
		usage    TokenUsage
		attempts int
	)
	for {
		attempts++
		body, err := l.requestBody(system, req)
		if err != nil {
			return LlmResponse{}, err
		}
		reply, err := l.client.ChatResponses(ctx, body)
		if err != nil {
			slog.ErrorContext(ctx, fmt.Sprintf("Failed to generate LLM response: %v", err))
			return LlmResponse{}, err
		}
		usage.PromptTokens += reply.Usage.InputTokens
		usage.CompletionTokens += reply.Usage.OutputTokens
		l.setLastUsage(usage)

		text := reply.OutputText()
		parsed, issues := validateLlmOutput([]byte(text))
		if len(issues) == 0 {
			return *parsed, nil
		}
		if attempts >= 2 {
			slog.ErrorContext(ctx, fmt.Sprintf("Failed to generate LLM response: malformed after %d attempts: %v", attempts, issues))
			return LlmResponse{}, &MalformedResponseError{Attempts: attempts, Issues: issues, Body: text}
		}
		// Retry once with the corrective instruction appended to the system
		// prompt (brief-specified semantics; source logs+rethrows instead).
		system = req.System + "\n\n" + fmt.Sprintf(correctiveRetryInstruction, strings.Join(issues, "; "))
	}
}

func (l *ResponsesLLM) setLastUsage(u TokenUsage) {
	l.mu.Lock()
	l.lastUsed = u
	l.mu.Unlock()
}

// validateLlmOutput parses model text and returns (response, nil) when it
// satisfies every constraint zod enforces on LlmResponseSchema:
//   - all top-level keys present (all fields are required in the schema)
//   - all crm_signals keys present (nullable ones may be null)
//   - intent/sentiment within their enums
//   - internal_confidence within [0,1]
//   - send_product_image_ids at most maxItems=5
//
// Unknown properties are ignored (zod strips them rather than erroring).
func validateLlmOutput(raw []byte) (*LlmResponse, []string) {
	var resp LlmResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, []string{"output is not valid JSON matching the schema types: " + err.Error()}
	}

	// Presence checks use raw maps: every schema field is required, but
	// nullable ones may carry an explicit null, which pointer decoding would
	// conflate with absence.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, []string{"output is not valid JSON matching the schema shape: " + err.Error()}
	}
	var issues []string
	check := func(ok bool, format string, args ...any) {
		if !ok {
			issues = append(issues, fmt.Sprintf(format, args...))
		}
	}
	for _, key := range []string{
		"reply_text", "internal_confidence", "intent",
		"customer_requested_images", "send_product_image_ids",
		"referenced_product_ids", "crm_signals",
	} {
		v, ok := top[key]
		check(ok, "missing required field %s", key)
		check(ok && string(v) != "null", "%s must not be null", key)
	}

	crmRaw, hasCrm := top["crm_signals"]
	var crm map[string]json.RawMessage
	if hasCrm && string(crmRaw) != "null" {
		if err := json.Unmarshal(crmRaw, &crm); err != nil {
			check(false, "crm_signals is not a JSON object")
			crm = nil
		}
	} else {
		check(false, "missing required field crm_signals")
	}
	if crm != nil {
		for _, key := range []string{
			"order_confirmed", "detected_items", "delivery_area",
			"customer_name", "detected_preferences", "sentiment",
		} {
			_, ok := crm[key]
			check(ok, "missing required field crm_signals.%s", key)
		}
	}

	switch resp.Intent {
	case "product_inquiry", "greeting", "support_faq", "checkout_request", "complaint", "image_match", "unknown":
	default:
		check(false, "intent %q is not one of the allowed enum values", resp.Intent)
	}
	check(resp.InternalConfidence >= 0 && resp.InternalConfidence <= 1,
		"internal_confidence %v is outside the range [0,1]", resp.InternalConfidence)
	check(len(resp.SendProductImageIDs) <= 5,
		"send_product_image_ids has %d items, maximum is 5", len(resp.SendProductImageIDs))

	if crm != nil {
		switch resp.CrmSignals.Sentiment {
		case "positive", "neutral", "negative":
		default:
			check(false, "crm_signals.sentiment %q is not one of the allowed enum values", resp.CrmSignals.Sentiment)
		}
	}

	if len(issues) > 0 {
		return nil, issues
	}
	return &resp, nil
}

// Compile-time interface check against the shared client contract.
var _ LLMClient = (*ResponsesLLM)(nil)
