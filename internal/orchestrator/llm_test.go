package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
)

// responsesStub serves scripted /responses replies while recording every
// request body for wire-format assertions.
type responsesStub struct {
	mu      sync.Mutex
	bodies  []map[string]any
	replies []string // output_text payload per call
	status  int      // optional fixed non-2xx status
}

func (s *responsesStub) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	s.bodies = append(s.bodies, body)

	if s.status != 0 {
		w.WriteHeader(s.status)
		_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
		return
	}
	i := len(s.bodies) - 1
	if i >= len(s.replies) {
		i = len(s.replies) - 1
	}
	text := s.replies[i]
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":     "resp_test",
		"model":  "gpt-4o-mini",
		"status": "completed",
		"output": []any{
			map[string]any{
				"type": "message",
				"role": "assistant",
				"content": []any{
					map[string]any{"type": "output_text", "text": text},
				},
			},
		},
		"usage": map[string]any{"input_tokens": 11, "output_tokens": 7, "total_tokens": 18},
	})
}

func (s *responsesStub) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bodies)
}

// validLlmJSON is a fully-populated LlmResponse payload as the model would emit.
const validLlmJSON = `{
  "reply_text": "We have Shea Butter in stock!",
  "internal_confidence": 0.93,
  "intent": "product_inquiry",
  "customer_requested_images": false,
  "send_product_image_ids": [],
  "referenced_product_ids": ["ab12cd34"],
  "crm_signals": {
    "order_confirmed": false,
    "detected_items": [{"product_id": "ab12cd34", "quantity": 2}],
    "delivery_area": null,
    "customer_name": "Ama",
    "detected_preferences": ["skincare"],
    "sentiment": "positive"
  }
}`

func newTestLLM(t *testing.T, stub *responsesStub) *orchestrator.ResponsesLLM {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	t.Cleanup(srv.Close)
	return orchestrator.NewResponsesLLM(orchestrator.LLMConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
	})
}

func wantGenerateRequest() orchestrator.GenerateRequest {
	return orchestrator.GenerateRequest{
		System: "SYSTEM PROMPT TEXT",
		Messages: []orchestrator.ChatMessage{
			{Role: "user", Parts: []orchestrator.MessagePart{{Type: "text", Text: "hi"}}},
			{Role: "assistant", Parts: []orchestrator.MessagePart{{Type: "text", Text: "hello"}}},
			{Role: "user", Parts: []orchestrator.MessagePart{{Type: "text", Text: "do you have this?"}}},
		},
	}
}

func TestGenerateObjectHappyPath(t *testing.T) {
	stub := &responsesStub{replies: []string{validLlmJSON}}
	llm := newTestLLM(t, stub)

	resp, err := llm.GenerateObject(context.Background(), wantGenerateRequest())
	if err != nil {
		t.Fatalf("GenerateObject: %v", err)
	}
	if resp.ReplyText != "We have Shea Butter in stock!" || resp.Intent != "product_inquiry" {
		t.Errorf("parsed response wrong: %+v", resp)
	}
	if resp.InternalConfidence != 0.93 || resp.CustomerRequestedImages {
		t.Errorf("scalar fields wrong: %+v", resp)
	}
	if len(resp.ReferencedProductIDs) != 1 || resp.ReferencedProductIDs[0] != "ab12cd34" {
		t.Errorf("referenced ids wrong: %v", resp.ReferencedProductIDs)
	}
	if resp.CrmSignals.OrderConfirmed || resp.CrmSignals.Sentiment != "positive" || resp.CrmSignals.CustomerName == nil || *resp.CrmSignals.CustomerName != "Ama" {
		t.Errorf("crm signals wrong: %+v", resp.CrmSignals)
	}
	if resp.CrmSignals.DeliveryArea != nil {
		t.Errorf("nullable delivery_area should decode to nil, got %v", *resp.CrmSignals.DeliveryArea)
	}
	if got := llm.LastUsage(); got != (orchestrator.TokenUsage{PromptTokens: 11, CompletionTokens: 7}) {
		t.Errorf("LastUsage = %+v, want {11 7}", got)
	}
}

func TestGenerateObjectWireFormat(t *testing.T) {
	stub := &responsesStub{replies: []string{validLlmJSON}}
	llm := newTestLLM(t, stub)

	req := wantGenerateRequest()
	req.Messages[2].Parts = append(req.Messages[2].Parts,
		orchestrator.MessagePart{Type: "image", ImageURL: "data:image/jpeg;base64,/9j/4AAQ"})

	if _, err := llm.GenerateObject(context.Background(), req); err != nil {
		t.Fatalf("GenerateObject: %v", err)
	}
	if stub.calls() != 1 {
		t.Fatalf("expected exactly one /responses call, got %d", stub.calls())
	}
	body := stub.bodies[0]

	if body["model"] != "gpt-4o-mini" {
		t.Errorf("model = %v, want gpt-4o-mini", body["model"])
	}
	text, ok := body["text"].(map[string]any)
	if !ok {
		t.Fatal("request missing text.format block")
	}
	format := text["format"].(map[string]any)
	if format["type"] != "json_schema" || format["name"] != "response" || format["strict"] != true {
		t.Errorf("text.format = %v, want json_schema/response/strict:true", format)
	}
	var gotSchema, wantSchema any
	schemaRaw, _ := json.Marshal(format["schema"])
	_ = json.Unmarshal(schemaRaw, &gotSchema)
	_ = json.Unmarshal(orchestrator.LLMSchemaJSON(), &wantSchema)
	if string(schemaRaw) != string(mustJSON(t, wantSchema)) {
		t.Error("wire schema diverges from LLMSchemaJSON()")
	}

	input, ok := body["input"].([]any)
	if !ok {
		t.Fatal("request missing input array")
	}
	if len(input) != 4 { // system + 3 turns
		t.Fatalf("input length = %d, want 4", len(input))
	}
	system := input[0].(map[string]any)
	if system["role"] != "system" {
		t.Errorf("first input role = %v, want system", system["role"])
	}
	sysContent := system["content"].([]any)[0].(map[string]any)
	if sysContent["type"] != "input_text" || sysContent["text"] != "SYSTEM PROMPT TEXT" {
		t.Errorf("system part wrong: %v", sysContent)
	}

	// Assistant turn must be sent as output_text (Responses API rejects
	// input_text on an assistant message with a 400 invalid_value).
	assistant := input[2].(map[string]any)
	if assistant["role"] != "assistant" {
		t.Fatalf("input[2] role = %v, want assistant", assistant["role"])
	}
	assistantPart := assistant["content"].([]any)[0].(map[string]any)
	if assistantPart["type"] != "output_text" || assistantPart["text"] != "hello" {
		t.Errorf("assistant part wrong: %v", assistantPart)
	}

	// Multimodal snapshot: image rides on the LAST user turn as input_image
	// with the data URL verbatim, after its input_text sibling.
	last := input[3].(map[string]any)
	parts := last["content"].([]any)
	if len(parts) != 2 {
		t.Fatalf("last turn has %d parts, want 2", len(parts))
	}
	p0 := parts[0].(map[string]any)
	p1 := parts[1].(map[string]any)
	if p0["type"] != "input_text" || p0["text"] != "do you have this?" {
		t.Errorf("part 0 wrong: %v", p0)
	}
	if p1["type"] != "input_image" || p1["image_url"] != "data:image/jpeg;base64,/9j/4AAQ" {
		t.Errorf("part 1 wrong: %v", p1)
	}
	if _, present := p0["image_url"]; present {
		t.Error("input_text must not carry an image_url key")
	}
	if _, present := p1["text"]; present {
		t.Error("input_image must not carry a text key")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestGenerateObjectMalformedThenRetrySucceeds(t *testing.T) {
	stub := &responsesStub{replies: []string{"not json at all", validLlmJSON}}
	llm := newTestLLM(t, stub)

	resp, err := llm.GenerateObject(context.Background(), wantGenerateRequest())
	if err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if resp.Intent != "product_inquiry" {
		t.Errorf("retried response wrong: %+v", resp)
	}
	if stub.calls() != 2 {
		t.Fatalf("calls = %d, want 2", stub.calls())
	}
	// Second request carries the corrective instruction appended to the
	// original system prompt.
	second := stub.bodies[1]
	sys := second["input"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)
	sysText := sys["text"].(string)
	if !strings.HasPrefix(sysText, "SYSTEM PROMPT TEXT") {
		t.Error("corrective system prompt lost the original instructions")
	}
	if !strings.Contains(sysText, "did not satisfy the required JSON schema") ||
		!strings.Contains(sysText, "ONLY a single JSON object") {
		t.Errorf("corrective instruction missing from second call:\n%.400s", sysText[len("SYSTEM PROMPT TEXT"):])
	}
	// Usage accumulates across BOTH attempts.
	if got := llm.LastUsage(); got != (orchestrator.TokenUsage{PromptTokens: 22, CompletionTokens: 14}) {
		t.Errorf("LastUsage = %+v, want {22 14} (both attempts)", got)
	}
}

func TestGenerateObjectMalformedTwiceTypedError(t *testing.T) {
	stub := &responsesStub{replies: []string{"{\"reply_text\": oops}", "{\"reply_text\": still oops}"}}
	llm := newTestLLM(t, stub)

	_, err := llm.GenerateObject(context.Background(), wantGenerateRequest())
	var malformed *orchestrator.MalformedResponseError
	if !errors.As(err, &malformed) {
		t.Fatalf("want *MalformedResponseError, got %T: %v", err, err)
	}
	if malformed.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", malformed.Attempts)
	}
	if len(malformed.Issues) == 0 || !strings.Contains(malformed.Issues[0], "not valid JSON") {
		t.Errorf("issues should name the JSON failure, got %v", malformed.Issues)
	}
	if !strings.Contains(malformed.Body, "still oops") {
		t.Errorf("Body should carry the last attempt text, got %q", malformed.Body)
	}
	if stub.calls() != 2 {
		t.Errorf("calls = %d, want 2 (single retry)", stub.calls())
	}
}

func TestGenerateObjectValidationFailureEnumThenSuccess(t *testing.T) {
	invalidIntent := strings.Replace(validLlmJSON, `"product_inquiry"`, `"totally_mad"`, 1)
	stub := &responsesStub{replies: []string{invalidIntent, validLlmJSON}}
	llm := newTestLLM(t, stub)

	resp, err := llm.GenerateObject(context.Background(), wantGenerateRequest())
	if err != nil {
		t.Fatalf("retry after enum violation should succeed: %v", err)
	}
	if resp.Intent != "product_inquiry" {
		t.Errorf("final intent = %q", resp.Intent)
	}
	firstIssuesHint := stub.bodies[1]["input"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(firstIssuesHint, `"totally_mad" is not one of the allowed enum values`) {
		t.Errorf("corrective hint should quote the enum violation:\n%.300s", firstIssuesHint)
	}
}

func TestGenerateObjectValidationConstraints(t *testing.T) {
	cases := map[string]struct {
		mutate func(string) string
		issue  string
	}{
		"confidence too high": {
			mutate: func(s string) string { return strings.Replace(s, "0.93", "1.5", 1) },
			issue:  "outside the range [0,1]",
		},
		"six image ids": {
			mutate: func(s string) string {
				return strings.Replace(s, `"send_product_image_ids": []`,
					`"send_product_image_ids": ["a1","b2","c3","d4","e5","f6"]`, 1)
			},
			issue: "maximum is 5",
		},
		"missing crm_signals": {
			mutate: stripKey("crm_signals"),
			issue:  "missing required field crm_signals",
		},
		"missing reply_text": {
			mutate: stripKey("reply_text"),
			issue:  "missing required field reply_text",
		},
		"bad sentiment": {
			mutate: func(s string) string { return strings.Replace(s, `"positive"`, `"meh"`, 1) },
			issue:  "crm_signals.sentiment",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stub := &responsesStub{replies: []string{tc.mutate(validLlmJSON), validLlmJSON}}
			llm := newTestLLM(t, stub)
			if _, err := llm.GenerateObject(context.Background(), wantGenerateRequest()); err != nil {
				t.Fatalf("retry should succeed: %v", err)
			}
			hint := stub.bodies[1]["input"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
			if !strings.Contains(hint, tc.issue) {
				t.Errorf("corrective hint should contain %q:\n%.500s", tc.issue, hint)
			}
		})
	}
}

// stripKey removes a top-level key from the JSON payload text.
func stripKey(key string) func(string) string {
	return func(s string) string {
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			panic(err)
		}
		delete(m, key)
		out, _ := json.Marshal(m)
		return string(out)
	}
}

func TestGenerateObjectTransportErrorFailsFast(t *testing.T) {
	stub := &responsesStub{status: http.StatusInternalServerError}
	llm := newTestLLM(t, stub)

	_, err := llm.GenerateObject(context.Background(), wantGenerateRequest())
	var apiErr *openai.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("transport errors must surface as *openai.Error immediately, got %T: %v", err, err)
	}
	if apiErr.Status != http.StatusInternalServerError {
		t.Errorf("status = %d", apiErr.Status)
	}
	if stub.calls() != 1 {
		t.Errorf("calls = %d, want 1 (no malformed-retry on transport errors)", stub.calls())
	}
}
