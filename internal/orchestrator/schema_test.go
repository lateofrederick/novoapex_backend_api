package orchestrator_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
)

// Reference .describe() texts extracted verbatim from
// libs/orchestrator/src/llm/structured-output.ts. They are load-bearing prompt
// text: the schema literals must carry them byte-for-byte.
const (
	descOrderConfirmed          = "True ONLY if the user explicitly confirmed/agreed to place an order in THIS message turn (e.g. \"yes\", \"confirm\", \"go ahead\"). False for inquiries, browsing, or initial order requests that still need confirmation."
	descProductID               = "The product ID from the catalog (the short ID in [ID: xxx])"
	descQuantity                = "Quantity requested. Provide 1 if the user did not specify."
	descDetectedItems           = "Products the user wants to order. Only populate when there is a clear purchase intent, not just browsing or asking about a product."
	descDeliveryArea            = "Delivery location/area if the user mentioned it in this turn. Null if not mentioned."
	descCustomerName            = "The customer name if they introduced themselves or it can be clearly inferred from the message. Null otherwise."
	descDetectedPreferences     = "Product categories or attributes the customer shows interest in (e.g. \"organic\", \"large size\", \"spicy\", \"hair products\"). Empty array if none detected in this turn."
	descSentiment               = "Overall customer sentiment in this message turn. Complaints/frustration → negative, thanks/praise → positive, neutral otherwise."
	descCrmRoot                 = "Lightweight CRM signals. Only populate fields that are EXPLICITLY present in the current message turn. Do not guess or infer. When in doubt, leave fields as null/empty — false negatives are better than false positives for CRM data."
	descReplyText               = "The precise text response to send back to the user via WhatsApp."
	descInternalConfidence      = "Confidence score from 0.0 to 1.0 that this response is safe, accurate, and perfectly addresses the user intent without hallucinations."
	descIntent                  = "The extracted intent of the user's message. Use 'image_match' when the customer sent an image that visually matched a product in the catalog."
	descCustomerRequestedImages = "True ONLY if the customer explicitly asked to see a photo/picture/image in THIS message turn — including \"yes\"/\"sure\" in direct reply to your offer to send photos, or sending their own photo asking which product it is. False for every other turn, including when they merely mention or order a product."
	descSendProductImageIDs     = "Product short IDs whose images should be sent. Leave EMPTY unless customer_requested_images is true — product listings are text-first, and images are only sent on request. Only include products marked [HAS_IMAGE] in the catalog, and never a product listed under IMAGES ALREADY SENT unless the customer asked to see it again. Max 5."
	descReferencedProductIDs    = "The IDs of products referenced in the reply (from [ID: xxx] in the catalog). Empty array if no products were referenced."
)

func decodeSchema(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	if !json.Valid(raw) {
		t.Fatal("schema bytes are not valid JSON")
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	return doc
}

func schemaProps(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	p, ok := doc["properties"].(map[string]any)
	if !ok {
		t.Fatal("schema has no properties object")
	}
	return p
}

func descriptionOf(node map[string]any) string {
	d, _ := node["description"].(string)
	return d
}

func TestCRMSchemaDescriptionsByteEqual(t *testing.T) {
	doc := decodeSchema(t, orchestrator.CRMSchemaJSON())
	p := schemaProps(t, doc)

	for name, want := range map[string]string{
		"order_confirmed":      descOrderConfirmed,
		"detected_items":       descDetectedItems,
		"delivery_area":        descDeliveryArea,
		"customer_name":        descCustomerName,
		"detected_preferences": descDetectedPreferences,
		"sentiment":            descSentiment,
	} {
		node, ok := p[name].(map[string]any)
		if !ok {
			t.Fatalf("crm property %q missing", name)
		}
		if got := descriptionOf(node); got != want {
			t.Errorf("crm_signals.%s description mismatch:\n got: %q\nwant: %q", name, got, want)
		}
	}
	if got := descriptionOf(doc); got != descCrmRoot {
		t.Errorf("crm root description mismatch:\n got: %q\nwant: %q", got, descCrmRoot)
	}

	itemNode, ok := p["detected_items"].(map[string]any)
	if !ok {
		t.Fatal("detected_items missing")
	}
	itemProps := schemaProps(t, itemNode["items"].(map[string]any))
	for name, want := range map[string]string{
		"product_id": descProductID,
		"quantity":   descQuantity,
	} {
		if got := descriptionOf(itemProps[name].(map[string]any)); got != want {
			t.Errorf("detected_items[].%s description mismatch:\n got: %q\nwant: %q", name, got, want)
		}
	}
}

func TestLLMSchemaDescriptionsByteEqual(t *testing.T) {
	doc := decodeSchema(t, orchestrator.LLMSchemaJSON())
	p := schemaProps(t, doc)

	for name, want := range map[string]string{
		"reply_text":                descReplyText,
		"internal_confidence":       descInternalConfidence,
		"intent":                    descIntent,
		"customer_requested_images": descCustomerRequestedImages,
		"send_product_image_ids":    descSendProductImageIDs,
		"referenced_product_ids":    descReferencedProductIDs,
	} {
		node, ok := p[name].(map[string]any)
		if !ok {
			t.Fatalf("llm property %q missing", name)
		}
		if got := descriptionOf(node); got != want {
			t.Errorf("%s description mismatch:\n got: %q\nwant: %q", name, got, want)
		}
	}

	crm, ok := p["crm_signals"].(map[string]any)
	if !ok {
		t.Fatal("llm schema has no crm_signals subtree")
	}
	if got := descriptionOf(crm); got != descCrmRoot {
		t.Errorf("embedded crm_signals root description mismatch:\n got: %q\nwant: %q", got, descCrmRoot)
	}

	// Embedded crm subtree must stay structurally identical to CRMSchemaJSON.
	var embedded, standalone any
	emb, err := json.Marshal(crm)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(emb, &embedded); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(orchestrator.CRMSchemaJSON(), &standalone); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(embedded, standalone) {
		t.Error("crm_signals subtree inside LLMSchemaJSON diverges from CRMSchemaJSON")
	}
}

func TestCRMSchemaStructure(t *testing.T) {
	doc := decodeSchema(t, orchestrator.CRMSchemaJSON())

	if doc["type"] != "object" || doc["additionalProperties"] != false {
		t.Errorf("crm root type/additionalProperties wrong: %v", doc)
	}
	wantRequired := []any{
		"order_confirmed", "detected_items", "delivery_area",
		"customer_name", "detected_preferences", "sentiment",
	}
	if !reflect.DeepEqual(doc["required"], wantRequired) {
		t.Errorf("crm required = %v, want %v (declaration order)", doc["required"], wantRequired)
	}

	p := schemaProps(t, doc)

	sentiment := p["sentiment"].(map[string]any)
	if !reflect.DeepEqual(sentiment["enum"], []any{"positive", "neutral", "negative"}) {
		t.Errorf("sentiment enum = %v", sentiment["enum"])
	}

	// Nullable strings use the zod-to-json-schema anyOf shape.
	for _, name := range []string{"delivery_area", "customer_name"} {
		node := p[name].(map[string]any)
		wantAnyOf := []any{
			map[string]any{"type": "string"},
			map[string]any{"type": "null"},
		}
		if !reflect.DeepEqual(node["anyOf"], wantAnyOf) {
			t.Errorf("%s anyOf = %v, want %v (nullable shape)", name, node["anyOf"], wantAnyOf)
		}
	}

	itemsNode := p["detected_items"].(map[string]any)
	itemObj, ok := itemsNode["items"].(map[string]any)
	if !ok {
		t.Fatal("detected_items.items missing")
	}
	if !reflect.DeepEqual(itemObj["required"], []any{"product_id", "quantity"}) {
		t.Errorf("detected_items[].required = %v", itemObj["required"])
	}
	if itemObj["additionalProperties"] != false {
		t.Error("detected_items[] must set additionalProperties:false")
	}
	qty := schemaProps(t, itemObj)["quantity"].(map[string]any)
	if qty["type"] != "number" {
		t.Errorf("quantity type = %v, want number", qty["type"])
	}
}

func TestLLMSchemaStructure(t *testing.T) {
	doc := decodeSchema(t, orchestrator.LLMSchemaJSON())

	if doc["type"] != "object" || doc["additionalProperties"] != false {
		t.Errorf("llm root type/additionalProperties wrong: %v", doc)
	}
	wantRequired := []any{
		"reply_text", "internal_confidence", "intent",
		"customer_requested_images", "send_product_image_ids",
		"referenced_product_ids", "crm_signals",
	}
	if !reflect.DeepEqual(doc["required"], wantRequired) {
		t.Errorf("llm required = %v, want %v (declaration order)", doc["required"], wantRequired)
	}

	p := schemaProps(t, doc)

	confidence := p["internal_confidence"].(map[string]any)
	if confidence["minimum"] != float64(0) || confidence["maximum"] != float64(1) {
		t.Errorf("internal_confidence bounds = %v/%v, want 0/1", confidence["minimum"], confidence["maximum"])
	}

	wantIntents := []any{
		"product_inquiry", "greeting", "support_faq",
		"checkout_request", "complaint", "image_match", "unknown",
	}
	intent := p["intent"].(map[string]any)
	if !reflect.DeepEqual(intent["enum"], wantIntents) {
		t.Errorf("intent enum = %v, want %v", intent["enum"], wantIntents)
	}

	sendIDs := p["send_product_image_ids"].(map[string]any)
	if sendIDs["maxItems"] != float64(5) {
		t.Errorf("send_product_image_ids maxItems = %v, want 5", sendIDs["maxItems"])
	}
	if !reflect.DeepEqual(sendIDs["items"], map[string]any{"type": "string"}) {
		t.Errorf("send_product_image_ids items = %v", sendIDs["items"])
	}

	for _, name := range []string{"send_product_image_ids", "referenced_product_ids"} {
		arr := p[name].(map[string]any)
		if arr["type"] != "array" {
			t.Errorf("%s type = %v, want array", name, arr["type"])
		}
	}
}
