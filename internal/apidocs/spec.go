// Package apidocs ports the Swagger setup in apps/api/src/main.ts: an OpenAPI
// document for the whole HTTP surface plus Swagger UI at /api-docs. Request
// schemas mirror the zod DTOs (apps/*/src/**/dto, messages.dto.ts) field for
// field; the route inventory is checked against the live router in
// cmd/api/router_test.go so the document cannot silently drift.
package apidocs

// Document is an OpenAPI 3.0 document.
type Document = map[string]any

// Options carries the runtime facts the document describes.
type Options struct {
	// PaymentProviders are the providers accepted at
	// /webhooks/payments/{provider} (PaymentProviderFactory.getAvailableProviders).
	PaymentProviders []string
}

func (o Options) paymentProviders() []string {
	if len(o.PaymentProviders) == 0 {
		return []string{"paystack"}
	}
	return o.PaymentProviders
}

type obj = map[string]any

// operation describes one documented route.
type operation struct {
	method      string
	path        string // OpenAPI form: /products/{id}
	tag         string
	summary     string
	public      bool
	params      []obj
	body        obj // request body object (content already built)
	responses   obj
	description string
}

// Spec builds the OpenAPI document (DocumentBuilder in apps/api/src/main.ts:
// title 'NovOApex API', description 'The combined API documentation for
// NovOApex', version '1.0', bearer auth).
func Spec(opts Options) Document {
	paths := obj{}
	for _, op := range operations(opts) {
		item, ok := paths[op.path].(obj)
		if !ok {
			item = obj{}
			paths[op.path] = item
		}
		o := obj{
			"tags":      []string{op.tag},
			"summary":   op.summary,
			"responses": op.responses,
		}
		if op.description != "" {
			o["description"] = op.description
		}
		if len(op.params) > 0 {
			o["parameters"] = op.params
		}
		if op.body != nil {
			o["requestBody"] = op.body
		}
		if !op.public {
			o["security"] = []obj{{"bearer": []string{}}}
		}
		item[op.method] = o
	}
	return Document{
		"openapi": "3.0.0",
		"info": obj{
			"title":       "NovOApex API",
			"description": "The combined API documentation for NovOApex",
			"version":     "1.0",
			"contact":     obj{},
		},
		"tags":    tags(),
		"servers": []any{},
		"paths":   paths,
		"components": obj{
			"securitySchemes": obj{
				"bearer": obj{"scheme": "bearer", "bearerFormat": "JWT", "type": "http"},
			},
			"schemas": schemas(),
		},
	}
}

// Routes lists every documented "METHOD /path" pair (OpenAPI path form).
func Routes() []string {
	ops := operations(Options{})
	out := make([]string, 0, len(ops))
	for _, op := range ops {
		out = append(out, upper(op.method)+" "+op.path)
	}
	return out
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// building blocks
// ---------------------------------------------------------------------------

func ref(name string) obj { return obj{"$ref": "#/components/schemas/" + name} }

func jsonBody(schema string) obj {
	return obj{"required": true, "content": obj{"application/json": obj{"schema": ref(schema)}}}
}

func pathID(name, description string) obj {
	return obj{"name": name, "in": "path", "required": true, "description": description, "schema": obj{"type": "string"}}
}

func query(name, description string, schema obj) obj {
	return obj{"name": name, "in": "query", "required": false, "description": description, "schema": schema}
}

var pagination = []obj{
	query("page", "Page number (default 1)", obj{"type": "integer", "minimum": 1, "default": 1}),
	query("limit", "Page size (default 20, max 100)", obj{"type": "integer", "minimum": 1, "maximum": 100, "default": 20}),
}

func respJSON(description, schema string) obj {
	r := obj{"description": description}
	if schema != "" {
		r["content"] = obj{"application/json": obj{"schema": ref(schema)}}
	}
	return r
}

func responses(pairs ...any) obj {
	out := obj{}
	for i := 0; i+1 < len(pairs); i += 2 {
		out[pairs[i].(string)] = pairs[i+1]
	}
	return out
}

var (
	errValidation = respJSON("Validation failed", "ValidationError")
	errAuth       = respJSON("Invalid or missing session token", "ErrorEnvelope")
	errNoBusiness = respJSON("No business is associated with this account. Create a business first.", "ErrorEnvelope")
	errNotFound   = respJSON("Resource not found", "ErrorEnvelope")
	errThrottled  = respJSON("ThrottlerException: Too Many Requests", "ErrorEnvelope")
)

// vendor returns the standard response set of a business-scoped route.
func vendor(status, description, schema string, extra ...any) obj {
	pairs := append([]any{status, respJSON(description, schema), "401", errAuth, "403", errNoBusiness, "429", errThrottled}, extra...)
	return responses(pairs...)
}

func tags() []obj {
	return []obj{
		{"name": "auth", "description": "OTP login and session"},
		{"name": "businesses", "description": "Vendor onboarding"},
		{"name": "products", "description": "Catalog and product images"},
		{"name": "locations", "description": "Physical shops (pickup/delivery)"},
		{"name": "customers", "description": "Stealth CRM customers"},
		{"name": "orders", "description": "Orders and fulfillment"},
		{"name": "inbox", "description": "Conversations, human handoff and replies"},
		{"name": "analytics", "description": "Dashboard and analytics"},
		{"name": "payouts", "description": "Balance and withdrawals"},
		{"name": "messages", "description": "Manual WhatsApp sends"},
		{"name": "webhooks", "description": "Provider webhooks (signature-verified)"},
		{"name": "health", "description": "Probes and metrics"},
	}
}

// ---------------------------------------------------------------------------
// routes
// ---------------------------------------------------------------------------

func operations(opts Options) []operation {
	id := func(what string) []obj { return []obj{pathID("id", what+" id")} }
	return []operation{
		// health / platform
		{method: "get", path: "/health", tag: "health", public: true, summary: "Composite health (database + Redis)",
			responses: responses("200", respJSON("All dependencies up", "HealthResult"), "503", respJSON("A dependency is down", "HealthResult"))},
		{method: "get", path: "/health/live", tag: "health", public: true, summary: "Liveness probe",
			responses: responses("200", respJSON("Process is alive", "StatusOk"))},
		{method: "get", path: "/health/ready", tag: "health", public: true, summary: "Readiness probe (database + Redis)",
			responses: responses("200", respJSON("Ready", "HealthResult"), "503", respJSON("Not ready", "HealthResult"))},
		{method: "get", path: "/metrics", tag: "health", public: true, summary: "Prometheus metrics",
			responses: responses("200", obj{"description": "Prometheus text exposition", "content": obj{"text/plain": obj{"schema": obj{"type": "string"}}}})},
		{method: "get", path: "/", tag: "health", summary: "Hello World",
			responses: responses("200", obj{"description": "The string 'Hello World!'", "content": obj{"text/html": obj{"schema": obj{"type": "string"}}}}, "401", errAuth)},

		// auth
		{method: "post", path: "/auth/request-otp", tag: "auth", public: true, summary: "Send a one-time login code",
			description: "Delivers a 6-digit code (valid 5 minutes) by email or WhatsApp. Limited to 5 requests per minute per IP.",
			body:        jsonBody("RequestOtpDto"),
			responses:   responses("200", respJSON("OTP sent", "MessageResponse"), "400", errValidation, "429", errThrottled)},
		{method: "post", path: "/auth/verify-otp", tag: "auth", public: true, summary: "Exchange a code for a session token",
			description: "Codes are burned after use or after 5 wrong attempts. Tokens last 7 days. Limited to 5 requests per minute per IP.",
			body:        jsonBody("VerifyOtpDto"),
			responses:   responses("200", respJSON("Session issued", "VerifyOtpResponse"), "400", errValidation, "401", respJSON("Invalid or expired OTP", "ErrorEnvelope"), "429", errThrottled)},
		{method: "get", path: "/auth/me", tag: "auth", summary: "The authenticated vendor's business",
			responses: responses("200", respJSON("Business profile", "Business"), "401", respJSON("Invalid session or business profile not found", "ErrorEnvelope"))},

		// businesses
		{method: "post", path: "/businesses", tag: "businesses", summary: "Create the vendor's business",
			description: "One business per owner phone. The existing session token gains access to business routes immediately.",
			body:        jsonBody("CreateBusinessDto"),
			responses:   responses("201", respJSON("Business created", "Business"), "400", errValidation, "401", errAuth, "409", respJSON("Business already exists for this phone number", "ErrorEnvelope"))},

		// products
		{method: "get", path: "/products", tag: "products", summary: "List products", params: pagination,
			responses: vendor("200", "Paginated products (with ordered images and legacy imageUrl)", "PaginatedResponse", "400", errValidation)},
		{method: "post", path: "/products", tag: "products", summary: "Create a product", body: jsonBody("CreateProductDto"),
			description: "Queues a text-embedding job for semantic catalog search.",
			responses:   vendor("201", "Product created", "Product", "400", errValidation)},
		{method: "get", path: "/products/metrics", tag: "products", summary: "Inventory metrics",
			responses: vendor("200", "Totals, out-of-stock count and inventory value", "ProductMetrics")},
		{method: "get", path: "/products/{id}", tag: "products", summary: "Get a product", params: id("Product"),
			responses: vendor("200", "Product", "Product", "404", errNotFound)},
		{method: "patch", path: "/products/{id}", tag: "products", summary: "Update a product", params: id("Product"), body: jsonBody("UpdateProductDto"),
			responses: vendor("200", "Updated product", "Product", "400", errValidation, "404", errNotFound)},
		{method: "delete", path: "/products/{id}", tag: "products", summary: "Delete a product and its stored images", params: id("Product"),
			responses: vendor("200", "Deleted", "MessageResponse", "404", errNotFound)},
		{method: "post", path: "/products/{id}/out-of-stock", tag: "products", summary: "Mark out of stock (stock 0, unavailable)", params: id("Product"),
			responses: vendor("201", "Updated product", "Product", "404", errNotFound)},
		{method: "post", path: "/products/{id}/images", tag: "products", summary: "Upload 1–5 images", params: id("Product"),
			description: "PNG/JPEG up to 5 MB each; a product holds at most 5 images. Duplicate bytes are skipped, so retries are idempotent.",
			body:        multipart("files", true),
			responses:   vendor("201", "Product with its images", "Product", "400", errValidation, "404", errNotFound, "409", respJSON("A concurrent upload completed first; retry", "ErrorEnvelope"))},
		{method: "post", path: "/products/{id}/image", tag: "products", summary: "Upload a single image (legacy alias)", params: id("Product"),
			body:      multipart("file", false),
			responses: vendor("201", "Product with its images", "Product", "400", errValidation, "404", errNotFound)},
		{method: "delete", path: "/products/{id}/images/{imageId}", tag: "products", summary: "Remove an image",
			params:    []obj{pathID("id", "Product id"), pathID("imageId", "Product image id")},
			responses: vendor("200", "Product with remaining images", "Product", "404", errNotFound)},
		{method: "put", path: "/products/{id}/images/reorder", tag: "products", summary: "Reorder images", params: id("Product"),
			description: "imageIds must list every image of the product exactly once.",
			body:        jsonBody("ReorderImagesDto"),
			responses:   vendor("200", "Product with reordered images", "Product", "400", errValidation, "404", errNotFound)},

		// locations
		{method: "get", path: "/locations", tag: "locations", summary: "List locations", params: pagination,
			responses: vendor("200", "Paginated locations", "PaginatedResponse", "400", errValidation)},
		{method: "post", path: "/locations", tag: "locations", summary: "Create a location", body: jsonBody("CreateLocationDto"),
			responses: vendor("201", "Location created", "BusinessLocation", "400", errValidation)},
		{method: "get", path: "/locations/{id}", tag: "locations", summary: "Get a location", params: id("Location"),
			responses: vendor("200", "Location", "BusinessLocation", "404", errNotFound)},
		{method: "patch", path: "/locations/{id}", tag: "locations", summary: "Update a location", params: id("Location"), body: jsonBody("UpdateLocationDto"),
			responses: vendor("200", "Updated location", "BusinessLocation", "400", errValidation, "404", errNotFound)},
		{method: "delete", path: "/locations/{id}", tag: "locations", summary: "Delete a location", params: id("Location"),
			responses: vendor("200", "Deleted", "MessageResponse", "404", errNotFound)},

		// customers
		{method: "get", path: "/customers", tag: "customers", summary: "List customers (most recent contact first)", params: pagination,
			responses: vendor("200", "Paginated customers", "PaginatedResponse", "400", errValidation)},
		{method: "get", path: "/customers/summary", tag: "customers", summary: "Customer totals",
			responses: vendor("200", "Total and new-in-30-days counts", "CustomersSummary")},
		{method: "get", path: "/customers/{id}", tag: "customers", summary: "Customer with CRM profile", params: id("Customer"),
			responses: vendor("200", "Customer", "Customer", "404", errNotFound)},

		// orders
		{method: "get", path: "/orders", tag: "orders", summary: "List orders", params: pagination,
			responses: vendor("200", "Paginated orders with customer and location", "PaginatedResponse", "400", errValidation)},
		{method: "get", path: "/orders/summary", tag: "orders", summary: "Order totals and paid revenue",
			responses: vendor("200", "Order summary", "OrdersSummary")},
		{method: "get", path: "/orders/{id}", tag: "orders", summary: "Order with customer, location and items", params: id("Order"),
			responses: vendor("200", "Order", "Order", "404", errNotFound)},
		{method: "patch", path: "/orders/{id}/fulfillment", tag: "orders", summary: "Update order status", params: id("Order"),
			description: "Moving to DELIVERED schedules a delivery-confirmation follow-up two hours later.",
			body:        jsonBody("UpdateOrderDto"),
			responses:   vendor("200", "New status", "FulfillmentResponse", "400", errValidation, "404", errNotFound)},
		{method: "post", path: "/orders/{id}/escalate", tag: "orders", summary: "Hand the order's conversation to a human", params: id("Order"),
			responses: vendor("201", "Escalated", "SuccessMessage", "404", errNotFound)},

		// inbox / conversations
		{method: "get", path: "/inbox/summary", tag: "inbox", summary: "Conversation and handoff counts",
			responses: vendor("200", "Inbox summary", "InboxSummary")},
		{method: "get", path: "/inbox/handoffs", tag: "inbox", summary: "Conversations escalated to a human", params: pagination,
			responses: vendor("200", "Paginated conversations with customer", "PaginatedResponse", "400", errValidation)},
		{method: "get", path: "/conversations/{id}/messages", tag: "inbox", summary: "Merged inbound/outbound thread (oldest first)",
			params:    []obj{pathID("id", "Conversation id"), query("limit", "Most recent messages to return (default 50, max 200)", obj{"type": "integer", "minimum": 1, "maximum": 200, "default": 50})},
			responses: vendor("200", "Messages", "ConversationMessages", "404", errNotFound)},
		{method: "post", path: "/conversations/{id}/takeover", tag: "inbox", summary: "Pause automation and take over", params: id("Conversation"),
			responses: vendor("201", "Taken over", "SuccessResponse", "404", errNotFound)},
		{method: "post", path: "/conversations/{id}/release", tag: "inbox", summary: "Hand the conversation back to the assistant", params: id("Conversation"),
			responses: vendor("201", "Released", "SuccessResponse", "404", errNotFound)},
		{method: "post", path: "/conversations/{id}/reply", tag: "inbox", summary: "Reply to the customer", params: id("Conversation"),
			description: "The message is queued for delivery. Outside WhatsApp's 24-hour customer-service window it is recorded with status failed_24h_window_closed and not sent.",
			body:        jsonBody("ReplyDto"),
			responses:   vendor("201", "Outbound message record", "OutboundMessage", "400", errValidation, "404", errNotFound)},

		// analytics
		{method: "get", path: "/dashboard/summary", tag: "analytics", summary: "Dashboard headline numbers",
			responses: vendor("200", "Pending payments, active handoffs, settled revenue", "DashboardSummary")},
		{method: "get", path: "/analytics/overview", tag: "analytics", summary: "Rescued leads and conversation totals",
			responses: vendor("200", "Overview", "AnalyticsOverview")},
		{method: "get", path: "/analytics/handoff-reasons", tag: "analytics", summary: "Escalation reasons by frequency",
			responses: vendor("200", "Reasons", "HandoffReasons")},
		{method: "get", path: "/analytics/demand", tag: "analytics", summary: "Top 5 requested products",
			responses: vendor("200", "Products by request count", "DemandProducts")},

		// payouts
		{method: "get", path: "/payouts/balance", tag: "payouts", summary: "Withdrawable balance",
			responses: vendor("200", "Settled revenue minus committed payouts", "PayoutBalance", "400", respJSON("Business not found", "ErrorEnvelope"))},
		{method: "get", path: "/payouts/history", tag: "payouts", summary: "Payout history", params: pagination,
			responses: vendor("200", "Paginated payouts", "PaginatedResponse", "400", errValidation)},
		{method: "post", path: "/payouts/request", tag: "payouts", summary: "Withdraw to mobile money / bank", body: jsonBody("RequestPayoutDto"),
			description: "Funds are reserved atomically before the transfer; a failed transfer releases them.",
			responses:   vendor("201", "Payout record", "Payout", "400", respJSON("Validation failed, insufficient balance, or transfer failure", "ErrorEnvelope"))},

		// messages
		{method: "post", path: "/messages/send/text", tag: "messages", summary: "Send a free-form WhatsApp text",
			description: "Uses the platform phone number (WHATSAPP_PHONE_NUMBER_ID). Only delivered inside the 24-hour customer-service window.",
			body:        jsonBody("SendTextMessageDto"),
			responses:   responses("201", respJSON("Meta API response", "SendMessageResponse"), "400", errValidation, "401", errAuth, "429", errThrottled, "500", respJSON("Meta API error", "ErrorEnvelope"))},
		{method: "post", path: "/messages/send/template", tag: "messages", summary: "Send an approved WhatsApp template",
			body:      jsonBody("SendTemplateMessageDto"),
			responses: responses("201", respJSON("Meta API response", "SendMessageResponse"), "400", errValidation, "401", errAuth, "429", errThrottled, "500", respJSON("Meta API error", "ErrorEnvelope"))},

		// webhooks
		{method: "get", path: "/webhooks/whatsapp", tag: "webhooks", public: true, summary: "Meta webhook verification handshake",
			params: []obj{
				query("hub.mode", "Must be 'subscribe'", obj{"type": "string"}),
				query("hub.verify_token", "Must equal WHATSAPP_VERIFY_TOKEN", obj{"type": "string"}),
				query("hub.challenge", "Echoed back on success", obj{"type": "string"}),
			},
			responses: responses("200", obj{"description": "The raw challenge", "content": obj{"text/html": obj{"schema": obj{"type": "string"}}}}, "403", respJSON("Verification failed", "ErrorEnvelope"))},
		{method: "post", path: "/webhooks/whatsapp", tag: "webhooks", public: true, summary: "Inbound WhatsApp messages",
			description: "Verified with X-Hub-Signature-256 when WHATSAPP_APP_SECRET is set. Each message is deduplicated and processed asynchronously.",
			params:      []obj{{"name": "X-Hub-Signature-256", "in": "header", "required": false, "schema": obj{"type": "string"}, "description": "sha256=<hex HMAC of the raw body>"}},
			body:        obj{"required": true, "content": obj{"application/json": obj{"schema": obj{"type": "object", "description": "Meta Cloud API webhook payload"}}}},
			responses:   responses("200", respJSON("Accepted", "StatusOk"), "403", respJSON("Invalid signature", "ErrorEnvelope"))},
		{method: "post", path: "/webhooks/payments/{provider}", tag: "webhooks", public: true, summary: "Payment provider webhook",
			params: []obj{
				{"name": "provider", "in": "path", "required": true, "schema": obj{"type": "string", "enum": opts.paymentProviders()}},
				{"name": "X-Paystack-Signature", "in": "header", "required": false, "schema": obj{"type": "string"}, "description": "Hex HMAC-SHA512 of the raw body"},
			},
			body:      obj{"required": true, "content": obj{"application/json": obj{"schema": obj{"type": "object", "description": "Provider event payload"}}}},
			responses: responses("200", respJSON("Accepted", "StatusOk"), "400", respJSON("Unknown provider or invalid signature", "ProviderError"), "500", respJSON("Server configuration error", "ProviderError"))},
	}
}

func multipart(field string, many bool) obj {
	file := obj{"type": "string", "format": "binary"}
	prop := file
	if many {
		prop = obj{"type": "array", "items": file, "minItems": 1, "maxItems": 5}
	}
	return obj{"required": true, "content": obj{"multipart/form-data": obj{"schema": obj{
		"type": "object", "required": []string{field}, "properties": obj{field: prop},
	}}}}
}
