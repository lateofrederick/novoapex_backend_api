package apidocs

// Schema helpers. Nullable fields are always present on the wire (Prisma
// emits explicit nulls), so every property is listed in `required`.

func str() obj     { return obj{"type": "string"} }
func nstr() obj    { return obj{"type": "string", "nullable": true} }
func dt() obj      { return obj{"type": "string", "format": "date-time"} }
func ndt() obj     { return obj{"type": "string", "format": "date-time", "nullable": true} }
func integer() obj { return obj{"type": "integer"} }
func boolean() obj { return obj{"type": "boolean"} }
func money() obj {
	return obj{"type": "number", "description": "Money in major units (2 decimal places)"}
}
func arr(items obj) obj    { return obj{"type": "array", "items": items} }
func enum(v ...string) obj { return obj{"type": "string", "enum": v} }

// withNull marks a schema nullable. OpenAPI 3.0 ignores keywords next to
// $ref, so references are wrapped in allOf.
func withNull(o obj) obj {
	if _, isRef := o["$ref"]; isRef {
		return obj{"allOf": []obj{o}, "nullable": true}
	}
	o["nullable"] = true
	return o
}
func describe(o obj, d string) obj { o["description"] = d; return o }

func object(props obj) obj {
	required := make([]string, 0, len(props))
	for k := range props {
		required = append(required, k)
	}
	sortStrings(required)
	return obj{"type": "object", "required": required, "properties": props}
}

// input builds a request DTO: only the listed keys are required.
func input(required []string, props obj) obj {
	o := obj{"type": "object", "properties": props}
	if len(required) > 0 {
		o["required"] = required
	}
	return o
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

var (
	orderStatus        = enum("PENDING", "CONFIRMED", "PAYMENT_PENDING", "PAID", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED")
	paymentStatus      = enum("PENDING", "SUCCESS", "FAILED", "REFUNDED")
	conversationState  = enum("LEAD", "BROWSING", "CHECKOUT", "INVOICING", "SUPPORT", "ESCALATED")
	timeHHMM           = obj{"type": "string", "pattern": "^([01]\\d|2[0-3]):([0-5]\\d)$", "description": "24-hour HH:mm", "example": "08:00"}
	internationalPhone = obj{"type": "string", "pattern": "^\\+?\\d{7,15}$", "description": "International number, digits only (optional +)", "example": "233201234567"}
)

func schemas() obj {
	s := obj{}

	// --- envelopes ---------------------------------------------------------
	s["ErrorEnvelope"] = object(obj{
		"statusCode": integer(),
		"timestamp":  dt(),
		"path":       str(),
		"message":    obj{"description": "Error message (string) or exception response object"},
	})
	s["ValidationError"] = object(obj{
		"statusCode": obj{"type": "integer", "example": 400},
		"message":    obj{"type": "string", "example": "Validation failed"},
		"errors": arr(object(obj{
			"code":    str(),
			"path":    arr(str()),
			"message": str(),
		})),
	})
	s["ProviderError"] = object(obj{"error": str()})
	s["StatusOk"] = object(obj{"status": obj{"type": "string", "example": "ok"}})
	s["MessageResponse"] = object(obj{"message": str()})
	s["SuccessResponse"] = object(obj{"success": obj{"type": "boolean", "example": true}})
	s["SuccessMessage"] = object(obj{"success": boolean(), "message": str()})
	s["PaginatedResponse"] = object(obj{
		"data": arr(obj{"type": "object", "description": "Items of the listed resource (see its schema)"}),
		"meta": object(obj{"total": integer(), "page": integer(), "limit": integer(), "totalPages": integer()}),
	})
	s["HealthResult"] = object(obj{
		"status":  enum("ok", "error", "shutting_down"),
		"info":    obj{"type": "object", "additionalProperties": true},
		"error":   obj{"type": "object", "additionalProperties": true},
		"details": obj{"type": "object", "additionalProperties": true},
	})

	// --- request DTOs (zod schemas) ---------------------------------------
	s["RequestOtpDto"] = input([]string{"phone"}, obj{
		"phone":          obj{"type": "string", "minLength": 10, "description": "The WhatsApp phone number of the vendor (including country code, e.g. +233...)"},
		"email":          obj{"type": "string", "format": "email", "description": "The email address to send the OTP to (required when deliveryMethod is email)"},
		"deliveryMethod": obj{"type": "string", "enum": []string{"email", "whatsapp"}, "default": "email", "description": "How to deliver the OTP"},
	})
	s["VerifyOtpDto"] = input([]string{"phone", "code"}, obj{
		"phone": obj{"type": "string", "minLength": 10},
		"code":  obj{"type": "string", "minLength": 6, "maxLength": 6},
	})
	s["CreateBusinessDto"] = input([]string{"name", "whatsappPhoneNumberId"}, obj{
		"name":                  obj{"type": "string", "minLength": 2},
		"currency":              obj{"type": "string", "default": "GHS"},
		"whatsappPhoneNumberId": obj{"type": "string", "minLength": 5},
		"category":              str(),
		"location":              str(),
	})
	productFields := func() obj {
		return obj{
			"name":         obj{"type": "string", "minLength": 2},
			"description":  str(),
			"price":        obj{"type": "number", "exclusiveMinimum": true, "minimum": 0},
			"stock":        obj{"type": "integer", "minimum": 0},
			"sku":          str(),
			"isAvailable":  obj{"type": "boolean"},
			"category":     str(),
			"stockNote":    str(),
			"deliveryNote": str(),
		}
	}
	create := productFields()
	create["isAvailable"] = obj{"type": "boolean", "default": true}
	s["CreateProductDto"] = input([]string{"name", "price", "stock"}, create)
	s["UpdateProductDto"] = input(nil, productFields())
	s["ReorderImagesDto"] = input([]string{"imageIds"}, obj{
		"imageIds": obj{"type": "array", "minItems": 1, "maxItems": 5, "uniqueItems": true,
			"items": obj{"type": "string", "format": "uuid"}, "description": "Every image id of the product, in the new order"},
	})
	s["CreateLocationDto"] = describe(input([]string{"name", "address", "openingTime", "closingTime"}, obj{
		"name":           obj{"type": "string", "minLength": 2},
		"address":        obj{"type": "string", "minLength": 2},
		"shopNumber":     str(),
		"landmark":       str(),
		"openingTime":    timeHHMM,
		"closingTime":    timeHHMM,
		"offersDelivery": obj{"type": "boolean", "default": true},
		"offersPickup":   obj{"type": "boolean", "default": true},
	}), "A location must offer delivery, pickup, or both.")
	s["UpdateLocationDto"] = describe(input(nil, obj{
		"name":           obj{"type": "string", "minLength": 2},
		"address":        obj{"type": "string", "minLength": 2},
		"shopNumber":     str(),
		"landmark":       str(),
		"openingTime":    timeHHMM,
		"closingTime":    timeHHMM,
		"offersDelivery": boolean(),
		"offersPickup":   boolean(),
		"isActive":       boolean(),
	}), "The resulting location must still offer delivery, pickup, or both.")
	s["UpdateOrderDto"] = input([]string{"status"}, obj{"status": orderStatus})
	s["ReplyDto"] = input([]string{"text"}, obj{"text": obj{"type": "string", "minLength": 1}})
	s["RequestPayoutDto"] = input([]string{"amount", "accountNumber", "bankCode", "accountName"}, obj{
		"amount":        obj{"type": "number", "exclusiveMinimum": true, "minimum": 0},
		"accountNumber": obj{"type": "string", "minLength": 1},
		"bankCode":      obj{"type": "string", "minLength": 1},
		"accountName":   obj{"type": "string", "minLength": 1},
	})
	s["SendTextMessageDto"] = input([]string{"to", "text"}, obj{
		"to":   internationalPhone,
		"text": obj{"type": "string", "minLength": 1, "maxLength": 4096},
	})
	s["SendTemplateMessageDto"] = input([]string{"to", "templateName"}, obj{
		"to":           internationalPhone,
		"templateName": obj{"type": "string", "minLength": 1, "maxLength": 512},
		"languageCode": obj{"type": "string", "pattern": "^[a-z]{2}(_[A-Z]{2})?$", "default": "en_US"},
	})

	// --- response entities -------------------------------------------------
	business := obj{
		"id": str(), "name": str(), "ownerPhone": str(), "whatsappPhoneNumberId": str(),
		"createdAt": dt(), "updatedAt": dt(), "currency": str(), "category": nstr(), "location": nstr(),
		"assistantEnabled": boolean(), "confirmationDelayHours": integer(), "paystackRecipientCode": nstr(),
		"paymentCallbackUrl": nstr(), "templateLanguage": str(), "newArrivalsTemplateName": nstr(),
		"lastNewArrivalsNotifiedAt": ndt(),
	}
	s["Business"] = object(business)
	s["VerifyOtpResponse"] = object(obj{
		"token":     str(),
		"isNewUser": boolean(),
		"business":  withNull(ref("Business")),
	})

	productPlain := obj{
		"id": str(), "businessId": str(), "name": str(), "description": nstr(), "category": nstr(),
		"requestCount": integer(), "stockNote": nstr(), "deliveryNote": nstr(), "price": money(),
		"stock": integer(), "sku": nstr(), "isAvailable": boolean(), "createdAt": dt(), "updatedAt": dt(),
	}
	s["ProductImage"] = object(obj{
		"id": str(), "productId": str(), "url": str(), "storageKey": nstr(), "contentHash": nstr(),
		"position": integer(), "createdAt": dt(),
	})
	product := obj{}
	for k, v := range productPlain {
		product[k] = v
	}
	product["images"] = arr(ref("ProductImage"))
	product["imageUrl"] = describe(nstr(), "Primary image URL (legacy clients)")
	s["Product"] = object(product)
	s["ProductPlain"] = object(productPlain)
	s["ProductMetrics"] = object(obj{
		"totalProducts": integer(), "outOfStock": integer(), "totalInventoryItems": integer(), "totalInventoryValue": money(),
	})

	s["BusinessLocation"] = object(obj{
		"id": str(), "businessId": str(), "name": str(), "address": str(), "shopNumber": nstr(), "landmark": nstr(),
		"openingTime": str(), "closingTime": str(), "offersDelivery": boolean(), "offersPickup": boolean(),
		"isActive": boolean(), "createdAt": dt(), "updatedAt": dt(),
	})

	customerBase := obj{
		"id": str(), "businessId": str(), "phone": str(), "name": nstr(), "acquisitionChannel": nstr(),
		"marketingOptIn": boolean(), "firstContactAt": dt(), "lastContactAt": dt(), "createdAt": dt(), "updatedAt": dt(),
	}
	s["CustomerBase"] = object(customerBase)
	s["CustomerProfile"] = object(obj{
		"id": str(), "customerId": str(), "preferences": arr(str()), "deliveryArea": nstr(),
		"averageOrderValue": withNull(money()), "orderFrequencyDays": obj{"type": "number", "nullable": true},
		"lastOrderAt": ndt(), "lastReengagementAt": ndt(), "totalOrders": integer(), "totalSpent": money(),
		"sentiment": withNull(enum("positive", "neutral", "negative")), "latePaymentCount": integer(),
		"preferredPaymentNetwork": nstr(), "updatedAt": dt(),
	})
	customer := obj{}
	for k, v := range customerBase {
		customer[k] = v
	}
	customer["profile"] = withNull(ref("CustomerProfile"))
	s["Customer"] = object(customer)
	s["CustomersSummary"] = object(obj{"totalCustomers": integer(), "newCustomersLast30Days": integer()})

	s["OrderItem"] = object(obj{
		"id": str(), "orderId": str(), "productId": str(), "productName": str(), "quantity": integer(),
		"unitPrice": money(), "product": ref("ProductPlain"),
	})
	s["Order"] = object(obj{
		"id": str(), "businessId": str(), "customerId": str(), "conversationId": nstr(), "idempotencyKey": nstr(),
		"status": orderStatus, "totalAmount": money(), "currency": str(), "fulfillmentType": enum("DELIVERY", "PICKUP"),
		"locationId": nstr(), "createdAt": dt(), "updatedAt": dt(),
		"customer": withNull(ref("CustomerBase")), "location": withNull(ref("BusinessLocation")),
		"items": describe(arr(ref("OrderItem")), "Present on GET /orders/{id}"),
	})
	s["OrdersSummary"] = object(obj{"totalOrders": integer(), "pendingOrders": integer(), "paidOrders": integer(), "totalRevenue": money()})
	s["FulfillmentResponse"] = object(obj{"success": boolean(), "status": orderStatus})

	s["Conversation"] = object(obj{
		"id": str(), "businessId": str(), "customerId": nstr(), "customerPhone": str(), "state": conversationState,
		"isEscalatedToHuman": boolean(), "language": str(), "escalationReason": nstr(), "createdAt": dt(), "updatedAt": dt(),
		"customer": withNull(ref("CustomerBase")),
	})
	s["InboxSummary"] = object(obj{"totalConversations": integer(), "handoffs": integer()})
	outbound := obj{
		"id": str(), "whatsappMessageId": nstr(), "recipientPhone": str(), "messageType": enum("text", "image", "template"),
		"textContent": nstr(), "templateName": nstr(), "imageUrl": nstr(), "productId": nstr(),
		"rawPayload": obj{"type": "object"}, "metaResponse": obj{"type": "object", "nullable": true},
		"status":     describe(str(), "pending | sent | failed | failed_24h_window_closed | failed_stale"),
		"businessId": nstr(), "conversationId": nstr(), "createdAt": dt(),
	}
	s["OutboundMessage"] = object(outbound)
	inboundItem := object(obj{
		"type": enum("inbound"), "time": dt(), "id": str(), "whatsappMessageId": str(), "senderPhone": str(),
		"recipientPhone": str(), "messageType": str(), "textContent": nstr(), "rawPayload": obj{"type": "object"},
		"timestamp": dt(), "businessId": nstr(), "conversationId": nstr(), "createdAt": dt(),
	})
	outboundItem := obj{"type": enum("outbound"), "time": dt()}
	for k, v := range outbound {
		outboundItem[k] = v
	}
	s["ConversationMessages"] = arr(obj{"oneOf": []obj{inboundItem, object(outboundItem)}})

	s["DashboardSummary"] = object(obj{"pendingPayments": integer(), "activeHandoffs": integer(), "totalPayouts": describe(money(), "Sum of successful payments")})
	s["AnalyticsOverview"] = object(obj{"rescuedLeads": integer(), "totalConversations": integer(), "averageResponseTimeMins": integer()})
	s["HandoffReasons"] = arr(object(obj{"reason": str(), "count": integer()}))
	s["DemandProducts"] = arr(object(obj{"id": str(), "name": str(), "requestCount": integer()}))

	s["Payout"] = object(obj{
		"id": str(), "businessId": str(), "amount": money(), "currency": str(), "status": paymentStatus,
		"reference": nstr(), "paystackTransferId": nstr(), "createdAt": dt(), "updatedAt": dt(),
	})
	s["PayoutBalance"] = object(obj{"totalRevenue": money(), "committedPayouts": money(), "availableBalance": money(), "currency": str()})
	s["SendMessageResponse"] = object(obj{"success": obj{"type": "boolean", "example": true}, "data": obj{"type": "object", "description": "Meta Cloud API response"}})
	return s
}
