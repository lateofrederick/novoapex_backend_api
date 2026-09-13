// JSON Schema ports (T8.12) of LlmResponseSchema and CrmSignalsSchema from
// libs/orchestrator/src/llm/structured-output.ts. The literals are hand-written
// to match zod-to-json-schema's output SHAPE (type/enum/description/minimum/
// maximum/maxItems/items/required, required arrays in declaration order,
// additionalProperties:false on every object); all .describe() texts are
// embedded VERBATIM — they are load-bearing prompt text. Nullable strings use
// the zod-to-json-schema anyOf shape ({string},{null}).
package orchestrator

// CRMSchemaJSON returns the JSON Schema for CrmSignalsSchema.
func CRMSchemaJSON() []byte {
	return []byte(`{
  "type": "object",
  "properties": {
    "order_confirmed": {
      "type": "boolean",
      "description": "True ONLY if the user explicitly confirmed/agreed to place an order in THIS message turn (e.g. \"yes\", \"confirm\", \"go ahead\"). False for inquiries, browsing, or initial order requests that still need confirmation."
    },
    "detected_items": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "product_id": {
            "type": "string",
            "description": "The product ID from the catalog (the short ID in [ID: xxx])"
          },
          "quantity": {
            "type": "number",
            "description": "Quantity requested. Provide 1 if the user did not specify."
          }
        },
        "required": [
          "product_id",
          "quantity"
        ],
        "additionalProperties": false
      },
      "description": "Products the user wants to order. Only populate when there is a clear purchase intent, not just browsing or asking about a product."
    },
    "delivery_area": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "description": "Delivery location/area if the user mentioned it in this turn. Null if not mentioned."
    },
    "fulfillment_choice": {
      "anyOf": [
        {
          "type": "string",
          "enum": [
            "delivery",
            "pickup"
          ]
        },
        {
          "type": "null"
        }
      ],
      "description": "Whether the customer wants delivery or pickup. Populate this on the turn they choose it AND again on the confirmation turn (order_confirmed=true) — like detected_items, it must carry forward, not just appear once. Null if not yet addressed."
    },
    "pickup_location_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "description": "The short ID (from [LOC: xxxxxxxx] in the PICKUP LOCATIONS section) of the location the customer selected. Only relevant when fulfillment_choice is \"pickup\" — must also carry forward onto the confirmation turn. Null otherwise."
    },
    "customer_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "description": "The customer name if they introduced themselves or it can be clearly inferred from the message. Null otherwise."
    },
    "detected_preferences": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "Product categories or attributes the customer shows interest in (e.g. \"organic\", \"large size\", \"spicy\", \"hair products\"). Empty array if none detected in this turn."
    },
    "wants_updates": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "description": "True ONLY if the customer explicitly agrees to receive updates/notifications about new products or arrivals (e.g. \"yes please\", \"sure, keep me posted\", \"sign me up\") in direct response to being asked, or volunteers it unprompted. False ONLY if they explicitly decline such an offer. Null when the topic has not come up this turn — do NOT infer this from general enthusiasm about a product."
    },
    "sentiment": {
      "type": "string",
      "enum": [
        "positive",
        "neutral",
        "negative"
      ],
      "description": "Overall customer sentiment in this message turn. Complaints/frustration → negative, thanks/praise → positive, neutral otherwise."
    }
  },
  "required": [
    "order_confirmed",
    "detected_items",
    "delivery_area",
    "fulfillment_choice",
    "pickup_location_id",
    "customer_name",
    "detected_preferences",
    "wants_updates",
    "sentiment"
  ],
  "additionalProperties": false,
  "description": "Lightweight CRM signals. Only populate fields that are EXPLICITLY present in the current message turn. Do not guess or infer. When in doubt, leave fields as null/empty — false negatives are better than false positives for CRM data."
}`)
}

// LLMSchemaJSON returns the JSON Schema for LlmResponseSchema (crm_signals is
// the CrmSignalsSchema subtree).
func LLMSchemaJSON() []byte {
	return []byte(`{
  "type": "object",
  "properties": {
    "reply_text": {
      "type": "string",
      "description": "The precise text response to send back to the user via WhatsApp."
    },
    "internal_confidence": {
      "type": "number",
      "minimum": 0,
      "maximum": 1,
      "description": "Confidence score from 0.0 to 1.0 that this response is safe, accurate, and perfectly addresses the user intent without hallucinations."
    },
    "intent": {
      "type": "string",
      "enum": [
        "product_inquiry",
        "greeting",
        "support_faq",
        "checkout_request",
        "complaint",
        "image_match",
        "unknown"
      ],
      "description": "The extracted intent of the user's message. Use 'image_match' when the customer sent an image that visually matched a product in the catalog."
    },
    "customer_requested_images": {
      "type": "boolean",
      "description": "True ONLY if the customer explicitly asked to see a photo/picture/image in THIS message turn — including \"yes\"/\"sure\" in direct reply to your offer to send photos, or sending their own photo asking which product it is. False for every other turn, including when they merely mention or order a product."
    },
    "send_product_image_ids": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "maxItems": 5,
      "description": "Product short IDs whose images should be sent. Leave EMPTY unless customer_requested_images is true — product listings are text-first, and images are only sent on request. Only include products marked [HAS_IMAGE] in the catalog, and never a product listed under IMAGES ALREADY SENT unless the customer asked to see it again. Max 5."
    },
    "referenced_product_ids": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "The IDs of products referenced in the reply (from [ID: xxx] in the catalog). Empty array if no products were referenced."
    },
    "crm_signals": {
      "type": "object",
      "properties": {
        "order_confirmed": {
          "type": "boolean",
          "description": "True ONLY if the user explicitly confirmed/agreed to place an order in THIS message turn (e.g. \"yes\", \"confirm\", \"go ahead\"). False for inquiries, browsing, or initial order requests that still need confirmation."
        },
        "detected_items": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "product_id": {
                "type": "string",
                "description": "The product ID from the catalog (the short ID in [ID: xxx])"
              },
              "quantity": {
                "type": "number",
                "description": "Quantity requested. Provide 1 if the user did not specify."
              }
            },
            "required": [
              "product_id",
              "quantity"
            ],
            "additionalProperties": false
          },
          "description": "Products the user wants to order. Only populate when there is a clear purchase intent, not just browsing or asking about a product."
        },
        "delivery_area": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "description": "Delivery location/area if the user mentioned it in this turn. Null if not mentioned."
        },
        "fulfillment_choice": {
          "anyOf": [
            {
              "type": "string",
              "enum": [
                "delivery",
                "pickup"
              ]
            },
            {
              "type": "null"
            }
          ],
          "description": "Whether the customer wants delivery or pickup. Populate this on the turn they choose it AND again on the confirmation turn (order_confirmed=true) — like detected_items, it must carry forward, not just appear once. Null if not yet addressed."
        },
        "pickup_location_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "description": "The short ID (from [LOC: xxxxxxxx] in the PICKUP LOCATIONS section) of the location the customer selected. Only relevant when fulfillment_choice is \"pickup\" — must also carry forward onto the confirmation turn. Null otherwise."
        },
        "customer_name": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "description": "The customer name if they introduced themselves or it can be clearly inferred from the message. Null otherwise."
        },
        "detected_preferences": {
          "type": "array",
          "items": {
            "type": "string"
          },
          "description": "Product categories or attributes the customer shows interest in (e.g. \"organic\", \"large size\", \"spicy\", \"hair products\"). Empty array if none detected in this turn."
        },
        "wants_updates": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "description": "True ONLY if the customer explicitly agrees to receive updates/notifications about new products or arrivals (e.g. \"yes please\", \"sure, keep me posted\", \"sign me up\") in direct response to being asked, or volunteers it unprompted. False ONLY if they explicitly decline such an offer. Null when the topic has not come up this turn — do NOT infer this from general enthusiasm about a product."
        },
        "sentiment": {
          "type": "string",
          "enum": [
            "positive",
            "neutral",
            "negative"
          ],
          "description": "Overall customer sentiment in this message turn. Complaints/frustration → negative, thanks/praise → positive, neutral otherwise."
        }
      },
      "required": [
        "order_confirmed",
        "detected_items",
        "delivery_area",
        "fulfillment_choice",
        "pickup_location_id",
        "customer_name",
        "detected_preferences",
        "wants_updates",
        "sentiment"
      ],
      "additionalProperties": false,
      "description": "Lightweight CRM signals. Only populate fields that are EXPLICITLY present in the current message turn. Do not guess or infer. When in doubt, leave fields as null/empty — false negatives are better than false positives for CRM data."
    }
  },
  "required": [
    "reply_text",
    "internal_confidence",
    "intent",
    "customer_requested_images",
    "send_product_image_ids",
    "referenced_product_ids",
    "crm_signals"
  ],
  "additionalProperties": false
}`)
}
