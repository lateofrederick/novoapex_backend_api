// Package handlers — Stage 5 messages surface (T5.19, T5.20).
//
// messages.go ports MessagesController
// (apps/api/src/messages/messages.controller.ts):
//
//	POST /messages/send/text      body {to, text}
//	POST /messages/send/template  body {to, templateName, languageCode?}
//
// Researched contracts, quoted from source:
//   - AUTH POSTURE: NONE. apps/api registers no global guard
//     (app.module.ts providers carry only the ZodValidationPipe APP_PIPE;
//     the JwtAuthGuard is scoped to MobileApiModule), and the controller has
//     no guards — both routes are public. The Go mount adds no auth either.
//   - RESPONSE: Nest @Post default status 201 with
//     { success: true, data: <Meta API response object> }.
//   - Meta failure -> WhatsAppService throws Error('Meta API error: ...') ->
//     AllExceptionsFilter envelope 500 {"statusCode":500,...,
//     "message":"Internal server error"}.
//   - Validation: nestjs-zod pipe (apps/api app.module APP_PIPE) emits
//     {"statusCode":400,"message":"Validation failed","errors":[<zod v4
//     issues>]} for SendTextMessageDto / SendTemplateMessageDto
//     (messages.dto.ts). Field order follows schema definition order.
//     Delta (inherited from auth.go): FieldIssue carries code/path/message;
//     the extra zod-v4 keys (expected/origin/minimum/format/pattern) are not
//     rendered by httpx.WriteZodValidationError.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// MessageSender is the consumer-side seam over the WhatsApp client that the
// messages endpoints need; *whatsapp.Client satisfies it.
type MessageSender interface {
	SendTextMessageData(ctx context.Context, phoneNumberID, to, text string) (map[string]any, error)
	SendTemplateMessage(ctx context.Context, phoneNumberID, to, templateName, languageCode string) (map[string]any, error)
}

// MessagesDeps carries the messages collaborators.
type MessagesDeps struct {
	WA            MessageSender
	PhoneNumberID string // WHATSAPP_PHONE_NUMBER_ID ("system default" in the source)
}

// MountMessages registers POST /send/text and POST /send/template on r
// (mount under "/messages" centrally).
func MountMessages(r chi.Router, d MessagesDeps) {
	r.Post("/send/text", ms_sendText(d))
	r.Post("/send/template", ms_sendTemplate(d))
}

var ms_phoneRe = regexp.MustCompile(`^\+?\d{7,15}$`)

const ms_phonePattern = `/^\+?\d{7,15}$/`
const ms_languagePattern = `/^[a-z]{2}(_[A-Z]{2})?$/`

// ms_sendText ports sendText (messages.controller.ts:22-29): REST trigger
// log, system-default phone number id, {success:true,data:<meta response>}.
func ms_sendText(d MessagesDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		obj, ok := ms_object(w, r)
		if !ok {
			return
		}
		to, text, issues := ms_validateSendText(obj)
		if len(issues) > 0 {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		slog.Info(fmt.Sprintf("REST trigger: send text to %s", to))
		result, err := d.WA.SendTextMessageData(r.Context(), d.PhoneNumberID, to, text)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusCreated, map[string]any{"success": true, "data": result})
	}
}

// ms_sendTemplate ports sendTemplate (messages.controller.ts:38-51).
func ms_sendTemplate(d MessagesDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		obj, ok := ms_object(w, r)
		if !ok {
			return
		}
		to, name, lang, issues := ms_validateSendTemplate(obj)
		if len(issues) > 0 {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		slog.Info(fmt.Sprintf("REST trigger: send template %q to %s", name, to))
		result, err := d.WA.SendTemplateMessage(r.Context(), d.PhoneNumberID, to, name, lang)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		_ = httpx.WriteJSON(w, http.StatusCreated, map[string]any{"success": true, "data": result})
	}
}

// ---- zod-equivalent validation ---------------------------------------------
//
// Hand-rolled against SendTextMessageSchema / SendTemplateMessageSchema
// (apps/api/src/messages/messages.dto.ts), pinned zod v4.3.6 issue shapes:

// ms_object decodes the body and rejects non-objects with z.object's
// top-level invalid_type issue (shared helper from auth.go).
func ms_object(w http.ResponseWriter, r *http.Request) (*map[string]any, bool) {
	var raw any
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		raw = authMissingValue // empty body == undefined for zod
	}
	obj, issues := auth_objectIssues(raw)
	if len(issues) > 0 {
		httpx.WriteZodValidationError(w, issues)
		return nil, false
	}
	return obj, true
}

func ms_validateSendText(obj *map[string]any) (to, text string, issues []httpx.FieldIssue) {
	to, toErr := ms_checkPhone(auth_field(obj, "to"))
	if toErr != nil {
		issues = append(issues, *toErr)
	}
	text, textErr := ms_boundedString(auth_field(obj, "text"), "text", 1, 4096)
	if textErr != nil {
		issues = append(issues, *textErr)
	}
	return to, text, issues
}

func ms_validateSendTemplate(obj *map[string]any) (to, name, language string, issues []httpx.FieldIssue) {
	to, toErr := ms_checkPhone(auth_field(obj, "to"))
	if toErr != nil {
		issues = append(issues, *toErr)
	}
	name, nameErr := ms_boundedString(auth_field(obj, "templateName"), "templateName", 1, 512)
	if nameErr != nil {
		issues = append(issues, *nameErr)
	}

	// languageCode optional + defaulted ('en_US').
	language = "en_US"
	if v := auth_field(obj, "languageCode"); v != authMissingValue && v != nil {
		s, ok := v.(string)
		if !ok {
			issues = append(issues, httpx.FieldIssue{
				Code:    "invalid_type",
				Path:    "languageCode",
				Message: fmt.Sprintf("Invalid input: expected string, received %s", auth_jsonTypeName(v)),
			})
		} else if !ms_langRe.MatchString(s) {
			language = "" // failed candidate is not used downstream
			issues = append(issues, httpx.FieldIssue{
				Code:    "invalid_format",
				Path:    "languageCode",
				Message: fmt.Sprintf("Invalid string: must match pattern %s", ms_languagePattern),
			})
		} else {
			language = s
		}
	}
	return to, name, language, issues
}

var ms_langRe = regexp.MustCompile(`^[a-z]{2}(_[A-Z]{2})?$`)

// ms_checkPhone renders messages.dto.ts's phoneNumber schema:
// regex ^\+?\d{7,15}$ with the custom message 'must be a valid international
// phone number' (zod v4 invalid_format issue).
func ms_checkPhone(v any) (string, *httpx.FieldIssue) {
	s, err := auth_stringField(v, "to")
	if err != nil {
		return "", err
	}
	if !ms_phoneRe.MatchString(s) {
		return s, &httpx.FieldIssue{
			Code:    "invalid_format",
			Path:    "to",
			Message: "must be a valid international phone number",
		}
	}
	return s, nil
}

// ms_boundedString renders z.string().min(min).max(max).
func ms_boundedString(v any, field string, min, max int) (string, *httpx.FieldIssue) {
	s, err := auth_stringField(v, field)
	if err != nil {
		return "", err
	}
	switch {
	case len(s) < min:
		return s, &httpx.FieldIssue{
			Code:    "too_small",
			Path:    field,
			Message: fmt.Sprintf("Too small: expected string to have >=%d characters", min),
		}
	case len(s) > max:
		return s, &httpx.FieldIssue{
			Code:    "too_big",
			Path:    field,
			Message: fmt.Sprintf("Too big: expected string to have <=%d characters", max),
		}
	}
	return s, nil
}
