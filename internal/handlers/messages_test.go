package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
)

// ---- stub MessageSender -------------------------------------------------------

type s5_stubWA struct {
	mu        sync.Mutex
	phoneID   string
	texts     []s5_textCall
	templates []s5_templateCall
	resp      map[string]any
	err       error
}

type s5_textCall struct {
	PhoneNumberID, To, Text string
}

type s5_templateCall struct {
	PhoneNumberID, To, TemplateName, LanguageCode string
}

func (s *s5_stubWA) SendTextMessageData(_ context.Context, phoneNumberID, to, text string) (map[string]any, error) {
	s.mu.Lock()
	s.phoneID = phoneNumberID
	s.texts = append(s.texts, s5_textCall{phoneNumberID, to, text})
	resp, err := s.resp, s.err
	s.mu.Unlock()
	return resp, err
}

func (s *s5_stubWA) SendTemplateMessage(_ context.Context, phoneNumberID, to, templateName, languageCode string) (map[string]any, error) {
	s.mu.Lock()
	s.phoneID = phoneNumberID
	s.templates = append(s.templates, s5_templateCall{phoneNumberID, to, templateName, languageCode})
	resp, err := s.resp, s.err
	s.mu.Unlock()
	return resp, err
}

func (s *s5_stubWA) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.texts), len(s.templates)
}

func s5_mountMessages(t *testing.T, wa MessageSender) http.Handler {
	t.Helper()
	r := chi.NewRouter()
	r.Route("/messages", func(m chi.Router) {
		MountMessages(m, MessagesDeps{WA: wa, PhoneNumberID: "wni_s5_default"})
	})
	return r
}

func s5_postJSON(t *testing.T, h http.Handler, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

var s5_metaResponse = map[string]any{
	"messaging_product": "whatsapp",
	"contacts":          []any{map[string]any{"input": "+233201234567", "wa_id": "233201234567"}},
	"messages":          []any{map[string]any{"id": "wamid.s5msg1"}},
}

// T5.20 POST /messages/send/text — contract + public auth posture.
func TestS5_T5_20_SendTextContract(t *testing.T) {
	stub := &s5_stubWA{resp: s5_metaResponse}
	h := s5_mountMessages(t, stub)

	t.Run("happy path: 201 {success:true,data:<meta response>}", func(t *testing.T) {
		// NO Authorization header — apps/api has no guard on this controller.
		rec := s5_postJSON(t, h, "/messages/send/text", `{"to":"233201234567","text":"Hello!"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		v := ep_decode(t, rec)
		if v["success"] != true {
			t.Fatalf("body=%v", v)
		}
		data, _ := v["data"].(map[string]any)
		if data == nil || data["messaging_product"] != "whatsapp" {
			t.Fatalf("data=%v want the Meta response object", v["data"])
		}
		msgs, _ := data["messages"].([]any)
		if len(msgs) != 1 || msgs[0].(map[string]any)["id"] != "wamid.s5msg1" {
			t.Fatalf("messages=%v", msgs)
		}
		texts, _ := stub.counts()
		if texts != 1 {
			t.Fatalf("stub calls=%d", texts)
		}
		if stub.texts[0].PhoneNumberID != "wni_s5_default" || stub.texts[0].To != "233201234567" ||
			stub.texts[0].Text != "Hello!" {
			t.Fatalf("call args=%+v", stub.texts[0])
		}
	})

	t.Run("validation issues in schema field order", func(t *testing.T) {
		cases := []struct {
			name, body, code, path, msgFragment string
		}{
			{"missing to", `{"text":"hi"}`, "invalid_type", "to", "expected string, received undefined"},
			{"bad phone format", `{"to":"abc","text":"hi"}`, "invalid_format", "to", "must be a valid international phone number"},
			{"missing text", `{"to":"+233201234567"}`, "invalid_type", "text", "expected string, received undefined"},
			{"empty text", `{"to":"+233201234567","text":""}`, "too_small", "text", ">=1"},
			{"4097-char text", `{"to":"+233201234567","text":"` + strings.Repeat("a", 4097) + `"}`, "too_big", "text", "<=4096"},
		}
		for _, tc := range cases {
			rec := s5_postJSON(t, h, "/messages/send/text", tc.body)
			s5_assertZod400(t, tc.name, rec, tc.code, tc.path, tc.msgFragment)
		}
	})

	t.Run("non-object body -> top-level invalid_type", func(t *testing.T) {
		rec := s5_postJSON(t, h, "/messages/send/text", `[1,2]`)
		s5_assertZod400(t, "array body", rec, "invalid_type", "", "expected object")
	})

	t.Run("Meta failure -> 500 AllExceptionsFilter envelope", func(t *testing.T) {
		failing := &s5_stubWA{err: context.DeadlineExceeded}
		fh := s5_mountMessages(t, failing)
		rec := s5_postJSON(t, fh, "/messages/send/text", `{"to":"+233201234567","text":"hi"}`)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status=%d", rec.Code)
		}
		v := ep_decode(t, rec)
		if v["statusCode"] != json_Number("500") || v["message"] != "Internal server error" {
			t.Fatalf("envelope=%v", v)
		}
	})
}

// T5.20 POST /messages/send/template — languageCode optional/defaulted.
func TestS5_T5_20_SendTemplateContract(t *testing.T) {
	stub := &s5_stubWA{resp: s5_metaResponse}
	h := s5_mountMessages(t, stub)

	t.Run("explicit languageCode passes through", func(t *testing.T) {
		rec := s5_postJSON(t, h, "/messages/send/template",
			`{"to":"+233201234567","templateName":"order_shipped","languageCode":"en_GB"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		_, templates := stub.counts()
		if templates != 1 || stub.templates[0].LanguageCode != "en_GB" ||
			stub.templates[0].TemplateName != "order_shipped" || stub.templates[0].To != "+233201234567" {
			t.Fatalf("calls=%+v", stub.templates)
		}
	})

	t.Run("absent languageCode defaults to en_US", func(t *testing.T) {
		rec := s5_postJSON(t, h, "/messages/send/template",
			`{"to":"+233201234567","templateName":"hello_world"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d", rec.Code)
		}
		if last := stub.templates[len(stub.templates)-1]; last.LanguageCode != "en_US" {
			t.Fatalf("languageCode=%q want default en_US", last.LanguageCode)
		}
	})

	t.Run("validation issues", func(t *testing.T) {
		cases := []struct {
			name, body, code, path, msgFragment string
		}{
			{"missing templateName", `{"to":"+233201234567"}`, "invalid_type", "templateName", "undefined"},
			{"empty templateName", `{"to":"+233201234567","templateName":""}`, "too_small", "templateName", ">=1"},
			{"512+ templateName", `{"to":"+233201234567","templateName":"` + strings.Repeat("x", 513) + `"}`, "too_big", "templateName", "<=512"},
			{"bad languageCode case", `{"to":"+233201234567","templateName":"t","languageCode":"EN_us"}`, "invalid_format", "languageCode", "pattern"},
			{"numeric to", `{"to":233201234567,"templateName":"t"}`, "invalid_type", "to", "received number"},
		}
		for _, tc := range cases {
			rec := s5_postJSON(t, h, "/messages/send/template", tc.body)
			s5_assertZod400(t, tc.name, rec, tc.code, tc.path, tc.msgFragment)
		}
	})
}

// s5_assertZod400 checks the nestjs-zod pipe body shape:
// {"statusCode":400,"message":"Validation failed","errors":[<issue>]}.
func s5_assertZod400(t *testing.T, name string, rec *httptest.ResponseRecorder, code, path, msgFragment string) {
	t.Helper()
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("%s: status=%d body=%s", name, rec.Code, rec.Body.String())
	}
	var body struct {
		StatusCode int              `json:"statusCode"`
		Message    string           `json:"message"`
		Errors     []map[string]any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: decode %v", name, err)
	}
	if body.StatusCode != 400 || body.Message != "Validation failed" || len(body.Errors) == 0 {
		t.Fatalf("%s: body=%v", name, body)
	}
	first := body.Errors[0]
	if first["code"] != code {
		t.Errorf("%s: issue code=%v want %s (%v)", name, first["code"], code, first)
	}
	pathVal, _ := first["path"].([]any)
	var gotPath string
	for _, p := range pathVal {
		gotPath += p.(string)
	}
	if gotPath != path {
		t.Errorf("%s: issue path=%q want %q", name, gotPath, path)
	}
	msg, _ := first["message"].(string)
	if !strings.Contains(msg, msgFragment) {
		t.Errorf("%s: message=%q want fragment %q", name, msg, msgFragment)
	}
}

// Mounting shape sanity: MountMessages registers /send/text + /send/template.
func TestS5_MessagesRoutesMounted(t *testing.T) {
	stub := &s5_stubWA{resp: s5_metaResponse}
	h := s5_mountMessages(t, stub)

	for _, p := range []string{"/messages/send/text", "/messages/send/template"} {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("GET %s must not exist (got %d)", p, rec.Code)
		}
	}
}
