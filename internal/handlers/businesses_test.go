package handlers

// businesses_test.go — T4.2a/T4.3 coverage: POST /businesses zod shapes,
// owner-phone binding from JWT claims, 409 conflict (pre-check AND unique
// index race backstop), and the full-row Prisma payload.

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
)

var s4pBusinessKeys = []string{
	"id", "name", "whatsappPhoneNumberId", "createdAt", "updatedAt", "currency",
	"assistantEnabled", "category", "confirmationDelayHours", "location",
	"ownerPhone", "paystackRecipientCode", "paymentCallbackUrl", "templateLanguage",
	"newArrivalsTemplateName", "lastNewArrivalsNotifiedAt",
}

func TestS4pBusinessesCreateConflictAndZod(t *testing.T) {
	e := s4p_start(t)
	h := s4p_businessesHandler(e)
	phone := "+233555010101"
	claims := auth.Claims{Phone: phone}

	// CREATE — full row, DB defaults, ownerPhone bound to the token's phone.
	rec := s4p_json(t, h, http.MethodPost, "/businesses", &claims, map[string]any{
		"name":                  "Kofi's Store",
		"whatsappPhoneNumberId": "109876543210",
	})
	s4p_status(t, rec, http.StatusCreated)
	body := s4p_decode(t, rec)
	if got, want := ep_keys(t, body), s4pBusinessKeys; len(got) != len(want) {
		t.Fatalf("business keys:\n got %v\nwant %v", got, want)
	}
	if s4p_str(body, "ownerPhone") != phone ||
		s4p_str(body, "currency") != "GHS" ||
		s4p_str(body, "templateLanguage") != "en" ||
		s4p_str(body, "name") != "Kofi's Store" ||
		body["assistantEnabled"] != true ||
		body["confirmationDelayHours"] != json_Number("24") ||
		body["category"] != nil || body["location"] != nil ||
		body["paystackRecipientCode"] != nil || body["paymentCallbackUrl"] != nil {
		t.Fatalf("unexpected business payload: %v", body)
	}
	ep_assertISO(t, "createdAt", body["createdAt"])

	// Optional fields pass through when provided.
	rec = s4p_json(t, h, http.MethodPost, "/businesses", &auth.Claims{Phone: "+233555010102"},
		map[string]any{
			"name": "Ama's Shop", "currency": "USD",
			"whatsappPhoneNumberId": "222333444555", "category": "grocery", "location": "Accra",
		})
	s4p_status(t, rec, http.StatusCreated)
	b2 := s4p_decode(t, rec)
	if s4p_str(b2, "currency") != "USD" || s4p_str(b2, "category") != "grocery" || s4p_str(b2, "location") != "Accra" {
		t.Fatalf("optionals lost: %v", b2)
	}

	// CONFLICT — same phone again.
	rec = s4p_json(t, h, http.MethodPost, "/businesses", &claims, map[string]any{
		"name": "Second Store", "whatsappPhoneNumberId": "999888777666",
	})
	s4p_status(t, rec, http.StatusConflict)
	if msg := s4p_message(t, rec); msg != "Business already exists for this phone number" {
		t.Fatalf("conflict message = %q", msg)
	}
	v := s4p_decode(t, rec)
	if v["statusCode"] != json_Number("409") {
		t.Fatalf("conflict envelope: %s", rec.Body.String())
	}

	// ZOD — short name; missing/short whatsappPhoneNumberId.
	rec = s4p_json(t, h, http.MethodPost, "/businesses", &claims, map[string]any{
		"name": "A", "whatsappPhoneNumberId": "123",
	})
	s4p_status(t, rec, http.StatusBadRequest)
	v = s4p_decode(t, rec)
	if v["statusCode"] != json_Number("400") {
		t.Fatalf("zod envelope: %s", rec.Body.String())
	}
	issues := s4p_issues(t, rec)
	if len(issues) != 2 {
		t.Fatalf("want 2 issues, got %s", rec.Body.String())
	}
	p0, _ := issues[0]["path"].([]any)
	if s4p_str(issues[0], "code") != "too_small" || len(p0) != 1 || p0[0] != "name" ||
		s4p_str(issues[0], "message") != "Too small: expected string to have >=2 characters" {
		t.Fatalf("issue0 mismatch: %s", rec.Body.String())
	}
	p1, _ := issues[1]["path"].([]any)
	if s4p_str(issues[1], "code") != "too_small" || len(p1) != 1 || p1[0] != "whatsappPhoneNumberId" ||
		s4p_str(issues[1], "message") != "Too small: expected string to have >=5 characters" {
		t.Fatalf("issue1 mismatch: %s", rec.Body.String())
	}

	// ZOD — missing required field -> invalid_type / undefined at path.
	rec = s4p_json(t, h, http.MethodPost, "/businesses", &claims, map[string]any{"name": "Valid Name"})
	s4p_status(t, rec, http.StatusBadRequest)
	issues = s4p_issues(t, rec)
	if len(issues) != 1 {
		t.Fatalf("want 1 issue, got %s", rec.Body.String())
	}
	if s4p_str(issues[0], "code") != "invalid_type" ||
		s4p_str(issues[0], "message") != "Invalid input: expected string, received undefined" {
		t.Fatalf("missing-field issue mismatch: %s", rec.Body.String())
	}

	// RACE — two concurrent creates with one phone: exactly one wins; the
	// loser conflicts via pre-check or the owner_phone unique backstop.
	raceClaims := auth.Claims{Phone: "+233555010199"}
	var wg sync.WaitGroup
	codes := make([]int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := s4p_json(t, h, http.MethodPost, "/businesses", &raceClaims, map[string]any{
				"name": fmt.Sprintf("Racer %d", i), "whatsappPhoneNumberId": fmt.Sprintf("10000000000%d", i),
			})
			codes[i] = rec.Code
		}(i)
	}
	wg.Wait()
	wins, conflicts := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusCreated:
			wins++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("race produced unexpected status %d", c)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("race outcome wins=%d conflicts=%d (%v)", wins, conflicts, codes)
	}
}
