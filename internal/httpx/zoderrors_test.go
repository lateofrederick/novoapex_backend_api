package httpx_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// Researched shape (nestjs-zod ZodValidationException,
// node_modules/nestjs-zod/dist/index.mjs): top-level keys statusCode /
// message / errors; each error carries the raw zod issue key set
// code / path / message.
func TestWriteZodValidationError_Shape(t *testing.T) {
	rec := httptest.NewRecorder()

	httpx.WriteZodValidationError(rec, []httpx.FieldIssue{
		{Code: "too_small", Path: "page", Message: "Number must be greater than or equal to 1"},
		{Code: "too_big", Path: "limit", Message: "Number must be less than or equal to 100"},
	})

	if rec.Code != 400 {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}

	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	topKeys := map[string]bool{"statusCode": true, "message": true, "errors": true}
	for k := range decoded {
		if !topKeys[k] {
			t.Fatalf("unexpected top-level key %q in %v", k, decoded)
		}
	}
	for k := range topKeys {
		if _, ok := decoded[k]; !ok {
			t.Fatalf("missing top-level key %q", k)
		}
	}
	if decoded["statusCode"] != float64(400) || decoded["message"] != "Validation failed" {
		t.Fatalf("statusCode/message mismatch: %v", decoded)
	}

	errs, _ := decoded["errors"].([]any)
	if len(errs) != 2 {
		t.Fatalf("errors = %v", errs)
	}
	first, _ := errs[0].(map[string]any)
	issueKeys := map[string]bool{"code": true, "path": true, "message": true}
	for k := range first {
		if !issueKeys[k] {
			t.Fatalf("unexpected issue key %q in %v", k, first)
		}
	}
	for k := range issueKeys {
		if _, ok := first[k]; !ok {
			t.Fatalf("missing issue key %q", k)
		}
	}
	if first["code"] != "too_small" || first["message"] == "" {
		t.Fatalf("issue fields mismatch: %v", first)
	}
	// zod path is an array of segments.
	path, ok := first["path"].([]any)
	if !ok || len(path) != 1 || path[0] != "page" {
		t.Fatalf(`path should render as ["page"], got %#v`, first["path"])
	}
}

func TestWriteZodValidationError_EmptyIssuesStillArray(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.WriteZodValidationError(rec, nil)

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(decoded["errors"]) != "[]" {
		t.Fatalf(`errors should serialise as [], got %s`, decoded["errors"])
	}
}
