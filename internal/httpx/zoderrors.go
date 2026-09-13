package httpx

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/novoapex/novoapex-backend-api/internal/errreport"
)

// FieldIssue carries the universal fields of a raw ZodIssue
// (code/path/message). Path is a dot-joined accessor string; it is split back
// into zod's array-of-segments wire format on output.
type FieldIssue struct {
	Code    string
	Path    string
	Message string
}

// zod_issue renders one entry of the errors array with zod's key set.
type zod_issue struct {
	Code    string   `json:"code"`
	Path    []string `json:"path"`
	Message string   `json:"message"`
}

// WriteZodValidationError emits the 400 body thrown by the globally
// registered ZodValidationPipe. SOURCE note: apps/mobile-api registers
// nestjs-zod's pipe (mobile-api.module.ts:5,50-53), whose exception body is
// exactly {statusCode:400, message:"Validation failed", errors:<zod issues>}
// (node_modules/nestjs-zod/dist/index.mjs ZodValidationException), NOT the
// flattened shape produced by libs/common's local pipe (which mobile-api
// never wires up).
func WriteZodValidationError(w http.ResponseWriter, issues []FieldIssue) {
	errs := make([]zod_issue, 0, len(issues))
	for _, i := range issues {
		errs = append(errs, zod_issue{
			Code:    i.Code,
			Path:    zod_pathSegments(i.Path),
			Message: i.Message,
		})
	}

	errreport.Report(w, &errreport.Exception{Name: "ZodValidationException", Status: http.StatusBadRequest, Message: "Validation failed"})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)

	body := struct {
		StatusCode int         `json:"statusCode"`
		Message    string      `json:"message"`
		Errors     []zod_issue `json:"errors"`
	}{
		StatusCode: http.StatusBadRequest,
		Message:    "Validation failed",
		Errors:     errs,
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// zod_pathSegments converts a dot-joined FieldIssue.Path into zod's
// segments array ("page" -> ["page"], "a.b" -> ["a","b"], "" -> []).
func zod_pathSegments(p string) []string {
	if p == "" {
		return []string{}
	}
	return strings.Split(p, ".")
}
