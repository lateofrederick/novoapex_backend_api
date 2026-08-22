package httpx_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// Table mirrors the researched zod semantics of PaginationSchema
// (apps/mobile-api/src/common/dto/pagination.dto.ts:4-7):
// preprocess `(val ? Number(val) : default)` then int().min(1)[.max(100)].
func TestParsePageQuery(t *testing.T) {
	cases := []struct {
		name      string
		rawQuery  string
		wantPage  int
		wantLimit int
		wantErr   bool
	}{
		{"unset -> defaults", "", 1, 20, false},
		{"empty strings are falsy in JS -> defaults", "page=&limit=", 1, 20, false},
		{"valid coercion", "page=3&limit=50", 3, 50, false},
		{"limit at max", "page=1&limit=100", 1, 100, false},
		{"page has no max", "page=150&limit=20", 150, 20, false},
		{"page zero below min", "page=0", 0, 0, true},
		{"limit zero below min", "limit=0", 0, 0, true},
		{"negative rejected", "page=-5&limit=-5", 0, 0, true},
		{"non numeric NaN parity", "page=abc", 0, 0, true},
		{"float rejected (int violation)", "page=1.5", 0, 0, true},
		{"limit above max", "page=1&limit=150", 0, 0, true},
		// Documented divergences from Number(): strict parse rejects these
		// even though Node would coerce them to valid integers.
		{"whitespace-padded integer (Node accepts)", "page=%203%20", 0, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/?"+tc.rawQuery, nil)
			got, err := httpx.ParsePageQuery(req)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Page != tc.wantPage || got.Limit != tc.wantLimit {
				t.Fatalf("got {%d %d}, want {%d %d}", got.Page, got.Limit, tc.wantPage, tc.wantLimit)
			}
		})
	}
}

func TestParsePageQuery_ErrorsNameField(t *testing.T) {
	req := httptest.NewRequest("GET", "/?limit=abc", nil)
	_, err := httpx.ParsePageQuery(req)
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("error should name the offending field, got %v", err)
	}
}
