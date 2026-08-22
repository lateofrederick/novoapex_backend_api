package httpx_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	err := httpx.WriteJSON(rec, 201, map[string]string{"ok": "yes"})
	if err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	if rec.Code != 201 {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	var got map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got["ok"] != "yes" {
		t.Fatalf("body = %q (%v)", rec.Body.String(), err)
	}
}

func TestWritePaginated_EmptyDataIsArray(t *testing.T) {
	for name, data := range map[string][]string{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			if err := httpx.WritePaginated(rec, 200, data, 0, 1, 20); err != nil {
				t.Fatalf("WritePaginated: %v", err)
			}
			body := rec.Body.String()
			if !strings.Contains(body, `"data":[]`) {
				t.Fatalf(`want literal [], got %s`, body)
			}

			var decoded struct {
				Data []string        `json:"data"`
				Meta httpx.Meta      `json:"meta"`
				Raw  json.RawMessage `json:"-"`
			}
			if err := json.Unmarshal([]byte(body), &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if decoded.Data == nil || len(decoded.Data) != 0 {
				t.Fatalf("data must decode as empty array, got %#v", decoded.Data)
			}
			wantMeta := httpx.Meta{Total: 0, Page: 1, Limit: 20, TotalPages: 0}
			if decoded.Meta != wantMeta {
				t.Fatalf("meta = %+v, want %+v", decoded.Meta, wantMeta)
			}
		})
	}
}

// Math.ceil parity (products.service.ts:161 et al).
func TestWritePaginated_MetaMath(t *testing.T) {
	cases := []struct {
		total, limit, wantPages int
	}{
		{0, 20, 0},
		{1, 20, 1},
		{20, 20, 1},
		{21, 20, 2},
		{40, 20, 2},
		{41, 20, 3},
		{100, 100, 1},
		{101, 100, 2},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		if err := httpx.WritePaginated(rec, 200, []int{1}, tc.total, 1, tc.limit); err != nil {
			t.Fatalf("WritePaginated: %v", err)
		}
		var got struct {
			Meta httpx.Meta `json:"meta"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Meta.TotalPages != tc.wantPages || got.Meta.Total != tc.total || got.Meta.Limit != tc.limit {
			t.Fatalf("total=%d limit=%d -> meta %+v, want totalPages %d",
				tc.total, tc.limit, got.Meta, tc.wantPages)
		}
	}
}

// DecimalSerializerInterceptor parity: money fields must be bare JSON
// numbers, never strings ("45" would change the API contract).
func TestWritePaginated_MoneyNotStringified(t *testing.T) {
	type row struct {
		ID    string          `json:"id"`
		Price money.Number    `json:"price"`
		Raw   json.RawMessage `json:"-"`
	}

	d, err := money.FromString("45.00")
	if err != nil {
		t.Fatalf("FromString: %v", err)
	}

	rec := httptest.NewRecorder()
	if err := httpx.WritePaginated(rec, 200,
		[]row{{ID: "p1", Price: money.Number{Decimal: d}}}, 1, 1, 20); err != nil {
		t.Fatalf("WritePaginated: %v", err)
	}

	body := rec.Body.String()
	if strings.Contains(body, `"price":"45`) {
		t.Fatalf("money stringified: %s", body)
	}

	var decoded struct {
		Data []struct {
			Price json.Number `json:"price"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Data) != 1 {
		t.Fatalf("rows = %v", decoded.Data)
	}
	if _, err := decoded.Data[0].Price.Float64(); err != nil || decoded.Data[0].Price == "" {
		t.Fatalf("price not a bare JSON number: %#v (%s)", decoded.Data[0].Price, body)
	}
}
