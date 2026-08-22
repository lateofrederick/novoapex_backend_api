package httpx

import (
	"encoding/json"
	"net/http"
)

// WriteJSON serialises v the way Express res.json does (Nest handlers return
// plain objects; JS JSON.stringify never HTML-escapes, hence SetEscapeHTML
// false) and sets application/json.
//
// Money: response structs embed internal/money.Number, whose MarshalJSON
// emits an unquoted number — the Go equivalent of DecimalSerializerInterceptor
// converting Prisma.Decimal back to a bare JSON number
// (apps/mobile-api/src/common/interceptors/decimal-serializer.interceptor.ts,
// registered globally in apps/mobile-api/src/mobile-api.module.ts:55-58).
// Nothing here stringifies decimals.
func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// res_paginatedEnvelope ports PaginatedResponse<T>
// (apps/mobile-api/src/common/interfaces/pagination.interface.ts:1-9):
// {"data":[...],"meta":{"total","page","limit","totalPages"}}.
type res_paginatedEnvelope[T any] struct {
	Data []T  `json:"data"`
	Meta Meta `json:"meta"`
}

// WritePaginated writes the PaginatedResponse envelope. A nil data slice is
// normalised to [] — an empty Prisma findMany returns a JS array which
// JSON.stringify renders as [] (never null), and every list handler in the
// mobile API passes findMany results straight through (e.g.
// products.service.ts findAll).
func WritePaginated[T any](w http.ResponseWriter, status int, data []T, total, page, limit int) error {
	if data == nil {
		data = []T{}
	}

	return WriteJSON(w, status, res_paginatedEnvelope[T]{
		Data: data,
		Meta: Meta{
			Total:      total,
			Page:       page,
			Limit:      limit,
			TotalPages: pag_totalPages(total, limit),
		},
	})
}

// pag_totalPages mirrors Math.ceil(total / limit)
// (products.service.ts:161, orders.service.ts:32, customers.service.ts:30,
// conversations.service.ts:48, payouts.service.ts:58). limit is expected
// validated >=1; a non-positive limit yields 0 rather than a panic.
func pag_totalPages(total, limit int) int {
	if limit <= 0 || total <= 0 {
		return 0
	}
	return (total + limit - 1) / limit
}
