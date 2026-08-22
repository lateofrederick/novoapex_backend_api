package httpx

import (
	"fmt"
	"net/http"
	"strconv"
)

// Defaults and bounds port apps/mobile-api/src/common/dto/pagination.dto.ts:4-7
// (z.preprocess falsy->default, z.number().int().min(1) [limit .max(100)]).
const (
	pag_defaultPage  = 1
	pag_defaultLimit = 20

	pag_minPage  = 1 // page has no upper bound in the Node schema
	pag_minLimit = 1
	pag_maxLimit = 100
)

// PageQuery is the parsed PaginationDto.
type PageQuery struct {
	Page  int
	Limit int
}

// Meta ports PaginatedResponse.meta from
// apps/mobile-api/src/common/interfaces/pagination.interface.ts:3-8.
// TotalPages is computed by callers as ceil(total/limit), matching e.g.
// apps/mobile-api/src/products/products.service.ts:161.
type Meta struct {
	Total      int `json:"total"`
	Page       int `json:"page"`
	Limit      int `json:"limit"`
	TotalPages int `json:"totalPages"`
}

// ParsePageQuery parses ?page / ?limit with the exact researched Node
// semantics:
//
//   - absent or empty value -> default: the pagination.dto.ts preprocess
//     guard short-circuits falsy values before Number, so the Number of an
//     empty string (0) quirk is unreachable on the wire;
//   - non-integer, <1 (page and limit) or >100 (limit only) -> error;
//   - page has no maximum.
//
// Divergence vs Number(): Node also coerces ' 12 ', '0x10', '1e2' and '12.0'
// to integers; this strict strconv parse rejects all four. Whitespace and
// floats are likewise rejected by zod's .int()/number() checks for '12.0'
// (' 12 ' would pass in Node); hex/exponent forms are a documented,
// deliberate divergence — clients must send canonical decimal integers.
func ParsePageQuery(r *http.Request) (PageQuery, error) {
	q := r.URL.Query()

	page, err := pag_queryInt("page", q.Get("page"), pag_defaultPage, pag_minPage, 0)
	if err != nil {
		return PageQuery{}, err
	}

	limit, err := pag_queryInt("limit", q.Get("limit"), pag_defaultLimit, pag_minLimit, pag_maxLimit)
	if err != nil {
		return PageQuery{}, err
	}

	return PageQuery{Page: page, Limit: limit}, nil
}

func pag_queryInt(name, raw string, def, min, max int) (int, error) {
	if raw == "" { // absent or empty-string values are falsy in JS
		return def, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("httpx: %s: expected integer, got %q", name, raw)
	}
	if n < min {
		return 0, fmt.Errorf("httpx: %s: %d below minimum %d", name, n, min)
	}
	if max > 0 && n > max {
		return 0, fmt.Errorf("httpx: %s: %d above maximum %d", name, n, max)
	}
	return n, nil
}
