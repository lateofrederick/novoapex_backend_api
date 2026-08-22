// Package handlers implements the Stage 2 vendor-API read surface: chi
// sub-trees with one constructor per Node module controller, mounted
// centrally (see mounting notes in the stage report). Response bodies port
// the Prisma payload shapes key-for-key (camelCase, nulls preserved, money
// as bare JSON numbers via internal/money.Number, timestamps as Prisma/JS
// Date ISO strings).
//
// shared.go holds the cross-module glue only; each module file owns its
// endpoints.
package handlers

import (
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

// isoTime renders a timestamp the way JSON.stringify(new Date(...)) does:
// Date.prototype.toJSON -> toISOString(), e.g. "2026-08-22T09:30:00.000Z".
// Postgres TIMESTAMP(3) columns carry millisecond precision and pgx scans
// them in UTC, so the wire format matches byte-for-byte.
type isoTime struct{ time.Time }

func epISO(t time.Time) isoTime { return isoTime{Time: t.UTC()} }

func epISOPtr(t pgtype.Timestamp) *isoTime {
	if !t.Valid {
		return nil
	}
	v := epISO(t.Time)
	return &v
}

func (i isoTime) MarshalJSON() ([]byte, error) {
	return []byte(i.Time.UTC().Format(`"2006-01-02T15:04:05.000Z07:00"`)), nil
}

// ep_periodISO formats a date_trunc bucket exactly like RevenueService's
// r.period.toISOString() (revenue.service.ts:118).
func ep_periodISO(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// ep_text unwraps a nullable text column to a nilable pointer so nulls stay
// null on the wire (Prisma emits explicit null keys, never omits them).
func ep_text(t pgtype.Text) *string {
	if !t.Valid {
		return nil
	}
	s := t.String
	return &s
}

// ep_numeric converts a nullable NUMERIC column. Nullable numeric columns do
// not pick up the sqlc decimal override, so they arrive as pgtype.Numeric;
// the Int*10^Exp reconstruction is exact (no float involvement).
func ep_numeric(n pgtype.Numeric) *money.Number {
	if !n.Valid || n.Int == nil || n.NaN {
		return nil
	}
	return &money.Number{Decimal: decimal.NewFromBigInt(n.Int, n.Exp)}
}

func ep_num(d decimal.Decimal) money.Number { return money.Number{Decimal: d} }

func ep_float8(f pgtype.Float8) *float64 {
	if !f.Valid {
		return nil
	}
	v := f.Float64
	return &v
}

// ep_businessID ports the @BusinessId() decorator
// (auth/current-user.decorator.ts): business-scoped routes 403 when the JWT
// carries no businessId, so an empty scope can never reach a query.
func ep_businessID(w http.ResponseWriter, r *http.Request) (string, bool) {
	claims, ok := auth.FromContext(r.Context())
	if !ok || claims.BusinessID == "" {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusForbidden,
			"No business is associated with this account. Create a business first."))
		return "", false
	}
	return claims.BusinessID, true
}

// ep_page parses ?page/?limit; failures answer with the nestjs-zod pipe body
// ({"statusCode":400,"message":"Validation failed","errors":[<issue>]}) that
// the mobile API's globally registered validation produces for PaginationDto.
func ep_page(w http.ResponseWriter, r *http.Request) (httpx.PageQuery, bool) {
	pq, err := httpx.ParsePageQuery(r)
	if err != nil {
		httpx.WriteZodValidationError(w, []httpx.FieldIssue{ep_zodPageIssue(err)})
		return pq, false
	}
	return pq, true
}

// ep_zodPageIssue maps httpx.ParsePageQuery's strict-parser errors onto the
// zod issue codes PaginationSchema would emit for the equivalent input
// (NaN -> invalid_type, <min -> too_small, >max -> too_big). Message wording
// follows zod v3 defaults; the documented Number()-coercion divergences of
// ParsePageQuery itself are inherited unchanged.
func ep_zodPageIssue(err error) httpx.FieldIssue {
	msg := err.Error()
	field := "limit"
	if strings.Contains(msg, "page:") {
		field = "page"
	}
	switch {
	case strings.Contains(msg, "expected integer"):
		return httpx.FieldIssue{Code: "invalid_type", Path: field, Message: "Expected number, received nan"}
	case strings.Contains(msg, "above maximum"):
		return httpx.FieldIssue{Code: "too_big", Path: field, Message: "Number must be less than or equal to 100"}
	default:
		return httpx.FieldIssue{Code: "too_small", Path: field, Message: "Number must be greater than or equal to 1"}
	}
}
