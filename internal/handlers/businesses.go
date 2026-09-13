package handlers

// businesses.go ports the POST /businesses surface of
// apps/mobile-api/src/businesses (T4.2a, T4.3): zod-shaped 400s, owner-phone
// binding from the JWT's phone claim, 409 on the per-phone uniqueness rule,
// and the full Prisma row as the response payload.
//
// Mount: chi Mount("/businesses", MountBusinesses(deps)) — Mount strips the
// prefix, so routes below are declared relative to /businesses.

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// BusinessesDeps carries everything POST /businesses needs.
type BusinessesDeps struct {
	Pool *pgxpool.Pool
}

// MountBusinesses attaches POST /businesses on the given router (mirroring
// BusinessesController; one route today).
func MountBusinesses(r chi.Router, deps BusinessesDeps) {
	r.Post("/", s4p_businessCreate(deps.Pool))
}

// businessJSON renders a full Prisma business row key-for-key (camelCase,
// explicit nulls): every column of the generated model — businesses has no
// Unsupported columns.
type businessJSON struct {
	ID                        string   `json:"id"`
	Name                      string   `json:"name"`
	WhatsappPhoneNumberID     string   `json:"whatsappPhoneNumberId"`
	CreatedAt                 isoTime  `json:"createdAt"`
	UpdatedAt                 isoTime  `json:"updatedAt"`
	Currency                  string   `json:"currency"`
	AssistantEnabled          bool     `json:"assistantEnabled"`
	Category                  *string  `json:"category"`
	ConfirmationDelayHours    int32    `json:"confirmationDelayHours"`
	Location                  *string  `json:"location"`
	OwnerPhone                string   `json:"ownerPhone"`
	PaystackRecipientCode     *string  `json:"paystackRecipientCode"`
	PaymentCallbackURL        *string  `json:"paymentCallbackUrl"`
	TemplateLanguage          string   `json:"templateLanguage"`
	NewArrivalsTemplateName   *string  `json:"newArrivalsTemplateName"`
	LastNewArrivalsNotifiedAt *isoTime `json:"lastNewArrivalsNotifiedAt"`
}

func s4p_mapBusiness(b gen.Business) businessJSON {
	return businessJSON{
		ID:                        b.ID,
		Name:                      b.Name,
		WhatsappPhoneNumberID:     b.WhatsappPhoneNumberID,
		CreatedAt:                 epISO(b.CreatedAt.Time),
		UpdatedAt:                 epISO(b.UpdatedAt.Time),
		Currency:                  b.Currency,
		AssistantEnabled:          b.AssistantEnabled,
		Category:                  ep_text(b.Category),
		ConfirmationDelayHours:    b.ConfirmationDelayHours,
		Location:                  ep_text(b.Location),
		OwnerPhone:                b.OwnerPhone,
		PaystackRecipientCode:     ep_text(b.PaystackRecipientCode),
		PaymentCallbackURL:        ep_text(b.PaymentCallbackUrl),
		TemplateLanguage:          b.TemplateLanguage,
		NewArrivalsTemplateName:   ep_text(b.NewArrivalsTemplateName),
		LastNewArrivalsNotifiedAt: epISOPtr(b.LastNewArrivalsNotifiedAt),
	}
}

// s4p_businessCreate ports BusinessesService.create:
// pre-check findUnique({ownerPhone}) -> ConflictException, then create with
// ownerPhone taken from req.user.phone (never from the body). The unique
// index businesses_owner_phone_key is the race backstop and maps to the
// same 409.
func s4p_businessCreate(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.FromContext(r.Context())
		if !ok || claims.Phone == "" {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusUnauthorized, "Unauthorized"))
			return
		}

		m, ok := s4p_rawBody(w, r)
		if !ok {
			return
		}

		// CreateBusinessSchema: name min(2), currency default('GHS'),
		// whatsappPhoneNumberId min(5), category/location optional strings.
		issues := make([]httpx.FieldIssue, 0, 4)
		name := s4p_reqString(&issues, "name", m["name"], 2)
		currency := s4p_optString(&issues, "currency", m["currency"])
		waPhoneID := s4p_reqString(&issues, "whatsappPhoneNumberId", m["whatsappPhoneNumberId"], 5)
		category := s4p_optString(&issues, "category", m["category"])
		location := s4p_optString(&issues, "location", m["location"])
		if len(issues) > 0 {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		_, err := q.ListBusinessesByOwnerPhone(r.Context(), claims.Phone)
		switch {
		case err == nil:
			s4p_conflict(w, r)
			return
		case !errors.Is(err, pgx.ErrNoRows):
			httpx.WriteError(w, r, err)
			return
		}

		currencyVal := "GHS"
		if currency != nil {
			currencyVal = *currency
		}

		created, err := q.CreateBusiness(r.Context(), gen.CreateBusinessParams{
			ID:                    s4p_newUUID(),
			Name:                  *name,
			WhatsappPhoneNumberID: *waPhoneID,
			OwnerPhone:            claims.Phone,
			Currency:              currencyVal,
			Category:              s4p_nillableText(category),
			Location:              s4p_nillableText(location),
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" &&
				pgErr.ConstraintName == "businesses_owner_phone_key" {
				s4p_conflict(w, r)
				return
			}
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusCreated, s4p_mapBusiness(created))
	}
}

func s4p_conflict(w http.ResponseWriter, r *http.Request) {
	slog.Error(fmt.Sprintf("%s %s %d - %q", r.Method, r.URL.RequestURI(),
		http.StatusConflict, "Business already exists for this phone number"))
	httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusConflict,
		"Business already exists for this phone number"))
}
