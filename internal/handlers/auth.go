package handlers

// Auth ports apps/mobile-api/src/auth (auth.controller.ts + auth.service.ts
// + otp.service.ts): POST /auth/request-otp, POST /auth/verify-otp (public,
// OTP-throttled) and GET /auth/me (JWT-guarded via the centrally welded
// auth.Middleware).
//
// Mount: chi r.Route("/auth", func(r chi.Router) { handlers.MountAuth(r,
// deps) }) inside the tree wrapped by auth.Middleware.
//
// Error shapes follow the researched Nest surface of apps/api (the harness
// boots dist/apps/api whose main.ts registers AllExceptionsFilter globally):
// httpx.WriteError emits {"statusCode","timestamp","path","message"} with
// message = exception.getResponse() verbatim — string payloads stay strings
// (ThrottlerException, generic delivery errors -> "Internal server error")
// and UnauthorizedException payloads keep createBody's key order
// {message,error,statusCode}. Validation failures use the pinned Stage 2
// nestjs-zod pipe body via httpx.WriteZodValidationError.
import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

// Throttle contracts (auth.controller.ts:16,25 + mobile-api.module.ts:29-34):
// both public OTP routes override the global default to 5/min; every route
// still sits under the app-wide 120/min guard. Names mirror Nest's generateKey
// prefix `${class}-${handler}-${name}` so counters are shaped identically
// (per route x per client IP).
const (
	authOTPLimit    = 5
	authGlobalLimit = 120
	authWindow      = time.Minute

	authThrottleRequestOTP = "AuthController-requestOtp-default"
	authThrottleVerifyOTP  = "AuthController-verifyOtp-default"
	authThrottleMe         = "AuthController-getMe-default"

	// authWhatsAppTemplate is the EXACT WhatsApp body from
	// auth.service.ts:37 (asterisks intact — WhatsApp emphasis markers).
	authWhatsAppTemplate = "Your NovoApex login code is: *%s*. It will expire in 5 minutes."

	authMsgOTPSent = "OTP sent successfully"
)

// Mailer sends the OTP email (libs/common EmailService.sendOtpEmail).
type Mailer interface {
	SendOtpEmail(to, code string) error
}

// WhatsAppSender ports WhatsAppService.sendTextMessage.
type WhatsAppSender interface {
	SendTextMessage(phoneNumberID, to, text string) error
}

// AuthDeps carries everything MountAuth needs.
type AuthDeps struct {
	Redis  auth.Redis // OTP store (go-redis adapter or test fake)
	Pool   *pgxpool.Pool
	Secret string // JWT_SECRET

	Mailer     Mailer
	WhatsApp   WhatsAppSender
	WhatsAppID string // WHATSAPP_PHONE_NUMBER_ID (fallback channel)
}

// MountAuth registers the /auth subtree routes on r.
func MountAuth(r chi.Router, deps AuthDeps) {
	otp := httpx.NewThrottler(authThrottleRequestOTP, authOTPLimit, authWindow)
	verify := httpx.NewThrottler(authThrottleVerifyOTP, authOTPLimit, authWindow)
	me := httpx.NewThrottler(authThrottleMe, authGlobalLimit, authWindow)

	q := gen.New(deps.Pool)
	svc := &authService{redis: deps.Redis, q: q, secret: deps.Secret}

	r.With(otp.Middleware).Post("/request-otp", authRequestOTP(deps, svc))
	r.With(verify.Middleware).Post("/verify-otp", authVerifyOTP(svc))
	r.With(me.Middleware).Get("/me", authMe(q))
}

// authService bundles what the POST handlers share.
type authService struct {
	redis  auth.Redis
	q      *gen.Queries
	secret string
}

// ---- POST /auth/request-otp ------------------------------------------------

func authRequestOTP(deps AuthDeps, svc *authService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, ok := auth_decodeJSONBody(w, r)
		if !ok {
			return
		}

		dto, issues := validateRequestOtp(raw)
		if len(issues) > 0 {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		// Flow order mirrors auth.service.ts:24-45: generate (which stores)
		// FIRST, deliver second — a failed delivery leaves the code in redis.
		code, err := auth.Generate(r.Context(), svc.redis, dto.Phone)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		if dto.DeliveryMethod == "email" && dto.Email != "" {
			err = deps.Mailer.SendOtpEmail(dto.Email, code)
		} else {
			// Service-level fallback (auth.service.ts:32-40): anything but a
			// well-formed email delivery goes out over WhatsApp.
			err = deps.WhatsApp.SendTextMessage(deps.WhatsAppID, dto.Phone,
				fmt.Sprintf(authWhatsAppTemplate, code))
		}
		if err != nil {
			// auth.service.ts:41-44 throws a plain Error — AllExceptionsFilter
			// renders any non-HttpException as a bare 500 Internal server error.
			slog.Error(fmt.Sprintf("Failed to send OTP to %s via %s", dto.Phone, dto.DeliveryMethod))
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, map[string]string{"message": authMsgOTPSent})
	}
}

// ---- POST /auth/verify-otp -------------------------------------------------

func authVerifyOTP(svc *authService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, ok := auth_decodeJSONBody(w, r)
		if !ok {
			return
		}

		dto, issues := validateVerifyOtp(raw)
		if len(issues) > 0 {
			httpx.WriteZodValidationError(w, issues)
			return
		}

		valid, err := auth.Verify(r.Context(), svc.redis, dto.Phone, dto.Code)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		if !valid {
			// auth.service.ts:49-51 throws UnauthorizedException with a string
			// message; HttpException.createBody renders that as the object
			// below (key order message,error,statusCode) which
			// AllExceptionsFilter embeds under `message`.
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusUnauthorized,
				auth_unauthorizedBody("Invalid or expired OTP")))
			return
		}

		// auth.service.ts:53-72: business lookup by owner phone decides
		// isNewUser and the token's businessId claim (undefined -> omitted by
		// jsonwebtoken, mirrored by auth.Mint's empty-string omission).
		business, err := svc.q.ListBusinessesByOwnerPhone(r.Context(), dto.Phone)
		isNewUser := false
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			isNewUser = true
		case err != nil:
			httpx.WriteError(w, r, err)
			return
		}

		claims := auth.Claims{Phone: dto.Phone}
		var payload *authBusinessJSON
		if !isNewUser {
			claims.BusinessID = business.ID
			payload = &authBusinessJSON{}
			auth_fillBusiness(payload, business)
		}

		token, err := auth.Mint(svc.secret, claims)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, verifyResponse{
			Token:     token,
			IsNewUser: isNewUser,
			Business:  payload,
		})
	}
}

// verifyResponse keeps auth.service.ts:68-72's key order.
type verifyResponse struct {
	Token     string            `json:"token"`
	IsNewUser bool              `json:"isNewUser"`
	Business  *authBusinessJSON `json:"business"`
}

// authBusinessJSON is the full Prisma Business row (findUnique by ownerPhone),
// camelCase keys in schema.prisma field order; no Decimal fields exist on the
// model so DecimalSerializerInterceptor never touches these responses.
type authBusinessJSON struct {
	ID                     string  `json:"id"`
	Name                   string  `json:"name"`
	OwnerPhone             string  `json:"ownerPhone"`
	WhatsappPhoneNumberID  string  `json:"whatsappPhoneNumberId"`
	CreatedAt              isoTime `json:"createdAt"`
	UpdatedAt              isoTime `json:"updatedAt"`
	Currency               string  `json:"currency"`
	Category               *string `json:"category"`
	Location               *string `json:"location"`
	AssistantEnabled       bool    `json:"assistantEnabled"`
	ConfirmationDelayHours int32   `json:"confirmationDelayHours"`
	PaystackRecipientCode  *string `json:"paystackRecipientCode"`
	PaymentCallbackUrl     *string `json:"paymentCallbackUrl"`
	TemplateLanguage       string  `json:"templateLanguage"`
}

func auth_fillBusiness(dst *authBusinessJSON, b gen.Business) {
	dst.ID = b.ID
	dst.Name = b.Name
	dst.OwnerPhone = b.OwnerPhone
	dst.WhatsappPhoneNumberID = b.WhatsappPhoneNumberID
	dst.CreatedAt = epISO(b.CreatedAt.Time)
	dst.UpdatedAt = epISO(b.UpdatedAt.Time)
	dst.Currency = b.Currency
	dst.Category = ep_text(b.Category)
	dst.Location = ep_text(b.Location)
	dst.AssistantEnabled = b.AssistantEnabled
	dst.ConfirmationDelayHours = b.ConfirmationDelayHours
	dst.PaystackRecipientCode = ep_text(b.PaystackRecipientCode)
	dst.PaymentCallbackUrl = ep_text(b.PaymentCallbackUrl)
	dst.TemplateLanguage = b.TemplateLanguage
}

// ---- GET /auth/me ----------------------------------------------------------

func authMe(q *gen.Queries) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.FromContext(r.Context())
		if !ok {
			// Defensive: central welding places auth.Middleware ahead of this
			// router, so absence means mis-mounting. Answer with the same
			// pinned guard body auth.Middleware writes.
			_ = httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{
				"statusCode": http.StatusUnauthorized,
				"message":    "Invalid or missing session token",
			})
			return
		}

		business, err := q.ListBusinessesByOwnerPhone(r.Context(), claims.Phone)
		if errors.Is(err, pgx.ErrNoRows) {
			// auth.service.ts:80-83 UnauthorizedException('Business profile
			// not found').
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusUnauthorized,
				auth_unauthorizedBody("Business profile not found")))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		out := authBusinessJSON{}
		auth_fillBusiness(&out, business)
		_ = httpx.WriteJSON(w, http.StatusOK, out)
	}
}

// ---- shared helpers --------------------------------------------------------

// auth_decodeJSONBody parses the request body as generic JSON.
//
// Undecodable bodies: express's body-parser SyntaxError reaches the api app's
// AllExceptionsFilter and renders as a 400 envelope whose message is
// createBody(parserMessage, 'Bad Request', 400) — characterised live as
// {"statusCode":400,…,"message":{"message":"<parser detail>","error":"Bad
// Request","statusCode":400},"content-type":"application/json"}. Delta: the
// embedded parser detail text is encoding/json's wording, not V8's.
func auth_decodeJSONBody(w http.ResponseWriter, r *http.Request) (any, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest,
			auth_badRequestBody("Invalid request body")))
		return nil, false
	}

	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusBadRequest,
			auth_badRequestBody("Invalid JSON body")))
		return nil, false
	}
	return raw, true
}

// authUnauthorizedBody / authBadRequestBody reproduce HttpException.createBody
// for string arguments; field order (message, error, statusCode) matters
// because the filter serialises the object verbatim.
type authUnauthorizedBody struct {
	Message    string `json:"message"`
	Error      string `json:"error"`
	StatusCode int    `json:"statusCode"`
}

func auth_unauthorizedBody(message string) authUnauthorizedBody {
	return authUnauthorizedBody{
		Message:    message,
		Error:      "Unauthorized",
		StatusCode: http.StatusUnauthorized,
	}
}

func auth_badRequestBody(message string) authBadRequestObject {
	return authBadRequestObject{
		Message:    message,
		Error:      "Bad Request",
		StatusCode: http.StatusBadRequest,
	}
}

type authBadRequestObject struct {
	Message    string `json:"message"`
	Error      string `json:"error"`
	StatusCode int    `json:"statusCode"`
}

// ---- zod-equivalent request validation -------------------------------------
//
// Hand-rolled validators reproducing the exact zod v4.3.6 issue arrays that
// the globally registered nestjs-zod ZodValidationPipe produces for
// RequestOtpSchema / VerifyOtpSchema (apps/mobile-api/src/auth/dto/). Issue
// shapes and ordering were pinned by executing the real schemas:
//
//	missing phone      -> [{"expected":"string","code":"invalid_type","path":["phone"],"message":"Invalid input: expected string, received undefined"}]
//	short phone        -> [{"origin":"string","code":"too_small","minimum":10,"inclusive":true,"path":["phone"],"message":"Too small: expected string to have >=10 characters"}]
//	bad email          -> [{"origin":"string","code":"invalid_format","format":"email","pattern":"/.../","path":["email"],"message":"Invalid email address"}]
//	bad deliveryMethod -> [{"code":"invalid_value","values":["email","whatsapp"],"path":["deliveryMethod"],"message":"Invalid option: expected one of \"email\"|\"whatsapp\""}]
//	refine violation   -> [{"code":"custom","path":["email"],"message":"Email is required when deliveryMethod is email"}]
//
// Fields are checked in schema-definition order (phone, email,
// deliveryMethod) and the .refine predicate runs LAST on the best-effort
// parsed object — it fires even when other fields failed, using the parsed
// value ('' stays falsy, matching the live empty-email probe).
//
// Delta: httpx.FieldIssue carries only code/path/message, so the extra
// zod-v4 issue keys (expected/received/origin/minimum/inclusive/exact/
// values/format/pattern) are not rendered; the Stage 2 pinned
// WriteZodValidationError body is emitted instead.

const authEmailPattern = `/^(?!\.)(?!.*\.\.)([A-Za-z0-9_'+\-\.]*)[A-Za-z0-9_+-]@([A-Za-z0-9][A-Za-z0-9\-]*\.)+[A-Za-z]{2,}$/`

const authMsgEmailRequired = "Email is required when deliveryMethod is email"

type requestOtpDTO struct {
	Phone          string
	Email          string
	DeliveryMethod string // defaulted to "email"
}

type verifyOtpDTO struct {
	Phone string
	Code  string
}

func validateRequestOtp(raw any) (requestOtpDTO, []httpx.FieldIssue) {
	var dto requestOtpDTO
	obj, objIssues := auth_objectIssues(raw)
	if len(objIssues) > 0 {
		return dto, objIssues
	}

	var issues []httpx.FieldIssue
	hardFailure := false // invalid_type/invalid_value suppress .refine in zod v4

	p, phoneErr := auth_checkPhone(auth_field(obj, "phone"))
	if phoneErr != nil {
		issues = append(issues, *phoneErr)
		if phoneErr.Code == "invalid_type" {
			hardFailure = true
		}
	}
	dto.Phone = p

	emailStr, emailSet := "", false
	if e, present := (*obj)["email"]; present && e != nil {
		emailSet = true
		s, err := auth_stringField(e, "email")
		switch {
		case err != nil:
			issues = append(issues, *err)
			hardFailure = true
		case !auth_isEmail(s):
			// zod v4 keeps the raw candidate as the parsed value when only
			// format checks fail, so the refine sees the truthy original.
			issues = append(issues, httpx.FieldIssue{
				Code:    "invalid_format",
				Path:    "email",
				Message: "Invalid email address",
			})
			emailStr = s
		default:
			emailStr = s
		}
	}
	dto.Email = emailStr

	method := "email" // z.enum(['email','whatsapp']).default('email')
	if m, present := (*obj)["deliveryMethod"]; present && m != nil {
		s, ok := m.(string)
		if !ok || (s != "email" && s != "whatsapp") {
			issues = append(issues, httpx.FieldIssue{
				Code:    "invalid_value",
				Path:    "deliveryMethod",
				Message: `Invalid option: expected one of "email"|"whatsapp"`,
			})
			method = "" // refine sees undefined -> not 'email' -> passes
			hardFailure = true
		} else {
			method = s
		}
	}
	dto.DeliveryMethod = method

	// .refine(data => !(data.deliveryMethod === 'email' && !data.email), ...)
	// Pinned behaviour: soft failures (too_small/too_big/invalid_format) leave
	// the best-effort parse in place and the refine still runs; hard failures
	// (invalid_type / invalid_value anywhere) suppress it entirely.
	if !hardFailure && method == "email" && (!emailSet || emailStr == "") {
		issues = append(issues, httpx.FieldIssue{
			Code:    "custom",
			Path:    "email",
			Message: authMsgEmailRequired,
		})
	}

	return dto, issues
}

func validateVerifyOtp(raw any) (verifyOtpDTO, []httpx.FieldIssue) {
	var dto verifyOtpDTO
	obj, objIssues := auth_objectIssues(raw)
	if len(objIssues) > 0 {
		return dto, objIssues
	}

	var issues []httpx.FieldIssue

	p, phoneErr := auth_checkPhone(auth_field(obj, "phone"))
	if phoneErr != nil {
		issues = append(issues, *phoneErr)
	}
	dto.Phone = p

	c, codeErr := auth_stringField(auth_field(obj, "code"), "code")
	switch {
	case codeErr != nil:
		issues = append(issues, *codeErr)
	case len(c) < 6:
		issues = append(issues, httpx.FieldIssue{
			Code:    "too_small",
			Path:    "code",
			Message: "Too small: expected string to have >=6 characters",
		})
	case len(c) > 6:
		issues = append(issues, httpx.FieldIssue{
			Code:    "too_big",
			Path:    "code",
			Message: "Too big: expected string to have <=6 characters",
		})
	default:
		dto.Code = c
	}

	return dto, issues
}

// auth_objectIssues renders z.object's top-level invalid_type issue for
// non-object bodies (empty path).
func auth_objectIssues(raw any) (*map[string]any, []httpx.FieldIssue) {
	if m, ok := raw.(map[string]any); ok {
		return &m, nil
	}
	return nil, []httpx.FieldIssue{{
		Code:    "invalid_type",
		Path:    "",
		Message: fmt.Sprintf("Invalid input: expected object, received %s", auth_jsonTypeName(raw)),
	}}
}

// auth_field distinguishes an absent key (zod "undefined") from an explicit
// JSON null ("null"): absent keys read as authMissingValue.
type authMissingType struct{}

var authMissingValue = authMissingType{}

func auth_field(obj *map[string]any, key string) any {
	if v, present := (*obj)[key]; present {
		return v
	}
	return authMissingValue
}

func auth_checkPhone(v any) (string, *httpx.FieldIssue) {
	s, err := auth_stringField(v, "phone")
	if err != nil {
		return "", err
	}
	if len(s) < 10 {
		issue := httpx.FieldIssue{
			Code:    "too_small",
			Path:    "phone",
			Message: "Too small: expected string to have >=10 characters",
		}
		return s, &issue
	}
	return s, nil
}

// auth_stringField renders z.string()'s invalid_type issue for non-strings.
func auth_stringField(v any, field string) (string, *httpx.FieldIssue) {
	s, ok := v.(string)
	if !ok {
		issue := httpx.FieldIssue{
			Code:    "invalid_type",
			Path:    field,
			Message: fmt.Sprintf("Invalid input: expected string, received %s", auth_jsonTypeName(v)),
		}
		return "", &issue
	}
	return s, nil
}

// auth_jsonTypeName maps a decoded JSON value onto zod's `received` names.
func auth_jsonTypeName(v any) string {
	switch v.(type) {
	case authMissingType:
		return "undefined"
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "number" // encoding/json decodes numbers as float64/json.Number
	}
}

// auth_isEmail mirrors the accepted shape of zod v4's email regex without
// RE2 lookaheads: local part may not start with '.', contain '..', or end
// with '.'; domain is dotted labels whose TLD is alphabetic with length >= 2.
// authEmailPattern documents the original source pattern.
func auth_isEmail(s string) bool {
	at := strings.LastIndexByte(s, '@')
	if at <= 0 || at == len(s)-1 {
		return false
	}
	local, domain := s[:at], s[at+1:]

	if local == "" ||
		strings.HasPrefix(local, ".") ||
		strings.Contains(local, "..") ||
		strings.HasSuffix(local, ".") ||
		strings.ContainsAny(local, "@ ") {
		return false
	}
	for i := 0; i < len(local); i++ {
		if !auth_emailLocalRune(local[i]) {
			return false
		}
	}

	labels := strings.Split(domain, ".")
	if len(labels) < 2 {
		return false
	}
	for _, label := range labels {
		if label == "" || !isASCIIAlphaNumeric(label[0]) || strings.Contains(label, "..") {
			return false
		}
		for i := 0; i < len(label); i++ {
			ch := label[i]
			if !isASCIIAlphaNumeric(ch) && ch != '-' && ch != '_' {
				return false
			}
		}
	}
	tld := labels[len(labels)-1]
	if len(tld) < 2 {
		return false
	}
	for i := 0; i < len(tld); i++ {
		b := tld[i]
		isAlpha := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
		if !isAlpha {
			return false
		}
	}
	return true
}

func auth_emailLocalRune(b byte) bool {
	return isASCIIAlphaNumeric(b) || b == '_' || b == '\'' || b == '+' || b == '-' || b == '.'
}

func isASCIIAlphaNumeric(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}
