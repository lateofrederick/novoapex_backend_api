package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/novoapex/novoapex-backend-api/internal/apidocs"
	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/handlers"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/smtp"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/storage"
)

// globalThrottleLimit mirrors ThrottlerModule.forRoot([{ttl: 60_000, limit: 120}]).
const globalThrottleLimit = 120

// routerDeps carries everything the HTTP surface is built from.
type routerDeps struct {
	cfg        *config.Config
	pool       *pgxpool.Pool
	rdb        *redis.Client
	publisher  queue.Publisher
	queueRedis asynq.RedisConnOpt
	storage    storage.StorageProvider
	wa         *whatsapp.Client
	paystack   *paystack.Client

	// mobileOnly serves just the mobile API modules, like the standalone
	// apps/mobile-api entrypoint: no webhooks, manual messages, API docs or
	// queue dashboard.
	mobileOnly bool

	// middleware wraps every request (Sentry instrumentation when enabled).
	middleware []func(http.Handler) http.Handler
}

// buildRouter assembles the whole HTTP surface (apps/api AppModule, which
// imports MobileApiModule). The returned closer releases dashboard resources.
func buildRouter(d routerDeps) (*httpx.Kernel, func() error, error) {
	cfg := d.cfg
	closer := func() error { return nil }

	kernel := httpx.New(d.middleware...)
	kernel.MountHealth(httpx.HealthDeps{
		DB:    httpx.NewPGPinger(d.pool),
		Redis: httpx.NewRedisPinger(d.rdb),
	})
	kernel.Router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound,
			fmt.Sprintf("Cannot %s %s", r.Method, r.URL.Path)))
	})

	// The global JwtAuthGuard applies app-wide; only @Public routes (webhooks,
	// health, OTP issuance) opt out. Every guarded route re-derives the
	// principal's businessId from the database (JwtStrategy.validate).
	requireAuth := auth.Middleware(cfg.JWTSecret, nil)
	resolveBusiness := auth.ResolveBusiness(handlers.BusinessIDByOwnerPhone(d.pool), httpx.WriteError)
	globalThrottle := httpx.NewThrottler("global", globalThrottleLimit, time.Minute).PerRouteMiddleware

	if !d.mobileOnly {
		// Public ingest: provider webhooks carry their own signatures and skip
		// throttling (@SkipThrottle) — Meta and Paystack burst from few IPs.
		handlers.MountWebhooks(kernel.Router, handlers.WebhookDeps{
			Pool:           d.pool,
			Publisher:      d.publisher,
			AppSecret:      cfg.WhatsAppAppSecret,
			VerifyToken:    cfg.WhatsAppVerifyToken,
			PaystackSecret: cfg.PaystackSecretKey,
		})

		// QueueAdminModule: dashboard behind fail-closed basic auth.
		closer = httpx.MountQueueDashboard(kernel.Router, httpx.QueueDashboardConfig{
			Redis:    d.queueRedis,
			User:     cfg.BullBoardUser,
			Password: cfg.BullBoardPassword,
		})

		if apidocs.Enabled(cfg.NodeEnv, cfg.EnableSwagger) {
			if err := apidocs.Mount(kernel.Router, apidocs.Options{PaymentProviders: handlers.PaymentProviderNames()}); err != nil {
				return nil, closer, fmt.Errorf("mount api docs: %w", err)
			}
		}
	}

	kernel.Router.Route("/auth", func(a chi.Router) {
		a.Use(auth.Middleware(cfg.JWTSecret, func(r *http.Request) bool {
			if r.Method != http.MethodPost {
				return false
			}
			return r.URL.Path == "/auth/request-otp" || r.URL.Path == "/auth/verify-otp"
		}))
		handlers.MountAuth(a, handlers.AuthDeps{
			Redis:      auth.NewGoRedisAdapter(d.rdb),
			Pool:       d.pool,
			Secret:     cfg.JWTSecret,
			Mailer:     otpMailer{cfg: smtp.Config{Host: cfg.SMTPHost, Port: cfg.SMTPPort, User: cfg.SMTPUser, Pass: cfg.SMTPPass}},
			WhatsApp:   waSender{c: d.wa},
			WhatsAppID: cfg.WhatsAppPhoneNumberID,
		})
	})

	kernel.Router.Group(func(v chi.Router) {
		v.Use(globalThrottle, requireAuth, resolveBusiness)

		if !d.mobileOnly {
			v.Route("/messages", func(m chi.Router) {
				handlers.MountMessages(m, handlers.MessagesDeps{
					WA:            d.wa,
					PhoneNumberID: cfg.WhatsAppPhoneNumberID,
					Pool:          d.pool,
				})
			})
		}

		v.Method(http.MethodGet, "/", handlers.NewRoot())
		v.Mount("/customers", handlers.NewCustomers(d.pool))
		v.Route("/products", func(pr chi.Router) {
			pr.Mount("/", handlers.NewProducts(d.pool))
			handlers.MountProductsWrite(pr, handlers.ProductsDeps{
				Pool:    d.pool,
				Storage: d.storage,
				Embeds:  handlers.QueueEmbedPublisher{Pub: d.publisher},
			})
		})
		v.Route("/orders", func(or chi.Router) {
			or.Mount("/", handlers.NewOrders(d.pool))
			handlers.MountOrdersWrite(or, handlers.OrdersWriteDeps{Pool: d.pool})
		})
		v.Mount("/inbox", handlers.NewInbox(d.pool))
		v.Route("/conversations", func(cr chi.Router) {
			cr.Mount("/", handlers.NewConversations(d.pool))
			handlers.MountConversationsWrite(cr, handlers.ConversationsWriteDeps{
				Pool: d.pool,
				WA:   d.wa,
			})
		})
		v.Mount("/dashboard", handlers.NewDashboard(d.pool))
		v.Mount("/analytics", handlers.NewAnalytics(d.pool))
		v.Route("/payouts", func(pr chi.Router) {
			pr.Mount("/", handlers.NewPayouts(d.pool))
			handlers.MountPayoutsWrite(pr, handlers.PayoutsWriteDeps{Pool: d.pool, Paystack: d.paystack})
		})
		v.Route("/businesses", func(b chi.Router) { handlers.MountBusinesses(b, handlers.BusinessesDeps{Pool: d.pool}) })
		v.Route("/locations", func(l chi.Router) {
			l.Mount("/", handlers.NewLocations(d.pool))
			handlers.MountLocationsWrite(l, handlers.LocationsWriteDeps{Pool: d.pool})
		})
	})

	return kernel, closer, nil
}

type otpMailer struct{ cfg smtp.Config }

func (m otpMailer) SendOtpEmail(to, code string) error {
	return smtp.SendOTPEmail(m.cfg, to, code)
}

type waSender struct{ c *whatsapp.Client }

func (w waSender) SendTextMessage(phoneNumberID, to, text string) error {
	return w.c.SendTextMessage(context.Background(), phoneNumberID, to, text)
}
