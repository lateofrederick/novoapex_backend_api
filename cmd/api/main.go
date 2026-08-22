package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/db"
	"github.com/novoapex/novoapex-backend-api/internal/handlers"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/smtp"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
	"github.com/novoapex/novoapex-backend-api/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL, db.PoolConfig{
		MaxConns:    int32(cfg.DatabasePoolMax),
		IdleTimeout: time.Duration(cfg.DatabasePoolIdleTimeoutMS) * time.Millisecond,
		ConnTimeout: time.Duration(cfg.DatabasePoolConnectionTimeoutMS) * time.Millisecond,
	})
	if err != nil {
		logger.Error("connect database", slog.Any("error", err))
		os.Exit(1)
	}
	defer pool.Close()

	rdb, err := db.NewRedis(db.RedisConfig{
		Host:     cfg.RedisHost,
		Port:     cfg.RedisPort,
		Password: cfg.RedisPassword,
		TLS:      cfg.RedisTLS,
	})
	if err != nil {
		logger.Error("configure redis", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = rdb.Close() }()

	kernel := httpx.New()
	kernel.MountHealth(httpx.HealthDeps{
		DB:    httpx.NewPGPinger(pool),
		Redis: httpx.NewRedisPinger(rdb),
	})

	kernel.Router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound,
			fmt.Sprintf("Cannot %s %s", r.Method, r.URL.Path)))
	})

	storageProvider := storage.NewDefaultProvider(cfg)
	waClient := whatsapp.New(cfg.WhatsAppAPIVersion, cfg.WhatsAppAccessToken)
	psClient := paystack.New(paystack.Config{
		SecretKey: cfg.PaystackSecretKey,
		BaseURL:   cfg.PaystackBaseURL,
	})

	authDeps := handlers.AuthDeps{
		Redis:      auth.NewGoRedisAdapter(rdb),
		Pool:       pool,
		Secret:     cfg.JWTSecret,
		Mailer:     otpMailer{cfg: smtp.Config{Host: cfg.SMTPHost, Port: cfg.SMTPPort, User: cfg.SMTPUser, Pass: cfg.SMTPPass}},
		WhatsApp:   waSender{c: waClient},
		WhatsAppID: cfg.WhatsAppPhoneNumberID,
	}
	kernel.Router.Route("/auth", func(a chi.Router) {
		a.Use(auth.Middleware(cfg.JWTSecret, func(r *http.Request) bool {
			if r.Method != http.MethodPost {
				return false
			}
			return r.URL.Path == "/auth/request-otp" || r.URL.Path == "/auth/verify-otp"
		}))
		handlers.MountAuth(a, authDeps)
	})

	kernel.Router.Group(func(v chi.Router) {
		v.Use(auth.Middleware(cfg.JWTSecret, nil))
		v.Handle("/", handlers.NewRoot())
		v.Mount("/customers", handlers.NewCustomers(pool))
		v.Route("/products", func(pr chi.Router) {
			pr.Mount("/", handlers.NewProducts(pool))
			handlers.MountProductsWrite(pr, handlers.ProductsDeps{
				Pool: pool, Storage: storageProvider,
			})
		})
		v.Route("/orders", func(or chi.Router) {
			or.Mount("/", handlers.NewOrders(pool))
			handlers.MountOrdersWrite(or, handlers.OrdersWriteDeps{Pool: pool})
		})
		v.Mount("/inbox", handlers.NewInbox(pool))
		v.Route("/conversations", func(cr chi.Router) {
			cr.Mount("/", handlers.NewConversations(pool))
			handlers.MountConversationsWrite(cr, handlers.ConversationsWriteDeps{Pool: pool})
		})
		v.Mount("/dashboard", handlers.NewDashboard(pool))
		v.Mount("/analytics", handlers.NewAnalytics(pool))
		v.Route("/payouts", func(pr chi.Router) {
			pr.Mount("/", handlers.NewPayouts(pool))
			handlers.MountPayoutsWrite(pr, handlers.PayoutsWriteDeps{Pool: pool, Paystack: psClient})
		})
		v.Route("/businesses", func(b chi.Router) { handlers.MountBusinesses(b, handlers.BusinessesDeps{Pool: pool}) })
	})

	addr := ":" + itoa(cfg.Port)
	srv, err := kernel.Start(addr)
	if err != nil {
		logger.Error("start server", slog.String("addr", addr), slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("api listening", slog.String("addr", addr))

	<-ctx.Done()
	kernel.BeginShutdown()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpx.Shutdown(shutdownCtx, srv); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
	}
}

type otpMailer struct{ cfg smtp.Config }

func (m otpMailer) SendOtpEmail(to, code string) error {
	return smtp.SendOTPEmail(m.cfg, to, code)
}

type waSender struct{ c *whatsapp.Client }

func (w waSender) SendTextMessage(phoneNumberID, to, text string) error {
	return w.c.SendTextMessage(context.Background(), phoneNumberID, to, text)
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
