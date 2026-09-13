package observability

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/getsentry/sentry-go"
	sentryhttpclient "github.com/getsentry/sentry-go/httpclient"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TraceTask runs one queue task as a "queue.process" transaction on its own
// hub, so the task's database queries, provider calls and logs are grouped
// (and profiled when picked). It is a no-op wrapper without tracing.
func TraceTask(ctx context.Context, taskType string, run func(ctx context.Context) error) error {
	hub := sentry.GetHubFromContext(ctx)
	if hub == nil {
		hub = sentry.CurrentHub().Clone()
		ctx = sentry.SetHubOnContext(ctx, hub)
	}
	hub.Scope().SetTag("task", taskType)
	if !tracingEnabled(hub) {
		return run(ctx)
	}

	queueName, _, _ := strings.Cut(taskType, ":")
	tx := sentry.StartTransaction(ctx, taskType,
		sentry.WithOpName("queue.process"),
		sentry.WithTransactionSource(sentry.SourceTask),
	)
	tx.SetData("messaging.system", "asynq")
	tx.SetData("messaging.destination.name", queueName)
	if id, ok := asynq.GetTaskID(ctx); ok {
		tx.SetData("messaging.message.id", id)
	}
	if retry, ok := asynq.GetRetryCount(ctx); ok {
		tx.SetData("messaging.message.retry.count", retry)
	}
	prof := startProfile(tx)

	var err error
	defer func() {
		if rec := recover(); rec != nil {
			tx.Status = sentry.SpanStatusInternalError
			prof.stop()
			tx.Finish()
			panic(rec)
		}
		tx.Status = sentry.SpanStatusOK
		if err != nil {
			tx.Status = sentry.SpanStatusInternalError
		}
		prof.stop()
		tx.Finish()
	}()
	err = run(tx.Context())
	return err
}

// PGXTracer records every query run inside a transaction as a "db.sql.query"
// span (the Prisma/pg auto-instrumentation). Queries outside a transaction are
// not traced.
type PGXTracer struct{}

type pgxSpanKey struct{}

func (PGXTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	parent := sentry.SpanFromContext(ctx)
	if parent == nil || !parent.Sampled.Bool() {
		return ctx
	}
	statement := strings.Join(strings.Fields(data.SQL), " ")
	span := parent.StartChild("db.sql.query", sentry.WithDescription(statement))
	span.SetData("db.system", "postgresql")
	span.SetData("db.statement", statement)
	return context.WithValue(span.Context(), pgxSpanKey{}, span)
}

func (PGXTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, ok := ctx.Value(pgxSpanKey{}).(*sentry.Span)
	if !ok {
		return
	}
	switch {
	case data.Err == nil:
		span.Status = sentry.SpanStatusOK
		span.SetData("db.rows_affected", data.CommandTag.RowsAffected())
	case errors.Is(data.Err, pgx.ErrNoRows):
		span.Status = sentry.SpanStatusNotFound
	default:
		span.Status = sentry.SpanStatusInternalError
		var pgErr *pgconn.PgError
		if errors.As(data.Err, &pgErr) {
			span.SetData("db.error.code", pgErr.Code)
		}
	}
	span.Finish()
}

// instrumentOutboundHTTP makes every provider call made through the default
// transport (Meta, OpenAI, Gemini, Paystack, Cloudinary) an "http.client"
// span of the surrounding transaction, propagating the trace.
func instrumentOutboundHTTP() {
	if _, done := http.DefaultTransport.(*sentryhttpclient.SentryRoundTripper); done {
		return
	}
	http.DefaultTransport = sentryhttpclient.NewSentryRoundTripper(http.DefaultTransport)
}
