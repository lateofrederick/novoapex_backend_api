package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

// Revenue ports libs/common/src/payments/revenue.service.ts — the five
// revenue-intelligence reads (T2.28a-e). The Node service exposes no HTTP
// routes of its own; these are service-level ports with identical status
// filters and Decimal math, validated against T0.10/T0.16 money behaviour
// and cross-checked via the /payouts/balance + /dashboard/summary outputs.
//
// If central integration later wants an HTTP surface, mount a chi router
// with GET handlers over these methods (suggested paths in the stage report).

// RevenueDateRange is the optional inclusive paid_at window
// (RevenueService DateRange).
type RevenueDateRange struct {
	From time.Time
	To   time.Time
}

// RevenuePeriodEntry mirrors RevenueService's entry:
// {period: ISO string, totalRevenue: number, transactionCount: number}.
type RevenuePeriodEntry struct {
	Period           string       `json:"period"`
	TotalRevenue     money.Number `json:"totalRevenue"`
	TransactionCount int          `json:"transactionCount"`
}

// TopProduct mirrors {productId, productName, totalQuantity, totalRevenue}.
type TopProduct struct {
	ProductID     string       `json:"productId"`
	ProductName   string       `json:"productName"`
	TotalQuantity int          `json:"totalQuantity"`
	TotalRevenue  money.Number `json:"totalRevenue"`
}

// PaymentStatusBreakdown mirrors {status, count, totalAmount}.
type PaymentStatusBreakdown struct {
	Status      string       `json:"status"`
	Count       int          `json:"count"`
	TotalAmount money.Number `json:"totalAmount"`
}

type RevenueService struct {
	q *gen.Queries
}

func NewRevenueService(pool *pgxpool.Pool) *RevenueService {
	return &RevenueService{q: gen.New(pool)}
}

// GetTotalRevenue sums SUCCESS payment amounts, optionally bounded by an
// inclusive paidAt window (revenue.service.ts:47-60). Empty set -> 0.
func (rv *RevenueService) GetTotalRevenue(ctx context.Context, businessID string, dateRange *RevenueDateRange) (decimal.Decimal, error) {
	if dateRange == nil {
		return rv.q.SumSuccessfulPaymentAmounts(ctx, businessID)
	}
	return rv.q.SumSuccessfulPaymentAmountsBetween(ctx, gen.SumSuccessfulPaymentAmountsBetweenParams{
		BusinessID: businessID,
		PaidAt:     pgtype.Timestamp{Time: dateRange.From, Valid: true},
		PaidAt_2:   pgtype.Timestamp{Time: dateRange.To, Valid: true},
	})
}

// GetPipelineValue sums order totals still awaiting money:
// status IN (CONFIRMED, PAYMENT_PENDING) (revenue.service.ts:66-76).
func (rv *RevenueService) GetPipelineValue(ctx context.Context, businessID string) (decimal.Decimal, error) {
	return rv.q.SumPipelineOrderTotals(ctx, businessID)
}

var revenueGranularities = map[string]bool{"day": true, "week": true, "month": true}

// GetRevenueByPeriod groups SUCCESS payments under date_trunc windows,
// newest period first (revenue.service.ts:81-122). Granularity is one of
// day|week|month; the returned Period strings are toISOString() renderings
// of the truncation buckets.
func (rv *RevenueService) GetRevenueByPeriod(ctx context.Context, businessID, granularity string, dateRange *RevenueDateRange) ([]RevenuePeriodEntry, error) {
	if !revenueGranularities[granularity] {
		return nil, fmt.Errorf("revenue: granularity must be day, week or month, got %q", granularity)
	}

	rows, err := func() ([]gen.RevenueByPeriodAllTimeRow, error) {
		if dateRange == nil {
			return rv.q.RevenueByPeriodAllTime(ctx, gen.RevenueByPeriodAllTimeParams{
				BusinessID:  businessID,
				Granularity: granularity,
			})
		}
		between, berr := rv.q.RevenueByPeriodBetween(ctx, gen.RevenueByPeriodBetweenParams{
			BusinessID:  businessID,
			Granularity: granularity,
			PaidAt:      pgtype.Timestamp{Time: dateRange.From, Valid: true},
			PaidAt_2:    pgtype.Timestamp{Time: dateRange.To, Valid: true},
		})
		if berr != nil {
			return nil, berr
		}
		out := make([]gen.RevenueByPeriodAllTimeRow, len(between))
		for i, row := range between {
			out[i] = gen.RevenueByPeriodAllTimeRow(row)
		}
		return out, nil
	}()
	if err != nil {
		return nil, err
	}

	entries := make([]RevenuePeriodEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, RevenuePeriodEntry{
			Period:           ep_periodISO(row.Period.Time),
			TotalRevenue:     ep_num(row.TotalRevenue),
			TransactionCount: int(row.TransactionCount),
		})
	}
	return entries, nil
}

// GetTopProducts aggregates order_items across non-cancelled orders by
// quantity and revenue, ordered by revenue desc (revenue.service.ts:127-157).
// limit <= 0 falls back to the Node default of 10.
func (rv *RevenueService) GetTopProducts(ctx context.Context, businessID string, limit int) ([]TopProduct, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := rv.q.TopProductsByRevenue(ctx, gen.TopProductsByRevenueParams{
		BusinessID: businessID,
		Limit:      int32(limit),
	})
	if err != nil {
		return nil, err
	}

	out := make([]TopProduct, 0, len(rows))
	for _, row := range rows {
		out = append(out, TopProduct{
			ProductID:     row.ProductID,
			ProductName:   row.ProductName,
			TotalQuantity: int(row.TotalQuantity),
			TotalRevenue:  ep_num(row.TotalRevenue),
		})
	}
	return out, nil
}

// GetPaymentsByStatus breaks payments down per status, count desc
// (revenue.service.ts:162-181).
func (rv *RevenueService) GetPaymentsByStatus(ctx context.Context, businessID string) ([]PaymentStatusBreakdown, error) {
	rows, err := rv.q.PaymentTotalsByStatus(ctx, businessID)
	if err != nil {
		return nil, err
	}

	out := make([]PaymentStatusBreakdown, 0, len(rows))
	for _, row := range rows {
		out = append(out, PaymentStatusBreakdown{
			Status:      string(row.Status),
			Count:       int(row.Total),
			TotalAmount: ep_num(row.TotalAmount),
		})
	}
	return out, nil
}
