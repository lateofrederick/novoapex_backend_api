package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
)

// T2.28a-e: exact ports of libs/common/src/payments/revenue.service.ts,
// validated against T0.10/T0.16 money discipline (DECIMAL sums only).

type revWorld struct {
	e    *epEnv
	biz  harness.Business
	day1 time.Time // 2026-08-10T10:15Z (a Monday)
	day2 time.Time // 2026-08-13T22:30Z (same ISO week)
	pA   harness.Product
	pB   harness.Product
}

func ep_seedRevenueWorld(t *testing.T) *revWorld {
	e := ep_startTest(t)
	biz := e.F.Business()
	cust := e.F.Customer(biz.ID)

	w := &revWorld{
		e:    e,
		biz:  biz,
		day1: time.Date(2026, 8, 10, 10, 15, 0, 0, time.UTC),
		day2: time.Date(2026, 8, 13, 22, 30, 0, 0, time.UTC),
		pA:   e.F.Product(biz.ID),
		pB:   e.F.Product(biz.ID),
	}
	now := time.Now().UTC()

	ep_seedPayment(t, e, "rev_s1", biz.ID, "", "SUCCESS", "100.50", w.day1, now)
	ep_seedPayment(t, e, "rev_s2", biz.ID, "", "SUCCESS", "49.50", w.day2, now)
	ep_seedPayment(t, e, "rev_p", biz.ID, "", "PENDING", "500.00", w.day1, now)
	ep_seedPayment(t, e, "rev_f", biz.ID, "", "FAILED", "75.00", w.day2, now)

	ep_seedOrder(t, e, "rev_paid", biz.ID, cust.ID, "", "PAID", "35.50", now)
	ep_seedOrderItem(t, e, "ri_a1", "rev_paid", w.pA.ID, w.pA.Name, 3, "10.00")
	ep_seedOrderItem(t, e, "ri_a2", "rev_paid", w.pA.ID, w.pA.Name, 2, "10.00") // duplicate line item aggregates

	ep_seedOrder(t, e, "rev_conf", biz.ID, cust.ID, "", "CONFIRMED", "20.00", now)
	ep_seedOrderItem(t, e, "ri_b1", "rev_conf", w.pB.ID, w.pB.Name, 2, "5.25")

	ep_seedOrder(t, e, "rev_pp", biz.ID, cust.ID, "", "PAYMENT_PENDING", "12.25", now)
	ep_seedOrder(t, e, "rev_pend", biz.ID, cust.ID, "", "PENDING", "99.00", now)
	return w
}

// T2.28a getTotalRevenue: SUCCESS payment amounts only.
func TestRevenueGetTotalRevenue(t *testing.T) {
	w := ep_seedRevenueWorld(t)
	rv := NewRevenueService(w.e.Pool)
	ctx := context.Background()

	got, err := rv.GetTotalRevenue(ctx, w.biz.ID, nil)
	if err != nil {
		t.Fatalf("total revenue: %v", err)
	}
	if !got.Equal(decimal.RequireFromString("150")) {
		t.Fatalf("totalRevenue = %s, want 150.00 exactly (no float slop)", got)
	}

	// Inclusive paidAt window hitting only day1's payment.
	onlyDay1, err := rv.GetTotalRevenue(ctx, w.biz.ID, &RevenueDateRange{From: w.day1, To: w.day1})
	if err != nil || !onlyDay1.Equal(decimal.RequireFromString("100.50")) {
		t.Fatalf("window [day1,day1] = %v err=%v, want 100.50 (gte/lte bounds)", onlyDay1, err)
	}

	fromDay2, err := rv.GetTotalRevenue(ctx, w.biz.ID, &RevenueDateRange{From: w.day2, To: w.day2.Add(time.Hour)})
	if err != nil || !fromDay2.Equal(decimal.RequireFromString("49.50")) {
		t.Fatalf("window [day2,+1h] = %v err=%v, want 49.50", fromDay2, err)
	}

	emptyBiz := w.e.F.Business().ID
	empty, err := rv.GetTotalRevenue(ctx, emptyBiz, nil)
	if err != nil || !empty.IsZero() {
		t.Fatalf("empty business = %v err=%v, want exact 0 (Prisma ?? 0)", empty, err)
	}
}

// T2.28b getPipelineValue: CONFIRMED + PAYMENT_PENDING order totals.
func TestRevenuePipelineValue(t *testing.T) {
	w := ep_seedRevenueWorld(t)
	rv := NewRevenueService(w.e.Pool)

	got, err := rv.GetPipelineValue(context.Background(), w.biz.ID)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if !got.Equal(decimal.RequireFromString("32.25")) { // 20.00 + 12.25
		t.Fatalf("pipeline = %s, want 32.25 (PAID/PENDING/CANCELLED excluded)", got)
	}
}

// T2.28c getRevenueByPeriod: date_trunc buckets over SUCCESS payments.
func TestRevenueByPeriod(t *testing.T) {
	w := ep_seedRevenueWorld(t)
	rv := NewRevenueService(w.e.Pool)
	ctx := context.Background()

	t.Run("day buckets newest first", func(t *testing.T) {
		rows, err := rv.GetRevenueByPeriod(ctx, w.biz.ID, "day", nil)
		if err != nil {
			t.Fatalf("by period: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("rows = %d (%+v)", len(rows), rows)
		}
		wantPeriods := []string{"2026-08-13T00:00:00.000Z", "2026-08-10T00:00:00.000Z"}
		wantTotals := []string{"49.50", "100.50"}
		for i, row := range rows {
			if row.Period != wantPeriods[i] {
				t.Errorf("rows[%d].period = %s, want %s (toISOString of trunc)", i, row.Period, wantPeriods[i])
			}
			if !row.TotalRevenue.Equal(decimal.RequireFromString(wantTotals[i])) {
				t.Errorf("rows[%d].totalRevenue = %s, want %s", i, row.TotalRevenue, wantTotals[i])
			}
			if row.TransactionCount != 1 {
				t.Errorf("rows[%d].transactionCount = %d", i, row.TransactionCount)
			}
		}
	})

	t.Run("week truncation is ISO Monday", func(t *testing.T) {
		rows, err := rv.GetRevenueByPeriod(ctx, w.biz.ID, "week", nil)
		if err != nil {
			t.Fatalf("week: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("both days share the week of Mon 2026-08-10, got %+v", rows)
		}
		if rows[0].Period != "2026-08-10T00:00:00.000Z" ||
			!rows[0].TotalRevenue.Equal(decimal.RequireFromString("150")) ||
			rows[0].TransactionCount != 2 {
			t.Fatalf("week bucket = %+v", rows[0])
		}
	})

	t.Run("month bucket", func(t *testing.T) {
		rows, err := rv.GetRevenueByPeriod(ctx, w.biz.ID, "month", nil)
		if err != nil || len(rows) != 1 ||
			rows[0].Period != "2026-08-01T00:00:00.000Z" ||
			rows[0].TransactionCount != 2 ||
			!rows[0].TotalRevenue.Equal(decimal.RequireFromString("150")) {
			t.Fatalf("month = %+v err=%v", rows, err)
		}
	})

	t.Run("date window filters buckets", func(t *testing.T) {
		rows, err := rv.GetRevenueByPeriod(ctx, w.biz.ID, "day",
			&RevenueDateRange{From: time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 8, 13, 23, 59, 0, 0, time.UTC)})
		if err != nil || len(rows) != 1 || rows[0].Period != "2026-08-13T00:00:00.000Z" {
			t.Fatalf("windowed = %+v err=%v, want only Aug 13", rows, err)
		}
	})

	t.Run("invalid granularity rejected like raw-query assembly would break", func(t *testing.T) {
		if _, err := rv.GetRevenueByPeriod(ctx, w.biz.ID, "hour", nil); err == nil {
			t.Fatal("expected error for unsupported granularity")
		}
	})
}

// T2.28d getTopProducts: quantity/revenue per product across non-cancelled
// orders, duplicate line items aggregated.
func TestRevenueTopProducts(t *testing.T) {
	w := ep_seedRevenueWorld(t)
	rv := NewRevenueService(w.e.Pool)

	rows, err := rv.GetTopProducts(context.Background(), w.biz.ID, 10)
	if err != nil {
		t.Fatalf("top products: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (cancelled orders excluded entirely)", len(rows))
	}
	first, second := rows[0], rows[1]
	if first.ProductID != w.pA.ID || first.TotalQuantity != 5 ||
		!first.TotalRevenue.Equal(decimal.RequireFromString("50")) {
		t.Errorf("first = %+v, want pA qty5 revenue 50.00 (3+2 items aggregated)", first)
	}
	if second.ProductID != w.pB.ID || second.TotalQuantity != 2 ||
		!second.TotalRevenue.Equal(decimal.RequireFromString("10.50")) {
		t.Errorf("second = %+v, want pB qty2 revenue 10.50", second)
	}
	if first.ProductName != w.pA.Name {
		t.Errorf("productName snapshot = %q, want %q", first.ProductName, w.pA.Name)
	}

	single, err := rv.GetTopProducts(context.Background(), w.biz.ID, 1)
	if err != nil || len(single) != 1 || single[0].ProductID != w.pA.ID {
		t.Fatalf("limit=1 = %+v err=%v", single, err)
	}

	def, err := rv.GetTopProducts(context.Background(), w.biz.ID, 0)
	if err != nil || len(def) == 0 {
		t.Errorf("limit<=0 must fall back to Node default 10, got %+v err=%v", def, err)
	}
}

// T2.28e getPaymentsByStatus: full breakdown, count desc.
func TestRevenuePaymentsByStatus(t *testing.T) {
	w := ep_seedRevenueWorld(t)
	rv := NewRevenueService(w.e.Pool)

	rows, err := rv.GetPaymentsByStatus(context.Background(), w.biz.ID)
	if err != nil {
		t.Fatalf("by status: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d (%+v)", len(rows), rows)
	}
	type row struct {
		status string
		count  int
		total  string
	}
	var got []row
	for _, r := range rows {
		got = append(got, row{r.Status, r.Count, r.TotalAmount.String()})
	}
	want := []row{
		{"SUCCESS", 2, "150"},
		{"FAILED", 1, "75"},   // count tie broken deterministically by status ASC
		{"PENDING", 1, "500"}, // Node leaves tie order undefined
	}
	for i, wk := range want {
		if got[i] != wk {
			t.Errorf("rows[%d] = %+v, want %+v", i, got[i], wk)
		}
	}
}

// T2.28 cross-check: service output equals what flows through the
// payouts/dashboard endpoints (the only Node-visible consumers of the same
// aggregates), proving one consistent money path end-to-end.
func TestRevenueMatchesPayoutsDashboardOutputs(t *testing.T) {
	w := ep_seedRevenueWorld(t)
	rv := NewRevenueService(w.e.Pool)
	claims := auth.Claims{Phone: w.biz.OwnerPhone, BusinessID: w.biz.ID}

	serviceTotal, err := rv.GetTotalRevenue(context.Background(), w.biz.ID, nil)
	if err != nil {
		t.Fatalf("service total: %v", err)
	}

	recBalance := ep_do(t, ep_mount(t, "/payouts", NewPayouts(w.e.Pool)),
		http.MethodGet, "/payouts/balance", &claims)
	balance := ep_decode(t, recBalance)
	balanceNum, ok := balance["totalRevenue"].(json.Number)
	if !ok {
		t.Fatalf("balance.totalRevenue not a JSON number: %v", balance["totalRevenue"])
	}
	if got, err := decimal.NewFromString(balanceNum.String()); err != nil || !got.Equal(serviceTotal) {
		t.Errorf("balance.totalRevenue = %v, service = %s (err=%v)", balance["totalRevenue"], serviceTotal, err)
	}

	recDash := ep_do(t, ep_mount(t, "/dashboard", NewDashboard(w.e.Pool)), http.MethodGet, "/dashboard/summary", &claims)
	dash := ep_decode(t, recDash)
	dashNum, ok := dash["totalPayouts"].(json.Number)
	if !ok {
		t.Fatalf("dashboard.totalPayouts not a JSON number: %v", dash["totalPayouts"])
	}
	if got, err := decimal.NewFromString(dashNum.String()); err != nil || !got.Equal(serviceTotal) {
		t.Errorf("dashboard.totalPayouts = %v, service = %s (err=%v)", dash["totalPayouts"], serviceTotal, err)
	}
}
