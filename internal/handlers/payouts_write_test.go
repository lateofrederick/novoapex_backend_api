package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
)

// s4o_paystub is a local Paystack API stub (httptest) wired through the REAL
// paystack.Client, recording hits so tests can assert endpoint order,
// minor-unit amounts and recipient-code caching.
type s4o_paystub struct {
	srv           *httptest.Server
	client        *paystack.Client
	recipientHits atomic.Int64
	transferHits  atomic.Int64

	mu               sync.Mutex
	lastRecipientBod map[string]any
	lastTransferBod  map[string]any
	transferRefs     []string
	transferAmounts  []float64

	transferStatus string // canned data.status; "" -> success
	transferFails  bool   // when true, /transfer answers status:false
}

func newS4OPayStub(t *testing.T) *s4o_paystub {
	t.Helper()
	st := &s4o_paystub{transferStatus: "success"}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /transferrecipient", func(w http.ResponseWriter, r *http.Request) {
		st.recipientHits.Add(1)
		st.mu.Lock()
		st.lastRecipientBod = s4o_decodeBody(t, r)
		st.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  true,
			"message": "Transfer recipient created successfully",
			"data":    map[string]any{"recipient_code": "RCP_s4o_001", "type": "momo"},
		})
	})
	mux.HandleFunc("POST /transfer", func(w http.ResponseWriter, r *http.Request) {
		st.transferHits.Add(1)
		body := s4o_decodeBody(t, r)
		st.mu.Lock()
		st.lastTransferBod = body
		if n, ok := body["amount"].(float64); ok {
			st.transferAmounts = append(st.transferAmounts, n)
		}
		if ref, ok := body["reference"].(string); ok {
			st.transferRefs = append(st.transferRefs, ref)
		}
		fail := st.transferFails
		status := st.transferStatus
		st.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if fail {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": false, "message": "The amount specified is too large", "data": nil,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  true,
			"message": "Transfer successful",
			"data":    map[string]any{"status": status, "transfer_code": "TRF_s4o_001", "reference": body["reference"]},
		})
	})
	st.srv = httptest.NewServer(mux)
	t.Cleanup(st.srv.Close)
	st.client = paystack.New(paystack.Config{SecretKey: "sk_test_s4o", BaseURL: st.srv.URL})
	return st
}

func s4o_decodeBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read stub body: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("stub body not JSON: %v (%s)", err, raw)
	}
	return m
}

func (st *s4o_paystub) snapshot() (recip map[string]any, transfer map[string]any) {
	st.mu.Lock()
	defer st.mu.Unlock()
	recip, transfer = st.lastRecipientBod, st.lastTransferBod
	return recip, transfer
}

// payoutsWriteTree builds the write subtree like central integration will.
func payoutsWriteTree(e *epEnv, mover PaystackMover) http.Handler {
	r := chi.NewRouter()
	MountPayoutsWrite(r, PayoutsWriteDeps{Pool: e.Pool, Paystack: mover})
	return r
}

var s4o_referenceRe = regexp.MustCompile(`^payout_[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func s4o_requestPayout(t *testing.T, h http.Handler, target string, body string, claims *auth.Claims) *httptest.ResponseRecorder {
	t.Helper()
	return s4o_do(t, h, http.MethodPost, target, body, claims)
}

const s4o_payoutDTOBody = `{"amount":%s,"accountNumber":"0244000011","bankCode":"MTN","accountName":"Fixture Vendor"}`

// T4.21 POST /payouts/request — full money path.
func TestS4O_T4_21_RequestPayout(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	now := time.Now().UTC()

	// Balance inputs: SUCCESS payments only count as revenue.
	ep_seedPayment(t, e, "s4o-pay-1", biz.ID, "", "SUCCESS", "80.00", now, now)
	ep_seedPayment(t, e, "s4o-pay-2", biz.ID, "", "SUCCESS", "43.50", now, now)
	ep_seedPayment(t, e, "s4o-pay-3", biz.ID, "", "FAILED", "500.00", now, now)
	ep_seedPayout(t, e, "s4o-po-1", biz.ID, "SUCCESS", "20.00", "s4o-ref-1", now)

	stub := newS4OPayStub(t)
	h := ep_mount(t, "/payouts", payoutsWriteTree(e, stub.client))
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}

	t.Run("success flow: recipient created then transfer in MINOR units", func(t *testing.T) {
		rec := s4o_requestPayout(t, h, "/payouts/request",
			fmt.Sprintf(s4o_payoutDTOBody, "80"), &claims)
		if rec.Code != http.StatusCreated { // Nest @Post default 201
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}

		body := ep_decode(t, rec)
		wantKeys := "[amount businessId createdAt currency id paystackTransferId reference status updatedAt]"
		if ks := fmt.Sprint(ep_keys(t, body)); ks != wantKeys {
			t.Fatalf("key set =\n%s\nwant     \n%s", ks, wantKeys)
		}
		ep_assertMoney(t, body["amount"], "80")
		if body["status"] != "SUCCESS" { // provider data.status === 'success'
			t.Errorf("status = %v, want SUCCESS", body["status"])
		}
		if body["paystackTransferId"] != "TRF_s4o_001" {
			t.Errorf("paystackTransferId = %v", body["paystackTransferId"])
		}
		ep_assertISO(t, "createdAt", body["createdAt"])
		ep_assertISO(t, "updatedAt", body["updatedAt"])

		ref, _ := body["reference"].(string)
		if !s4o_referenceRe.MatchString(ref) {
			t.Errorf("reference = %q, want payout_<uuid v4>", ref)
		}

		// Stub saw /transferrecipient first, then /transfer with 80 GHS ->
		// 8000 pesewas and the SAME reference.
		recip, transfer := stub.snapshot()
		if recip == nil || transfer == nil {
			t.Fatalf("stub calls missing: recip=%v transfer=%v", recip, transfer)
		}
		for k, want := range map[string]any{
			"type": "momo", "name": "Fixture Vendor",
			"account_number": "0244000011", "bank_code": "MTN", "currency": "GHS",
		} {
			if recip[k] != want {
				t.Errorf("/transferrecipient %s = %v, want %v", k, recip[k], want)
			}
		}
		if transfer["source"] != "balance" {
			t.Errorf("/transfer source = %v, want balance", transfer["source"])
		}
		if n, ok := transfer["amount"].(float64); !ok || n != 8000 {
			t.Errorf("/transfer amount = %v, want numeric 8000 minor units", transfer["amount"])
		}
		if transfer["recipient"] != "RCP_s4o_001" {
			t.Errorf("/transfer recipient = %v", transfer["recipient"])
		}
		if transfer["reason"] != "Vendor Payout" {
			t.Errorf("/transfer reason = %v, want 'Vendor Payout'", transfer["reason"])
		}
		if transfer["reference"] != ref {
			t.Errorf("/transfer reference = %v, want %s", transfer["reference"], ref)
		}

		// Payout row persisted with all researched fields.
		var (
			dbStatus   string
			dbRef      string
			dbTransfer string
			dbCurrency string
			dbAmount   string
		)
		err := e.DB.QueryRow(`SELECT status::text, reference, COALESCE(paystack_transfer_id,''), currency, amount::text
			FROM payouts WHERE id = $1`, body["id"]).Scan(&dbStatus, &dbRef, &dbTransfer, &dbCurrency, &dbAmount)
		if err != nil {
			t.Fatalf("scan payout row: %v", err)
		}
		if dbStatus != "SUCCESS" || dbRef != ref || dbTransfer != "TRF_s4o_001" || dbCurrency != "GHS" || dbAmount != "80.00" {
			t.Errorf("payout row = status:%s ref:%s transfer:%s currency:%s amount:%s",
				dbStatus, dbRef, dbTransfer, dbCurrency, dbAmount)
		}

		// Recipient code cached on the business.
		var code string
		if err := e.DB.QueryRow(`SELECT COALESCE(paystack_recipient_code,'') FROM businesses WHERE id = $1`, biz.ID).
			Scan(&code); err != nil || code != "RCP_s4o_001" {
			t.Errorf("cached recipient code = %q err=%v", code, err)
		}
	})

	t.Run("second request reuses cached recipient code (no /transferrecipient hit)", func(t *testing.T) {
		beforeRecip := stub.recipientHits.Load()
		rec := s4o_requestPayout(t, h, "/payouts/request",
			fmt.Sprintf(s4o_payoutDTOBody, "20"), &claims)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if stub.recipientHits.Load() != beforeRecip {
			t.Errorf("/transferrecipient called again (%d -> %d): cache miss",
				beforeRecip, stub.recipientHits.Load())
		}
		if stub.transferHits.Load() != 2 {
			t.Errorf("/transfer hits = %d, want 2", stub.transferHits.Load())
		}
	})

	t.Run("below-balance reject keeps the source message shape", func(t *testing.T) {
		rec := s4o_requestPayout(t, h, "/payouts/request",
			fmt.Sprintf(s4o_payoutDTOBody, "999999"), &claims)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		body := ep_decode(t, rec)
		msg, _ := body["message"].(string)
		if body["statusCode"] != json_Number("400") ||
			!strings.Contains(msg, "Insufficient balance") ||
			!strings.Contains(msg, "GHS 3.50") || // 123.50 - 80 - 20 - 20 reserved above
			!strings.Contains(msg, "GHS 999999.00") {
			t.Errorf("reject body = %v", body)
		}

		var n int
		_ = e.DB.QueryRow(`SELECT COUNT(*) FROM payouts WHERE amount = 999999`).Scan(&n)
		if n != 0 {
			t.Errorf("rejected payout must not reserve funds, found %d rows", n)
		}
	})

	t.Run("RequestPayoutDto zod issues", func(t *testing.T) {
		cases := []struct {
			name, body, code, path string
		}{
			{"missing amount", `{"accountNumber":"1","bankCode":"MTN","accountName":"n"}`, "invalid_type", "amount"},
			{"string amount", `{"amount":"80","accountNumber":"1","bankCode":"MTN","accountName":"n"}`, "invalid_type", "amount"},
			{"zero amount", fmt.Sprintf(s4o_payoutDTOBody, "0"), "too_small", "amount"},
			{"negative amount", fmt.Sprintf(s4o_payoutDTOBody, "-5"), "too_small", "amount"},
			{"empty accountNumber", `{"amount":5,"accountNumber":"","bankCode":"MTN","accountName":"n"}`, "too_small", "accountNumber"},
			{"missing bankCode", `{"amount":5,"accountNumber":"1","accountName":"n"}`, "invalid_type", "bankCode"},
			{"missing accountName", `{"amount":5,"accountNumber":"1","bankCode":"MTN"}`, "invalid_type", "accountName"},
		}
		for _, tc := range cases {
			rec := s4o_requestPayout(t, h, "/payouts/request", tc.body, &claims)
			s4o_assertZodIssue(t, rec, tc.code, tc.path)
		}
	})

	t.Run("unknown business -> BadRequest 'Business not found'", func(t *testing.T) {
		rec := s4o_requestPayout(t, h, "/payouts/request", fmt.Sprintf(s4o_payoutDTOBody, "5"),
			&auth.Claims{Phone: biz.OwnerPhone, BusinessID: "s4o-ghost-biz"})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if body := ep_decode(t, rec); body["message"] != "Business not found" {
			t.Errorf("body = %v", body)
		}
	})
}

// T4.21 failure path: provider rejects the transfer -> reservation released
// (payout FAILED) and the source's BadRequestException message surfaces.
func TestS4O_T4_21_TransferFailureReleasesReservation(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	now := time.Now().UTC()
	ep_seedPayment(t, e, "s4o-payf-1", biz.ID, "", "SUCCESS", "100.00", now, now)

	stub := newS4OPayStub(t)
	stub.transferFails = true
	h := ep_mount(t, "/payouts", payoutsWriteTree(e, stub.client))
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}

	rec := s4o_requestPayout(t, h, "/payouts/request", fmt.Sprintf(s4o_payoutDTOBody, "40"), &claims)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if body := ep_decode(t, rec); body["message"] != "Failed to initiate transfer via Paystack" {
		t.Errorf("body = %v", body)
	}

	var status string
	var ref string
	if err := e.DB.QueryRow(`SELECT status::text, reference FROM payouts WHERE business_id = $1`, biz.ID).
		Scan(&status, &ref); err != nil || status != "FAILED" {
		t.Errorf("payout row = status:%q ref:%q err=%v, want FAILED reservation release", status, ref, err)
	}
	if !s4o_referenceRe.MatchString(ref) {
		t.Errorf("failed payout keeps its reference format: %q", ref)
	}
	// Recipient was still created+cached before the failed transfer.
	var code string
	_ = e.DB.QueryRow(`SELECT COALESCE(paystack_recipient_code,'') FROM businesses WHERE id = $1`, biz.ID).Scan(&code)
	if code != "RCP_s4o_001" {
		t.Errorf("recipient cache after failed transfer = %q", code)
	}

	// The released reservation is spendable again: retry succeeds.
	stub.transferFails = false
	rec = s4o_requestPayout(t, h, "/payouts/request", fmt.Sprintf(s4o_payoutDTOBody, "100"), &claims)
	if rec.Code != http.StatusCreated {
		t.Fatalf("retry status=%d body=%s", rec.Code, rec.Body.String())
	}
}

// T4.23b concurrent double payout request: the advisory lock + PENDING
// reservation mean only ONE of two racing requests can succeed when the
// balance covers a single withdrawal.
func TestS4O_T4_23b_ConcurrentDoublePayout(t *testing.T) {
	e := ep_startTest(t)
	biz := e.F.Business()
	now := time.Now().UTC()
	ep_seedPayment(t, e, "s4o-payc-1", biz.ID, "", "SUCCESS", "100.00", now, now)

	stub := newS4OPayStub(t)
	h := ep_mount(t, "/payouts", payoutsWriteTree(e, stub.client))

	const racers = 8
	start := make(chan struct{})
	results := make(chan int, racers) // response codes
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			rec := s4o_requestPayout(t, h, "/payouts/request",
				fmt.Sprintf(s4o_payoutDTOBody, "60"), &auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID})
			results <- rec.Code
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	ok, rejected := 0, 0
	for code := range results {
		switch code {
		case http.StatusCreated:
			ok++
		case http.StatusBadRequest:
			rejected++
		default:
			t.Errorf("unexpected racer status %d", code)
		}
	}
	if ok != 1 || rejected != racers-1 {
		t.Fatalf("racing payouts: ok=%d rejected=%d, want exactly one success", ok, rejected)
	}

	// Committed payouts total exactly the single successful reservation:
	// no double-spend slipped through.
	var pendingPlusSuccess, failed int
	var sum string
	if err := e.DB.QueryRow(`SELECT COUNT(*), COALESCE(SUM(amount),0)::text FROM payouts
		WHERE business_id = $1 AND status IN ('SUCCESS','PENDING')`, biz.ID).
		Scan(&pendingPlusSuccess, &sum); err != nil {
		t.Fatalf("scan committed: %v", err)
	}
	_ = e.DB.QueryRow(`SELECT COUNT(*) FROM payouts WHERE business_id = $1 AND status = 'FAILED'`, biz.ID).Scan(&failed)
	if pendingPlusSuccess != 1 || sum != "60.00" {
		t.Errorf("committed payouts = %d rows sum %s, want 1 row 60.00", pendingPlusSuccess, sum)
	}
	if failed != racers-1 {
		t.Logf("note: %d losers surfaced as insufficient-balance rejections (never reserved)", failed)
	}
	if stub.transferHits.Load() != 1 {
		t.Errorf("/transfer must fire once, got %d", stub.transferHits.Load())
	}
}
