package harness

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func moneyFindLogged(stack *NodeStack, path string) map[string]any {
	for _, r := range paystackRequestLog(stack) {
		if r.Path == path {
			return r.Body
		}
	}
	return nil
}

func TestT016_PayoutBalanceGuardsAndTransfer(t *testing.T) {
	h := startHarness(t)
	_, db := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	const vendorPhone = "+233701234601"
	token := signupVendorViaAPI(t, stack, h, vendorPhone, "payout-vendor@test.example")
	biz := createBusinessViaAPI(t, stack, token, "Payout Vendor Goods")
	bizID := biz["id"].(string)

	moneySeedPayments(t, db, bizID, []moneySeedPayment{
		{ID: "pay_t016_success_1", Amount: "100.00", Status: "SUCCESS"},
		{ID: "pay_t016_success_2", Amount: "23.50", Status: "SUCCESS"},
		{ID: "pay_t016_failed", Amount: "50.00", Status: "FAILED"},
		{ID: "pay_t016_pending", Amount: "70.00", Status: "PENDING"},
		{ID: "pay_t016_refunded", Amount: "10.00", Status: "REFUNDED"},
	})

	balResp := apiRequest(t, stack.BaseURL, http.MethodGet, "/payouts/balance", nil, token)
	if balResp.Status != http.StatusOK {
		t.Fatalf("GET /payouts/balance status=%d body=%s", balResp.Status, balResp.Body)
	}
	moneyAssertBalance(t, balResp.Body, 123.50, 0, 123.50, "GHS")

	negReq := apiRequest(t, stack.BaseURL, http.MethodPost, "/payouts/request",
		map[string]any{"amount": -5, "accountNumber": "0244000011", "bankCode": "MTN", "accountName": "Fixture Vendor"}, token)
	if negReq.Status != http.StatusBadRequest {
		t.Errorf("negative-amount payout request status = %d, want 400 (zod amount.positive()); body=%s", negReq.Status, negReq.Body)
	} else {
		t.Logf("characterized: below-minimum rejection is zod validation, status=400 body=%s", negReq.Body)
	}

	overReq := apiRequest(t, stack.BaseURL, http.MethodPost, "/payouts/request",
		map[string]any{"amount": 999999, "accountNumber": "0244000011", "bankCode": "MTN", "accountName": "Fixture Vendor"}, token)
	if overReq.Status != http.StatusBadRequest {
		t.Errorf("over-balance payout request status = %d, want 400; body=%s", overReq.Status, overReq.Body)
	}
	var overErr struct {
		Message    any    `json:"message"`
		Error      string `json:"error"`
		StatusCode int    `json:"statusCode"`
	}
	if err := json.Unmarshal(overReq.Body, &overErr); err != nil {
		t.Fatalf("decode over-balance error: %v (%s)", err, overReq.Body)
	}
	msgStr := fmt.Sprint(overErr.Message)
	if !strings.Contains(msgStr, "Insufficient balance") || !strings.Contains(msgStr, "GHS 123.50") || !strings.Contains(msgStr, "GHS 999999.00") {
		t.Errorf("insufficient-balance message shape mismatch, got message=%q error=%q statusCode=%d", overErr.Message, overErr.Error, overErr.StatusCode)
	}

	validReq := apiRequest(t, stack.BaseURL, http.MethodPost, "/payouts/request",
		map[string]any{"amount": 80, "accountNumber": "0244000011", "bankCode": "MTN", "accountName": "Fixture Vendor"}, token)
	if validReq.Status != http.StatusOK && validReq.Status != http.StatusCreated {
		t.Fatalf("valid payout request status = %d body=%s\n--- logs ---\n%s", validReq.Status, validReq.Body, stack.DumpLogs())
	}

	var payoutResp struct {
		ID                 string  `json:"id"`
		Amount             float64 `json:"amount"`
		Status             string  `json:"status"`
		Reference          string  `json:"reference"`
		PaystackTransferID *string `json:"paystackTransferId"`
		Currency           string  `json:"currency"`
		BusinessID         string  `json:"businessId"`
	}
	if err := json.Unmarshal(validReq.Body, &payoutResp); err != nil {
		t.Fatalf("decode payout response: %v (%s)", err, validReq.Body)
	}
	if !moneyNear(payoutResp.Amount, 80) {
		t.Errorf("payout response amount = %v, want 80", payoutResp.Amount)
	}
	if payoutResp.Status != "SUCCESS" {
		t.Errorf("payout response status = %q, want SUCCESS (stub transfer returns status success)", payoutResp.Status)
	}
	if !strings.HasPrefix(payoutResp.Reference, "payout_") {
		t.Errorf("payout reference = %q, want payout_<uuid> prefix", payoutResp.Reference)
	}
	if payoutResp.PaystackTransferID == nil || *payoutResp.PaystackTransferID != "TRF_test_001" {
		t.Errorf("paystackTransferId = %v, want TRF_test_001", payoutResp.PaystackTransferID)
	}

	recipientBody := moneyFindLogged(stack, "/transferrecipient")
	if recipientBody == nil {
		t.Errorf("/transferrecipient never hit on paystack stub; logged paths: %+v", paystackRequestLog(stack))
	} else {
		if recipientBody["type"] != "momo" {
			t.Errorf("transferrecipient type = %v, want momo", recipientBody["type"])
		}
		if recipientBody["account_number"] != "0244000011" || recipientBody["bank_code"] != "MTN" || recipientBody["name"] != "Fixture Vendor" || recipientBody["currency"] != "GHS" {
			t.Errorf("transferrecipient body mismatch: %+v", recipientBody)
		}
	}

	transferBody := moneyFindLogged(stack, "/transfer")
	if transferBody == nil {
		t.Errorf("/transfer never hit on paystack stub; logged paths: %+v", paystackRequestLog(stack))
	} else {
		amt, _ := transferBody["amount"].(float64)
		if amt != 8000 {
			t.Errorf("transfer amount = %v, want 8000 (characterized reality: initiateTransfer also converts major to MINOR units)", amt)
		}
		if transferBody["source"] != "balance" {
			t.Errorf("transfer source = %v, want balance", transferBody["source"])
		}
		if transferBody["recipient"] != "RCPT_test_001" {
			t.Errorf("transfer recipient = %v, want RCPT_test_001", transferBody["recipient"])
		}
		if transferBody["reason"] != "Vendor Payout" {
			t.Errorf("transfer reason = %v, want Vendor Payout", transferBody["reason"])
		}
		if transferBody["reference"] != payoutResp.Reference {
			t.Errorf("transfer reference = %v, want %s", transferBody["reference"], payoutResp.Reference)
		}
	}

	recipientCode := scalarString(t, db, `SELECT COALESCE(paystack_recipient_code,'') FROM businesses WHERE id = $1`, bizID)
	if recipientCode != "RCPT_test_001" {
		t.Errorf("businesses.paystack_recipient_code = %q, want RCPT_test_001 (persisted for reuse)", recipientCode)
	}

	payoutStatus := scalarString(t, db, `SELECT status::text FROM payouts WHERE reference = $1`, payoutResp.Reference)
	if payoutStatus != "SUCCESS" {
		t.Errorf("payout row status = %s, want SUCCESS", payoutStatus)
	}
	payoutAmount := scalarString(t, db, `SELECT amount::text FROM payouts WHERE reference = $1`, payoutResp.Reference)
	if payoutAmount != "80.00" {
		t.Errorf("payout row amount = %s, want 80.00", payoutAmount)
	}

	balResp2 := apiRequest(t, stack.BaseURL, http.MethodGet, "/payouts/balance", nil, token)
	if balResp2.Status != http.StatusOK {
		t.Fatalf("GET /payouts/balance after payout status=%d body=%s", balResp2.Status, balResp2.Body)
	}
	moneyAssertBalance(t, balResp2.Body, 123.50, 80, 43.50, "GHS")

	histResp := apiRequest(t, stack.BaseURL, http.MethodGet, "/payouts/history?page=1&limit=10", nil, token)
	if histResp.Status != http.StatusOK {
		t.Fatalf("GET /payouts/history status=%d body=%s", histResp.Status, histResp.Body)
	}
	var hist struct {
		Data []map[string]any `json:"data"`
		Meta struct {
			Total float64 `json:"total"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(histResp.Body, &hist); err != nil {
		t.Fatalf("decode history: %v (%s)", err, histResp.Body)
	}
	foundRef := false
	for _, row := range hist.Data {
		if ref, _ := row["reference"].(string); ref == payoutResp.Reference {
			foundRef = true
		}
	}
	if !foundRef || int(hist.Meta.Total) < 1 {
		t.Errorf("history does not contain the new payout (foundRef=%v total=%v): %s", foundRef, hist.Meta.Total, histResp.Body)
	}
}
