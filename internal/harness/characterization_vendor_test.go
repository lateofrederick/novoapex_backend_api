package harness

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"

	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type apiResponse struct {
	Status int
	Body   []byte
}

func (r apiResponse) JSON(t *testing.T) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(r.Body, &v); err != nil {
		t.Fatalf("non-JSON response %d: %s", r.Status, r.Body)
	}
	return v
}

func apiRequest(t *testing.T, baseURL, method, path string, body any, token string) apiResponse {
	t.Helper()
	var rdr io.Reader = bytes.NewReader(nil)
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, baseURL+path, rdr)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return apiResponse{Status: resp.StatusCode, Body: raw}
}

var otpDigitsRe = regexp.MustCompile(`\d{6}`)

func redisGet(t *testing.T, addr, password, key string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial redis: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	auth := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(RedisPassword), RedisPassword)
	get := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
	if _, err := conn.Write([]byte(auth + get)); err != nil {
		t.Fatalf("redis write: %v", err)
	}

	rd := bufio.NewReader(conn)
	readLine := func() string {
		line, err := rd.ReadString('\n')
		if err != nil {
			t.Fatalf("redis read: %v", err)
		}
		return strings.TrimRight(line, "\r\n")
	}
	authReply := readLine()
	if !strings.HasPrefix(authReply, "+OK") {
		t.Fatalf("redis auth failed: %q", authReply)
	}
	valLine := readLine()
	if valLine == "$-1" {
		return ""
	}
	if !strings.HasPrefix(valLine, "$") {
		t.Fatalf("unexpected redis reply %q for GET %s", valLine, key)
	}
	var n int
	if _, err := fmt.Sscanf(valLine, "$%d", &n); err != nil {
		t.Fatalf("parse bulk length %q: %v", valLine, err)
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(rd, buf); err != nil {
		t.Fatalf("read bulk: %v", err)
	}
	return string(buf[:n])
}

func signupVendorViaAPI(t *testing.T, stack *NodeStack, h *Harness, phone, email string) string {
	t.Helper()

	r := apiRequest(t, stack.BaseURL, http.MethodPost, "/auth/request-otp",
		map[string]any{"phone": phone, "deliveryMethod": "email", "email": email}, "")
	if r.Status != http.StatusOK {
		t.Fatalf("request-otp status=%d body=%s", r.Status, r.Body)
	}

	stack.SMTP.WaitForMessage(t, email, 20*time.Second)

	code := redisGet(t, h.RedisAddr, RedisPassword, "otp:"+phone)
	if len(code) != 6 {
		t.Fatalf("OTP for %s missing/invalid in redis: %q", phone, code)
	}

	vr := apiRequest(t, stack.BaseURL, http.MethodPost, "/auth/verify-otp",
		map[string]any{"phone": phone, "code": code}, "")
	if vr.Status != http.StatusOK {
		t.Fatalf("verify-otp status=%d body=%s", vr.Status, vr.Body)
	}
	parsed := vr.JSON(t)
	token, ok := parsed["token"].(string)
	if !ok || token == "" {
		t.Fatalf("verify-otp returned no token: %s", vr.Body)
	}
	isNew, _ := parsed["isNewUser"].(bool)
	if !isNew {
		t.Errorf("fresh signup should be isNewUser=true")
	}
	if parsed["business"] != nil {
		t.Errorf("fresh signup should have business=null, got %v", parsed["business"])
	}
	return token
}

func createBusinessViaAPI(t *testing.T, stack *NodeStack, token, name string) map[string]any {
	t.Helper()
	r := apiRequest(t, stack.BaseURL, http.MethodPost, "/businesses",
		map[string]any{
			"name":                  name,
			"currency":              "GHS",
			"whatsappPhoneNumberId": uniqueWNI(),
		}, token)
	if r.Status != http.StatusOK && r.Status != http.StatusCreated {
		t.Fatalf("POST /businesses status=%d body=%s", r.Status, r.Body)
	}
	biz := r.JSON(t)
	if biz["id"] == nil || biz["id"] == "" {
		t.Fatalf("created business has no id: %s", r.Body)
	}
	return biz
}

func TestT017_TenantIsolationAcrossVendors(t *testing.T) {
	h := startHarness(t)
	factory, _ := newFactoryOn(t, h)
	stack := StartNodeStack(t, h)

	const phoneA = "+233701234501"
	const phoneB = "+233701234502"

	tokenA := signupVendorViaAPI(t, stack, h, phoneA, "vendor-a@test.example")
	bizA := createBusinessViaAPI(t, stack, tokenA, "Vendor A Goods")
	idA := bizA["id"].(string)

	tokenB := signupVendorViaAPI(t, stack, h, phoneB, "vendor-b@test.example")
	bizB := createBusinessViaAPI(t, stack, tokenB, "Vendor B Goods")
	idB := bizB["id"].(string)

	ca1 := factory.Customer(idA)
	ca2 := factory.Customer(idA)
	cb1 := factory.Customer(idB)

	me := apiRequest(t, stack.BaseURL, http.MethodGet, "/auth/me", nil, tokenA)
	if me.Status != http.StatusOK {
		t.Fatalf("/auth/me status=%d body=%s", me.Status, me.Body)
	}
	if got := me.JSON(t)["id"]; got != idA {
		t.Errorf("/auth/me business id = %v, want %s", got, idA)
	}

	listA := apiRequest(t, stack.BaseURL, http.MethodGet, "/customers?page=1&limit=10", nil, tokenA)
	if listA.Status != http.StatusOK {
		t.Fatalf("GET /customers A status=%d body=%s", listA.Status, listA.Body)
	}
	assertCustomerPage(t, listA.Body, []string{ca1.ID, ca2.ID}, idA)

	listB := apiRequest(t, stack.BaseURL, http.MethodGet, "/customers?page=1&limit=10", nil, tokenB)
	if listB.Status != http.StatusOK {
		t.Fatalf("GET /customers B status=%d body=%s", listB.Status, listB.Body)
	}
	assertCustomerPage(t, listB.Body, []string{cb1.ID}, idB)

	cross := apiRequest(t, stack.BaseURL, http.MethodGet,
		fmt.Sprintf("/customers/%s", ca1.ID), nil, tokenB)
	if cross.Status != http.StatusNotFound && cross.Status != http.StatusForbidden {
		t.Errorf("cross-tenant customer fetch as B = %d, want 404/403 (body=%s)", cross.Status, cross.Body)
	}

	noAuth := apiRequest(t, stack.BaseURL, http.MethodGet, "/customers", nil, "")
	if noAuth.Status != http.StatusUnauthorized {
		t.Errorf("unauthenticated /customers = %d, want 401", noAuth.Status)
	}
	garbage := apiRequest(t, stack.BaseURL, http.MethodGet, "/customers", nil, "not-a-jwt")
	if garbage.Status != http.StatusUnauthorized {
		t.Errorf("garbage-token /customers = %d, want 401", garbage.Status)
	}
}

func assertCustomerPage(t *testing.T, body []byte, wantIDs []string, businessID string) {
	t.Helper()
	var page struct {
		Data []map[string]any `json:"data"`
		Meta struct {
			Total      float64 `json:"total"`
			Page       float64 `json:"page"`
			Limit      float64 `json:"limit"`
			TotalPages float64 `json:"totalPages"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("decode customers page: %v (%s)", err, body)
	}
	if float64(len(page.Data)) != float64(len(wantIDs)) || page.Meta.Total != float64(len(wantIDs)) {
		t.Fatalf("page has %d rows meta.total=%v, want %d", len(page.Data), page.Meta.Total, len(wantIDs))
	}
	seen := map[string]bool{}
	for _, row := range page.Data {
		id, _ := row["id"].(string)
		seen[id] = true
		if row["businessId"] != businessID {
			t.Errorf("row %s leaked wrong businessId %v (want %s)", id, row["businessId"], businessID)
		}
	}
	for _, want := range wantIDs {
		if !seen[want] {
			t.Errorf("expected customer %s missing from page %s", want, body)
		}
	}
}

var wniSeq = struct {
	mu sync.Mutex
	n  int
}{}

func uniqueWNI() string {
	wniSeq.mu.Lock()
	defer wniSeq.mu.Unlock()
	wniSeq.n++
	return fmt.Sprintf("wni_api_%04d", wniSeq.n)
}
