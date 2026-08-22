package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	testWhatsAppAppSecret   = "test-app-secret-0123456789abcdef"
	testWhatsAppVerifyToken = "test-verify-token"
	testJWTSecret           = "0123456789abcdef0123456789abcdef"
)

type NodeStack struct {
	BaseURL   string
	SMTP      *SMTPStub
	OpenAI    *OpenAIStub
	Paystack  *PaystackStub
	repoDir   string
	apiCmd    *exec.Cmd
	workerCmd *exec.Cmd
	logs      *lockedBuffer
	stopOnce  sync.Once
	t         *testing.T
}

func StartNodeStack(t *testing.T, h *Harness) *NodeStack {
	t.Helper()
	repoDir, err := NovoApexRepoDir()
	if err != nil {
		t.Fatalf("locate novoapex repo: %v", err)
	}

	apiBinary := filepath.Join(repoDir, "dist/apps/api/apps/api/src/main.js")
	workerBinary := filepath.Join(repoDir, "dist/apps/worker/apps/worker/src/main.js")
	for _, bin := range []string{apiBinary, workerBinary} {
		if _, err := os.Stat(bin); err != nil {
			t.Fatalf("node dist missing (%s) — run `npm run build:all` in %s first: %v", bin, repoDir, err)
		}
	}

	stack := &NodeStack{
		repoDir: repoDir,
		logs:    &lockedBuffer{},
		t:       t,
	}

	smtpPort := stack.startSMTPStub(t)
	openaiPort := stack.startOpenAIStub(t)
	paystackPort := stack.startPaystackStub(t)
	apiPort := freePort(t)

	env := append(os.Environ(),
		"NODE_ENV=test",
		fmt.Sprintf("PORT=%d", apiPort),
		"MOBILE_API_PORT="+strconv.Itoa(freePortSilent()),
		"DATABASE_URL="+h.PostgresDSN+"&schema=public",
		fmt.Sprintf("DATABASE_POOL_MAX=%d", 5),
		fmt.Sprintf("REDIS_HOST=%s", hostOf(h.RedisAddr)),
		fmt.Sprintf("REDIS_PORT=%s", portOf(h.RedisAddr)),
		"REDIS_PASSWORD="+RedisPassword,
		"JWT_SECRET="+testJWTSecret,
		"LOG_LEVEL=info",
		"WHATSAPP_VERIFY_TOKEN="+testWhatsAppVerifyToken,
		"WHATSAPP_APP_SECRET="+testWhatsAppAppSecret,
		"WHATSAPP_PHONE_NUMBER_ID=wni_test_001",
		"WHATSAPP_ACCESS_TOKEN=fake-access-token",
		"WHATSAPP_API_VERSION=v25.0",
		"OPENAI_API_KEY=sk-test-dummy",
		fmt.Sprintf("OPENAI_BASE_URL=http://127.0.0.1:%d/v1", openaiPort),
		"PAYSTACK_SECRET_KEY=sk_test_dummy",
		"PAYSTACK_PUBLIC_KEY=pk_test_dummy",
		fmt.Sprintf("PAYSTACK_BASE_URL=http://127.0.0.1:%d", paystackPort),
		"SMTP_HOST=127.0.0.1",
		fmt.Sprintf("SMTP_PORT=%d", smtpPort),
		"SMTP_USER=stub@example.com",
		"SMTP_PASS=stub-pass",
	)

	stack.apiCmd = spawnNode(t, stack.logs, repoDir, apiBinary, env)
	stack.workerCmd = spawnNode(t, stack.logs, repoDir, workerBinary, env)

	stack.BaseURL = fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	if err := pollHealth(stack.BaseURL, 90*time.Second); err != nil {
		t.Fatalf("node stack did not become healthy: %v\n--- process logs ---\n%s", err, stack.logs.String())
	}

	t.Cleanup(stack.Stop)
	return stack
}

func (s *NodeStack) Stop() {
	s.stopOnce.Do(func() {
		for _, cmd := range []*exec.Cmd{s.workerCmd, s.apiCmd} {
			if cmd == nil || cmd.Process == nil {
				continue
			}
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		}
		deadline := time.Now().Add(10 * time.Second)
		for _, cmd := range []*exec.Cmd{s.workerCmd, s.apiCmd} {
			if cmd == nil || cmd.Process == nil {
				continue
			}
			done := make(chan struct{})
			go func(c *exec.Cmd) { _, _ = c.Process.Wait(); close(done) }(cmd)
			select {
			case <-done:
			case <-time.After(time.Until(deadline)):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		}
	})
}

func (s *NodeStack) DumpLogs() string {
	return s.logs.String()
}

func spawnNode(t *testing.T, logBuf *lockedBuffer, dir, binary string, env []string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("node", binary)
	cmd.Dir = dir
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = io.MultiWriter(logBuf, os.Stderr)
	cmd.Stderr = io.MultiWriter(logBuf, os.Stderr)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", binary, err)
	}
	return cmd
}

func pollHealth(baseURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/health")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("no 200 from /health within %s", timeout)
}

func freePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func freePortSilent() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func hostOf(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

func portOf(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "6379"
	}
	return port
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type SMTPStub struct {
	listener net.Listener
	mu       sync.Mutex
	bodies   []string
}

func (s *NodeStack) startSMTPStub(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("smtp listen: %v", err)
	}
	stub := &SMTPStub{listener: ln}
	s.SMTP = stub
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go stub.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().(*net.TCPAddr).Port
}

func (s *SMTPStub) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	writeLine(conn, "220 stub.smtp ESMTP")
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	inData := false
	var data strings.Builder
	for sc.Scan() {
		line := sc.Text()
		if inData {
			if line == "." {
				s.mu.Lock()
				s.bodies = append(s.bodies, data.String())
				s.mu.Unlock()
				data.Reset()
				inData = false
				writeLine(conn, "250 OK queued")
			} else {
				data.WriteString(line)
				data.WriteString("\n")
			}
			continue
		}
		upper := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(upper, "EHLO"):
			writeLine(conn, "250-stub.smtp")
			writeLine(conn, "250-8BITMIME")
			writeLine(conn, "250 OK")
		case strings.HasPrefix(upper, "HELO"),
			strings.HasPrefix(upper, "MAIL FROM"),
			strings.HasPrefix(upper, "RCPT TO"):
			writeLine(conn, "250 OK")
		case strings.HasPrefix(upper, "AUTH"):
			writeLine(conn, "235 2.7.0 Accepted")
		case upper == "DATA":
			inData = true
			writeLine(conn, "354 End data with <CR><LF>.<CR><LF>")
		case upper == "QUIT":
			writeLine(conn, "221 Bye")
			return
		default:
			writeLine(conn, "250 OK")
		}
	}
}

func (s *SMTPStub) Messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.bodies))
	copy(out, s.bodies)
	return out
}

func (s *SMTPStub) WaitForMessage(t *testing.T, substring string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range s.Messages() {
			if strings.Contains(m, substring) {
				return m
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no smtp message containing %q within %s; got %d message(s)", substring, timeout, len(s.Messages()))
	return ""
}

func writeLine(w io.Writer, s string) {
	_, _ = io.WriteString(w, s+"\r\n")
}

type OpenAIStub struct {
	server     *http.Server
	ChatReply  func(request map[string]any) map[string]any
	mu         sync.Mutex
	requestLog []map[string]any
}

func (s *NodeStack) startOpenAIStub(t *testing.T) int {
	t.Helper()
	port := freePortSilent()
	stub := &OpenAIStub{}
	s.OpenAI = stub

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		stub.mu.Lock()
		stub.requestLog = append(stub.requestLog, req)
		handler := stub.ChatReply
		stub.mu.Unlock()

		obj := defaultLLMResponse()
		if handler != nil {
			obj = handler(req)
		}
		content, _ := json.Marshal(obj)
		writeJSON(w, map[string]any{
			"id":      "chatcmpl-stub",
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   "gpt-4o-mini",
			"choices": []any{map[string]any{
				"index":         0,
				"message":       map[string]any{"role": "assistant", "content": string(content)},
				"finish_reason": "stop",
			}},
			"usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
		})
	})
	mux.HandleFunc("/v1/responses", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		stub.mu.Lock()
		stub.requestLog = append(stub.requestLog, req)
		handler := stub.ChatReply
		stub.mu.Unlock()

		obj := defaultLLMResponse()
		if handler != nil {
			obj = handler(req)
		}
		content, _ := json.Marshal(obj)
		text := string(content)
		now := time.Now().Unix()
		writeJSON(w, map[string]any{
			"id":         fmt.Sprintf("resp_stub_%d", now),
			"object":     "response",
			"created_at": now,
			"model":      "gpt-4o-mini",
			"status":     "completed",
			"output": []any{map[string]any{
				"type":   "message",
				"id":     fmt.Sprintf("msg_stub_%d", now),
				"status": "completed",
				"role":   "assistant",
				"content": []any{map[string]any{
					"type":        "output_text",
					"text":        text,
					"annotations": []any{},
				}},
			}},
			"parallel_tool_calls": false,
			"usage":               map[string]any{"input_tokens": 10, "output_tokens": 10, "total_tokens": 20},
		})
	})
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input any    `json:"input"`
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		texts := flattenInput(req.Input)
		data := make([]any, 0, len(texts))
		for i, txt := range texts {
			data = append(data, map[string]any{
				"object":    "embedding",
				"index":     i,
				"embedding": Embedding(txt, 768),
			})
		}
		writeJSON(w, map[string]any{
			"object": "list",
			"data":   data,
			"model":  req.Model,
			"usage":  map[string]any{"prompt_tokens": 1, "total_tokens": 1},
		})
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	stub.server = &http.Server{Addr: addr, Handler: mux}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("openai stub listen: %v", err)
	}
	go func() { _ = stub.server.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = stub.server.Shutdown(ctx)
	})
	return port
}

func (s *OpenAIStub) ChatRequests() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]any, len(s.requestLog))
	copy(out, s.requestLog)
	return out
}

func (s *NodeStack) SetChatReply(fn func(request map[string]any) map[string]any) {
	s.OpenAI.mu.Lock()
	defer s.OpenAI.mu.Unlock()
	s.OpenAI.ChatReply = fn
}

func defaultLLMResponse() map[string]any {
	return map[string]any{
		"reply_text":                "Hello! How can I help you today?",
		"internal_confidence":       0.95,
		"intent":                    "greeting",
		"customer_requested_images": false,
		"send_product_image_ids":    []any{},
		"referenced_product_ids":    []any{},
		"crm_signals": map[string]any{
			"order_confirmed":      false,
			"detected_items":       []any{},
			"delivery_area":        nil,
			"customer_name":        nil,
			"detected_preferences": []any{},
			"sentiment":            "neutral",
		},
	}
}

func flattenInput(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return []string{fmt.Sprint(v)}
	}
}

type PaystackStub struct {
	server     *http.Server
	Initialize func(request map[string]any) map[string]any
	mu         sync.Mutex
	requestLog []struct {
		Path string
		Body map[string]any
	}
}

func (s *NodeStack) startPaystackStub(t *testing.T) int {
	t.Helper()
	port := freePortSilent()
	stub := &PaystackStub{}
	s.Paystack = stub

	mux := http.NewServeMux()
	mux.HandleFunc("/transaction/initialize", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		stub.mu.Lock()
		stub.requestLog = append(stub.requestLog, struct {
			Path string
			Body map[string]any
		}{"/transaction/initialize", req})
		stub.mu.Unlock()

		var resp map[string]any
		if stub.Initialize != nil {
			resp = stub.Initialize(req)
		} else {
			resp = map[string]any{
				"status":  true,
				"message": "Authorization URL created",
				"data": map[string]any{
					"authorization_url": "https://checkout.paystack.test/pay/abc123",
					"access_code":       "ACCESS_test",
					"reference":         "ref_" + fmt.Sprint(time.Now().UnixNano()),
				},
			}
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("/transferrecipient", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		stub.mu.Lock()
		stub.requestLog = append(stub.requestLog, struct {
			Path string
			Body map[string]any
		}{"/transferrecipient", req})
		stub.mu.Unlock()

		writeJSON(w, map[string]any{
			"status":  true,
			"message": "Transfer recipient created",
			"data": map[string]any{
				"recipient_code": "RCPT_test_001",
				"type":           "momo",
			},
		})
	})
	mux.HandleFunc("/transfer", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		stub.mu.Lock()
		stub.requestLog = append(stub.requestLog, struct {
			Path string
			Body map[string]any
		}{"/transfer", req})
		stub.mu.Unlock()

		writeJSON(w, map[string]any{
			"status":  true,
			"message": "Transfer successful",
			"data": map[string]any{
				"status":        "success",
				"transfer_code": "TRF_test_001",
			},
		})
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	stub.server = &http.Server{Addr: addr, Handler: mux}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("paystack stub listen: %v", err)
	}
	go func() { _ = stub.server.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = stub.server.Shutdown(ctx)
	})
	return port
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
