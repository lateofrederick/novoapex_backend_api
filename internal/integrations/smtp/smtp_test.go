package smtp_test

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	smtplib "github.com/novoapex/novoapex-backend-api/internal/integrations/smtp"
)

// s3selfSignedCert builds an in-memory certificate for the implicit-TLS test.
func s3selfSignedCert() (*tls.Certificate, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

func s3tlsServer(raw net.Conn, cert *tls.Certificate) *tls.Conn {
	return tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{*cert}})
}

// s3SMTPStub mirrors the harness SMTPStub's protocol surface (plaintext,
// AUTH-accepting, DATA capture) so SendMail is proven against the same
// conversation shape the node stack tests use.
type s3SMTPStub struct {
	listener net.Listener
	mu       sync.Mutex
	messages []string
	dialogs  []string
}

func s3StartSMTPStub(t *testing.T) *s3SMTPStub {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	stub := &s3SMTPStub{listener: ln}
	go stub.acceptLoop()
	t.Cleanup(func() { _ = ln.Close() })
	return stub
}

func (s *s3SMTPStub) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.serve(conn)
	}
}

func (s *s3SMTPStub) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	var dialog strings.Builder
	write := func(line string) {
		_, _ = io.WriteString(conn, line+"\r\n")
	}
	write("220 stub.smtp ESMTP")

	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	inData := false
	var data strings.Builder
	for sc.Scan() {
		line := sc.Text()
		dialog.WriteString("> " + line + "\n")
		if inData {
			if line == "." {
				s.mu.Lock()
				s.messages = append(s.messages, data.String())
				s.dialogs = append(s.dialogs, dialog.String())
				s.mu.Unlock()
				data.Reset()
				inData = false
				write("250 OK queued")
				continue
			}
			data.WriteString(line)
			data.WriteString("\n")
			continue
		}
		upper := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(upper, "EHLO"):
			write("250-stub.smtp")
			write("250-8BITMIME")
			write("250 OK")
		case strings.HasPrefix(upper, "HELO"),
			strings.HasPrefix(upper, "MAIL FROM"),
			strings.HasPrefix(upper, "RCPT TO"):
			write("250 OK")
		case strings.HasPrefix(upper, "AUTH"):
			write("235 2.7.0 Accepted")
		case upper == "DATA":
			inData = true
			write("354 End data with <CR><LF>.<CR><LF>")
		case upper == "QUIT":
			write("221 Bye")
			return
		default:
			write("250 OK")
		}
	}
}

func (s *s3SMTPStub) port() int { return s.listener.Addr().(*net.TCPAddr).Port }

func (s *s3SMTPStub) messagesSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.messages))
	copy(out, s.messages)
	return out
}

func TestS3_SendMail_PlainTextAUTHlessPath(t *testing.T) {
	stub := s3StartSMTPStub(t)

	cfg := smtplib.Config{Host: "127.0.0.1", Port: stub.port()} // no user: no AUTH
	err := smtplib.SendMail(cfg, "noreply@novoapex.test", "vendor@example.com",
		"Hello", "Body line one.\r\nBody line two.")
	if err != nil {
		t.Fatalf("SendMail: %v", err)
	}

	msgs := stub.messagesSnapshot()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	for _, want := range []string{
		"From: noreply@novoapex.test",
		"To: vendor@example.com",
		"Subject: Hello",
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=utf-8",
		"Body line one.",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("message missing %q\n---\n%s", want, m)
		}
	}
}

// The email OTP template must carry the exact strings from
// libs/common/src/email/email.service.ts.
func TestS3_SendOTPEmail_TemplateParity(t *testing.T) {
	stub := s3StartSMTPStub(t)

	cfg := smtplib.Config{Host: "127.0.0.1", Port: stub.port(), User: "auth@novoapex.test", Pass: "pw"}
	if err := smtplib.SendOTPEmail(cfg, "vendor@example.com", "123456"); err != nil {
		t.Fatalf("SendOTPEmail: %v", err)
	}

	msgs := stub.messagesSnapshot()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages", len(msgs))
	}
	m := msgs[0]
	wantSubstrings := []string{
		`From: "NovoApex Auth" <auth@novoapex.test>`,
		"To: vendor@example.com",
		"Subject: Your NovoApex Login Code",
		"multipart/alternative",
		"Your NovoApex login code is: 123456. It will expire in 5 minutes.",
		"<p>Your NovoApex login code is: <strong>123456</strong>.</p><p>It will expire in 5 minutes.</p>",
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(m, want) {
			t.Errorf("missing %q\n---\n%s", want, m)
		}
	}
}

func TestS3_SendMail_TLSWhen465(t *testing.T) {
	// The server accepts only TLS, so a completed handshake plus a readable
	// SMTP dialog proves the client used tls.Dial on 465 — and that AUTH
	// rides inside it.
	cert, err := s3selfSignedCert()
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	// Implicit TLS is keyed on port 465 exactly, so bind the stub there
	// (macOS allows unprivileged binds); skip when taken.
	ln, err := net.Listen("tcp", "127.0.0.1:465")
	if err != nil {
		t.Skipf("cannot bind 127.0.0.1:465 here: %v", err)
	}
	defer func() { _ = ln.Close() }()

	captured := make(chan string, 1)
	go func() {
		rawConn, err := ln.Accept()
		if err != nil {
			captured <- ""
			return
		}
		defer func() { _ = rawConn.Close() }()
		_ = rawConn.SetDeadline(time.Now().Add(5 * time.Second))
		conn := s3tlsServer(rawConn, cert)

		var dialog strings.Builder
		write := func(line string) { _, _ = io.WriteString(conn, line+"\r\n") }
		if err := conn.Handshake(); err != nil {
			captured <- "HANDSHAKE FAIL: " + err.Error()
			return
		}
		write("220 stub.smtp ESMTP")

		sc := bufio.NewScanner(conn)
		for sc.Scan() {
			line := sc.Text()
			dialog.WriteString(line + "\n")
			switch {
			case strings.HasPrefix(line, "EHLO"):
				write("250-stub.smtp")
				write("250 OK")
			case strings.HasPrefix(line, "MAIL FROM"), strings.HasPrefix(line, "RCPT TO"):
				write("250 OK")
			case strings.HasPrefix(line, "AUTH"):
				write("235 2.7.0 Accepted")
				captured <- dialog.String()
				return
			}
		}
		captured <- dialog.String()
	}()

	port := ln.Addr().(*net.TCPAddr).Port
	cfg := smtplib.Config{Host: "127.0.0.1", Port: port, User: "u", Pass: "p"}
	// The server hangs up after AUTH; a subsequent client error is expected
	// and irrelevant — the captured dialog is the assertion target.
	_ = smtplib.SendMail(cfg, "a@b.c", "d@e.f", "S", "B")

	select {
	case dialog := <-captured:
		if dialog == "" || strings.HasPrefix(dialog, "HANDSHAKE FAIL") {
			t.Fatalf("TLS dialog failed: %q", dialog)
		}
		if !strings.Contains(dialog, "EHLO 127.0.0.1") {
			t.Fatalf("missing EHLO over implicit TLS:\n%s", dialog)
		}
		wantAuth := "AUTH PLAIN " + base64.StdEncoding.EncodeToString([]byte("\x00u\x00p"))
		if !strings.Contains(dialog, wantAuth) {
			t.Fatalf("missing/opaque AUTH PLAIN:\n%s\nwant %q", dialog, wantAuth)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for TLS connection")
	}
}

func TestS3_EnvelopeAddressStripsDisplayName(t *testing.T) {
	// Indirect check via a stub: MAIL FROM must be the bare address even when
	// from carries a display name.
	stub := s3StartSMTPStub(t)
	cfg := smtplib.Config{Host: "127.0.0.1", Port: stub.port()}
	from := `"NovoApex Auth" <auth@novoapex.test>`
	if err := smtplib.SendMail(cfg, from, "to@x.y", "S", "B"); err != nil {
		t.Fatal(err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.dialogs) != 1 || !strings.Contains(stub.dialogs[0], "MAIL FROM:<auth@novoapex.test>") {
		t.Fatalf("envelope MAIL FROM wrong:\n%s", stub.dialogs)
	}
}
