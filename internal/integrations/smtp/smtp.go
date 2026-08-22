// Package smtp ports libs/common/src/email/email.service.ts's nodemailer
// transport onto net/smtp. It speaks just enough ESMTP for the Zoho relay and
// the test harness stub: EHLO, optional AUTH PLAIN (nodemailer sends
// credentials opportunistically whenever auth is configured — even over a
// plaintext link), MAIL FROM / RCPT TO / DATA, QUIT.
//
// TLS policy mirrors nodemailer's `secure: port === 465`
// (email.service.ts:22): implicit TLS from the first byte on 465; plaintext
// with opportunistic STARTTLS otherwise.
package smtp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"strconv"
	"strings"
	"time"
)

// Config mirrors the SMTP_* config surface the Node transporter is built
// from (email.service.ts:17-27).
type Config struct {
	Host string
	Port int
	User string
	Pass string
}

type mailHeaders struct {
	contentType string
}

// Option tweaks optional message headers.
type Option func(*mailHeaders)

// WithContentType overrides the body's Content-Type (default text/plain;
// charset=utf-8) — e.g. multipart/alternative boundaries.
func WithContentType(contentType string) Option {
	return func(h *mailHeaders) { h.contentType = contentType }
}

// SendMail delivers one message. from/to are bare addresses; any display-name
// decoration belongs in from (the caller mirrors nodemailer's `"NovoApex
// Auth" <user>` envelope). The body is the bare payload — RFC 5322 headers
// are rendered here.
func SendMail(cfg Config, from, to, subject, body string, opts ...Option) error {
	h := mailHeaders{contentType: "text/plain; charset=utf-8"}
	for _, opt := range opts {
		opt(&h)
	}

	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))

	var conn net.Conn
	var err error
	if cfg.Port == 465 {
		conn, err = tls.Dial("tcp", addr, &tls.Config{ServerName: cfg.Host})
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("smtp: dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	c, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		return fmt.Errorf("smtp: handshake %s: %w", addr, err)
	}

	// Implicit TLS already covers 465; other ports upgrade opportunistically.
	if ok, _ := c.Extension("STARTTLS"); cfg.Port != 465 && ok {
		if err := c.StartTLS(&tls.Config{ServerName: cfg.Host}); err != nil {
			return fmt.Errorf("smtp: STARTTLS: %w", err)
		}
	}

	// AUTH only when configured (harness stub path is AUTH-less).
	if cfg.User != "" {
		if err := c.Auth(&plainAuth{username: cfg.User, password: cfg.Pass}); err != nil {
			return fmt.Errorf("smtp: auth: %w", err)
		}
	}

	envelopeFrom := envelopeAddress(from)
	if err := c.Mail(envelopeFrom); err != nil {
		return fmt.Errorf("smtp: MAIL FROM: %w", err)
	}
	if err := c.Rcpt(to); err != nil {
		return fmt.Errorf("smtp: RCPT TO: %w", err)
	}

	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("smtp: DATA: %w", err)
	}
	if _, err := w.Write(renderMessage(from, to, subject, h.contentType, body)); err != nil {
		return fmt.Errorf("smtp: write body: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp: end data: %w", err)
	}

	return c.Quit()
}

// renderMessage emits the RFC 5322 entity with CRLF line endings.
func renderMessage(from, to, subject, contentType, body string) []byte {
	out := ""
	out += "From: " + from + "\r\n"
	out += "To: " + to + "\r\n"
	out += "Subject: " + subject + "\r\n"
	out += "Date: " + time.Now().Format(time.RFC1123Z) + "\r\n"
	out += "MIME-Version: 1.0\r\n"
	out += "Content-Type: " + contentType + "\r\n"
	out += "\r\n"
	out += body
	return []byte(out)
}

// envelopeAddress strips any display name ("Name" <addr> / Name <addr>) down
// to the bare address required by MAIL FROM.
func envelopeAddress(from string) string {
	if i := strings.LastIndexByte(from, '<'); i >= 0 {
		if j := strings.IndexByte(from[i:], '>'); j > 0 {
			return from[i+1 : i+j]
		}
	}
	return from
}

// plainAuth implements smtp.Auth for mechanism PLAIN. Unlike net/smtp's
// smtp.PlainAuth it does not refuse plaintext connections — nodemailer sends
// AUTH whenever credentials exist (opportunistic TLS is the operator's
// choice), and the harness stub authenticates over cleartext.
type plainAuth struct {
	username, password string
}

func (a *plainAuth) Start(*smtp.ServerInfo) (string, []byte, error) {
	// net/smtp base64-encodes the initial response itself, so return raw
	// NUL-separated identity bytes.
	resp := []byte("\x00" + a.username + "\x00" + a.password)
	return "PLAIN", resp, nil
}

func (*plainAuth) Next(_ []byte, more bool) ([]byte, error) {
	if more {
		return nil, errors.New("smtp: unexpected server challenge")
	}
	return nil, nil
}
