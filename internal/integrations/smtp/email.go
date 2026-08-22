package smtp

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// OTP email contract from libs/common/src/email/email.service.ts:30-46:
//   - From: `"NovoApex Auth" <${SMTP_USER}>` (line 35)
//   - Subject: 'Your NovoApex Login Code' (line 37)
//   - text: `Your NovoApex login code is: ${code}. It will expire in 5
//     minutes.` (line 38)
//   - html: `<p>Your NovoApex login code is: <strong>${code}</strong>.</p>
//     <p>It will expire in 5 minutes.</p>` (line 39)
//
// nodemailer renders {text, html} as a multipart/alternative entity; the same
// structure is produced here.
func SendOTPEmail(cfg Config, to, code string) error {
	from := fmt.Sprintf(`"NovoApex Auth" <%s>`, cfg.User)
	subject := "Your NovoApex Login Code"

	text := fmt.Sprintf("Your NovoApex login code is: %s. It will expire in 5 minutes.", code)
	html := fmt.Sprintf(
		"<p>Your NovoApex login code is: <strong>%s</strong>.</p><p>It will expire in 5 minutes.</p>",
		code)

	body, contentType := buildAlternative(text, html)
	return SendMail(cfg, from, to, subject, body, WithContentType(contentType))
}

// buildAlternative produces a multipart/alternative entity (text part first,
// html second — nodemailer's ordering for {text, html}).
func buildAlternative(text, html string) (string, string) {
	boundary := "----novoapex-" + randomHex(12)

	var b strings.Builder
	writePart := func(contentType, content string) {
		fmt.Fprintf(&b, "--%s\r\n", boundary)
		b.WriteString("Content-Type: " + contentType + "; charset=utf-8\r\n")
		b.WriteString("Content-Transfer-Encoding: 7bit\r\n")
		b.WriteString("\r\n")
		b.WriteString(content)
		b.WriteString("\r\n")
	}
	writePart("text/plain", text)
	writePart("text/html", html)
	fmt.Fprintf(&b, "--%s--\r\n", boundary)

	return b.String(), fmt.Sprintf(`multipart/alternative; boundary="%s"`, boundary)
}

func randomHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf) // crypto/rand.Read never fails on supported platforms
	return hex.EncodeToString(buf)
}
