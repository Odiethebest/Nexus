package mailer

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"strings"
)

// Config holds SMTP connection and auth settings.
type Config struct {
	Host     string // e.g. smtp.resend.com
	Port     string // 587 for STARTTLS, 465 for TLS
	Username string
	Password string
	From     string // e.g. Nexus <no-reply@example.com>
}

// Mailer sends transactional email over SMTP.
// Port 587 uses STARTTLS; port 465 opens a TLS connection from the start.
type Mailer struct {
	cfg Config
}

// New returns a Mailer. Call Send to deliver messages.
func New(cfg Config) *Mailer {
	return &Mailer{cfg: cfg}
}

// Send delivers a plain-text email to a single recipient.
func (m *Mailer) Send(to, subject, body string) error {
	msg := buildMessage(m.cfg.From, to, subject, body)
	addr := net.JoinHostPort(m.cfg.Host, m.cfg.Port)
	auth := smtp.PlainAuth("", m.cfg.Username, m.cfg.Password, m.cfg.Host)

	if m.cfg.Port == "465" {
		return m.sendTLS(addr, auth, to, msg)
	}
	// port 587 (or any other) — use STARTTLS via smtp.SendMail
	return smtp.SendMail(addr, auth, m.cfg.From, []string{to}, msg)
}

// sendTLS dials with TLS from the start (implicit TLS / SMTPS, port 465).
func (m *Mailer) sendTLS(addr string, auth smtp.Auth, to string, msg []byte) error {
	tlsCfg := &tls.Config{ServerName: m.cfg.Host}
	conn, err := tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		return fmt.Errorf("mailer: tls dial: %w", err)
	}

	client, err := smtp.NewClient(conn, m.cfg.Host)
	if err != nil {
		return fmt.Errorf("mailer: smtp client: %w", err)
	}
	defer client.Close()

	if err := client.Auth(auth); err != nil {
		return fmt.Errorf("mailer: auth: %w", err)
	}
	if err := client.Mail(m.cfg.From); err != nil {
		return fmt.Errorf("mailer: MAIL FROM: %w", err)
	}
	if err := client.Rcpt(to); err != nil {
		return fmt.Errorf("mailer: RCPT TO: %w", err)
	}

	wc, err := client.Data()
	if err != nil {
		return fmt.Errorf("mailer: DATA: %w", err)
	}
	if _, err := wc.Write(msg); err != nil {
		return fmt.Errorf("mailer: write body: %w", err)
	}
	return wc.Close()
}

func buildMessage(from, to, subject, body string) []byte {
	headers := strings.Join([]string{
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
		fmt.Sprintf("From: %s", from),
		fmt.Sprintf("To: %s", to),
		fmt.Sprintf("Subject: %s", subject),
	}, "\r\n")
	return []byte(headers + "\r\n\r\n" + body)
}
