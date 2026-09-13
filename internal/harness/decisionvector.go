// Turn decision vectors (T8.17/T8.18/T8.24): summarise one conversation
// turn's durable effects — {intent, stateBefore->stateAfter, escalatedFlag(+
// reason), imageIDsSent, crmSignalsEmitted, replyLenBucket} — into a value
// tests can assert against directly, without depending on free-text reply
// wording.
package harness

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

// TurnDecision is one turn's decision vector. Free-text reply content is
// deliberately reduced to ReplyLenBucket.
type TurnDecision struct {
	Intent           string   `json:"intent"`
	StateTransition  string   `json:"state_transition"`
	Escalated        bool     `json:"escalated"`
	EscalationReason string   `json:"escalation_reason,omitempty"`
	ImageIDsSent     []string `json:"image_ids_sent"`
	CRMSignals       string   `json:"crm_signals"`
	ReplyLenBucket   string   `json:"reply_len_bucket"`
}

// TurnSnapshot is the pre-turn baseline used to attribute new rows.
type TurnSnapshot struct {
	BusinessID    string
	CustomerPhone string
	OutboundCount int64
	OrderCount    int64
	State         string
}

// SnapshotTranscriptTurn captures the per-business baseline before a turn.
func SnapshotTranscriptTurn(db *sql.DB, businessID, customerPhone string) (TurnSnapshot, error) {
	snap := TurnSnapshot{BusinessID: businessID, CustomerPhone: customerPhone}
	err := db.QueryRow(`SELECT COALESCE(state::text,''), COALESCE(is_escalated_to_human,false)
		FROM conversations WHERE business_id = $1 AND customer_phone = $2`,
		businessID, customerPhone).Scan(&snap.State, new(bool))
	if err != nil && err != sql.ErrNoRows {
		return snap, fmt.Errorf("transcript: snapshot state: %w", err)
	}
	if snap.State == "" {
		snap.State = "LEAD" // a brand-new conversation enters its first turn at LEAD
	}
	if err := db.QueryRow(trnOutboundCountSQL, businessID, customerPhone).Scan(&snap.OutboundCount); err != nil {
		return snap, fmt.Errorf("transcript: snapshot outbound count: %w", err)
	}
	if err := db.QueryRow(trnOrderCountSQL, businessID, customerPhone).Scan(&snap.OrderCount); err != nil {
		return snap, fmt.Errorf("transcript: snapshot order count: %w", err)
	}
	return snap, nil
}

const (
	trnConvBase         = `(SELECT id FROM conversations WHERE business_id = $1 AND customer_phone = $2)`
	trnOutboundCountSQL = `SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = ` + trnConvBase
	trnOrderCountSQL    = `SELECT COUNT(*) FROM orders WHERE conversation_id = ` + trnConvBase
)

// CollectTurnDecision extracts the post-turn vector given the pre-turn
// snapshot. tokens maps full product IDs to stable scenario tokens ("p0")
// so comparisons never depend on per-business UUIDs.
func CollectTurnDecision(db *sql.DB, before TurnSnapshot, intent string, tokens map[string]string) (TurnDecision, TurnSnapshot, error) {
	after, err := SnapshotTranscriptTurn(db, before.BusinessID, before.CustomerPhone)
	if err != nil {
		return TurnDecision{}, after, err
	}
	dec := TurnDecision{
		Intent:          intent,
		StateTransition: before.State + "->" + after.State,
		ImageIDsSent:    []string{},
		CRMSignals:      "-",
		ReplyLenBucket:  "none",
	}

	var escalated bool
	var reason *string
	if err := db.QueryRow(`SELECT is_escalated_to_human, escalation_reason FROM conversations
		WHERE business_id = $1 AND customer_phone = $2`,
		before.BusinessID, before.CustomerPhone).Scan(&escalated, &reason); err == nil {
		dec.Escalated = escalated
		if reason != nil {
			dec.EscalationReason = *reason
		}
	} else if err != sql.ErrNoRows {
		return dec, after, fmt.Errorf("transcript: read escalation flag: %w", err)
	}

	rows, err := db.Query(`SELECT message_type, COALESCE(text_content,''),
	       COALESCE(product_id,'')
		FROM outbound_messages WHERE conversation_id = `+trnConvBase+`
		ORDER BY created_at ASC OFFSET $3`, before.BusinessID, before.CustomerPhone, before.OutboundCount)
	if err != nil {
		return dec, after, fmt.Errorf("transcript: new outbound rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	maxLen := 0
	for rows.Next() {
		var kind, text, productID string
		if err := rows.Scan(&kind, &text, &productID); err != nil {
			return dec, after, fmt.Errorf("transcript: scan outbound rows: %w", err)
		}
		switch kind {
		case "text":
			if n := len(text); n > maxLen {
				maxLen = n
			}
		case "image":
			tok, ok := tokens[productID]
			if !ok {
				tok = "img:" + productID
			}
			dec.ImageIDsSent = append(dec.ImageIDsSent, tok)
		}
	}
	if err := rows.Err(); err != nil {
		return dec, after, fmt.Errorf("transcript: iterate outbound rows: %w", err)
	}
	dec.ReplyLenBucket = trnLenBucket(maxLen)

	newOrders := after.OrderCount - before.OrderCount
	crm, err := trnCRMVector(db, before.BusinessID, before.CustomerPhone, newOrders)
	if err != nil {
		return dec, after, err
	}
	dec.CRMSignals = crm
	return dec, after, nil
}

// trnCRMVector summarises durable CRM effects into one comparison token:
// orders=<n>,total=<sum>|sentiment=…|area=…|name=…|prefs=<n> ("-" when all
// absent).
func trnCRMVector(db *sql.DB, businessID, customerPhone string, newOrders int64) (string, error) {
	var parts []string
	if newOrders > 0 {
		rows, err := db.Query(`SELECT total_amount::text FROM orders
			WHERE conversation_id = `+trnConvBase+` ORDER BY created_at DESC LIMIT $3`,
			businessID, customerPhone, newOrders)
		if err != nil {
			return "", fmt.Errorf("transcript: order totals: %w", err)
		}
		defer func() { _ = rows.Close() }()
		sum := decimal.Zero
		count := int64(0)
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				return "", fmt.Errorf("transcript: scan order totals: %w", err)
			}
			d, err := decimal.NewFromString(raw)
			if err != nil {
				return "", fmt.Errorf("transcript: parse order total %s: %w", raw, err)
			}
			sum = sum.Add(d)
			count++
		}
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("transcript: iterate order totals: %w", err)
		}
		parts = append(parts, fmt.Sprintf("orders=%d,total=%s", count, sum.StringFixed(2)))
	}

	var name, sentiment, area sql.NullString
	var prefs sql.NullInt64
	err := db.QueryRow(`SELECT c.name, p.sentiment, p.delivery_area,
	       CASE WHEN p.customer_id IS NULL THEN 0 ELSE jsonb_array_length(p.preferences) END
		FROM customers c LEFT JOIN customer_profiles p ON p.customer_id = c.id
		WHERE c.business_id = $1 AND c.phone = $2`, businessID, customerPhone).
		Scan(&name, &sentiment, &area, &prefs)
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("transcript: crm profile read: %w", err)
	}
	if sentiment.Valid && sentiment.String != "" {
		parts = append(parts, "sentiment="+sentiment.String)
	}
	if area.Valid && area.String != "" {
		parts = append(parts, "area="+area.String)
	}
	if name.Valid && name.String != "" {
		parts = append(parts, "name="+name.String)
	}
	if prefs.Valid && prefs.Int64 > 0 {
		parts = append(parts, fmt.Sprintf("prefs=%d", prefs.Int64))
	}
	if len(parts) == 0 {
		return "-", nil
	}
	return strings.Join(parts, "|"), nil
}

func trnLenBucket(n int) string {
	switch {
	case n <= 0:
		return "none"
	case n < 80:
		return "s"
	case n < 160:
		return "m"
	case n < 320:
		return "l"
	default:
		return "xl"
	}
}

// DiffDecisions compares two recorded decision vectors element-wise; an empty
// result means they are IDENTICAL for every turn.
func DiffDecisions(want, got []TurnDecision) []string {
	var out []string
	if len(want) != len(got) {
		out = append(out, fmt.Sprintf("turn count: want %d, got %d", len(want), len(got)))
	}
	n := len(want)
	if len(got) < n {
		n = len(got)
	}
	for i := 0; i < n; i++ {
		w, g := want[i], got[i]
		if w.Intent != g.Intent {
			out = append(out, fmt.Sprintf("turn %d.intent: want %s, got %s", i, w.Intent, g.Intent))
		}
		if w.StateTransition != g.StateTransition {
			out = append(out, fmt.Sprintf("turn %d.state_transition: want %s, got %s", i, w.StateTransition, g.StateTransition))
		}
		if w.Escalated != g.Escalated || w.EscalationReason != g.EscalationReason {
			out = append(out, fmt.Sprintf("turn %d.escalated: want %v/%s, got %v/%s",
				i, w.Escalated, w.EscalationReason, g.Escalated, g.EscalationReason))
		}
		if strings.Join(w.ImageIDsSent, ",") != strings.Join(g.ImageIDsSent, ",") {
			out = append(out, fmt.Sprintf("turn %d.image_ids_sent: want [%s], got [%s]",
				i, strings.Join(w.ImageIDsSent, ","), strings.Join(g.ImageIDsSent, ",")))
		}
		if w.CRMSignals != g.CRMSignals {
			out = append(out, fmt.Sprintf("turn %d.crm_signals: want %s, got %s", i, w.CRMSignals, g.CRMSignals))
		}
		if w.ReplyLenBucket != g.ReplyLenBucket {
			out = append(out, fmt.Sprintf("turn %d.reply_len_bucket: want %s, got %s", i, w.ReplyLenBucket, g.ReplyLenBucket))
		}
	}
	return out
}
