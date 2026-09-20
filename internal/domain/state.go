// Package domain hosts cross-stage pure business rules ported from the Node
// monorepo libs. This file is the exact port of
// libs/orchestrator/src/conversation-state.service.ts (T7.x CRM wave): the
// explicit transition table plus compare-and-set persistence.
package domain

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConversationState mirrors the Prisma ConversationState enum
// (conversation-state.service.ts:7-13).
type ConversationState string

const (
	StateLead      ConversationState = "LEAD"
	StateBrowsing  ConversationState = "BROWSING"
	StateCheckout  ConversationState = "CHECKOUT"
	StateInvoicing ConversationState = "INVOICING"
	StatePaid      ConversationState = "PAID"
	StateCancelled ConversationState = "CANCELLED"
	StateSupport   ConversationState = "SUPPORT"
	StateEscalated ConversationState = "ESCALATED"
)

// ValidTransitions is the explicit transition table
// (conversation-state.service.ts:23-30), extended for the event-driven
// checkout lifecycle. Each key maps to the set of states it can transition TO;
// any pair not listed is illegal. ESCALATED is terminal — only a human action
// outside this service can un-escalate a conversation.
//
// Go-side extension (not part of the TS source): PAID and CANCELLED close the
// checkout loop. INVOICING reaches them on payment success/failure; both reset
// to BROWSING for an explicit reorder. Order creation remains legal only from
// CHECKOUT, which preserves the one-order-at-a-time invariant.
var ValidTransitions = map[ConversationState][]ConversationState{
	StateLead:      {StateBrowsing, StateCheckout, StateSupport, StateEscalated},
	StateBrowsing:  {StateCheckout, StateSupport, StateInvoicing, StateEscalated},
	StateCheckout:  {StateInvoicing, StateSupport, StateEscalated},
	StateInvoicing: {StatePaid, StateCancelled, StateSupport, StateEscalated},
	StatePaid:      {StateBrowsing, StateSupport, StateEscalated},
	StateCancelled: {StateBrowsing, StateSupport, StateEscalated},
	StateSupport:   {StateBrowsing, StateLead, StateEscalated},
	StateEscalated: {}, // terminal
}

// ValidateTransition reports whether from → to is listed in ValidTransitions.
// Unknown `from` states reject everything, exactly like the source's
// `!allowed || !allowed.includes(newState)` guard
// (conversation-state.service.ts:65-74).
func ValidateTransition(from, to ConversationState) bool {
	for _, allowed := range ValidTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// Transition attempts a state change on one conversation with compare-and-set
// semantics (conversation-state.service.ts:59-101): legality is validated
// first, then a single conditional UPDATE flips the row only while it still
// sits in `from`. A false return is a REJECTION (illegal move or stale
// current state), never an error — racing queues are expected to lose races.
//
// Port note: the source goes through Prisma updateMany({where:{id,state}},
// {data:{state}}) whose count==0 branch means "stale"; RowsAffected()==0 is
// the identical signal here. updated_at is bumped because Prisma's @updatedAt
// fires on updateMany too.
func Transition(ctx context.Context, pool *pgxpool.Pool, conversationID string, from, to ConversationState) (bool, error) {
	return transition(ctx, pool, conversationID, from, to)
}

// TransitionTx is Transition scoped to an open transaction, so the state flip
// is atomic with whatever write it guards (e.g. order creation). Returns the
// same (ok, error) contract; a false return is a rejection, never an error.
func TransitionTx(ctx context.Context, tx pgx.Tx, conversationID string, from, to ConversationState) (bool, error) {
	return transition(ctx, tx, conversationID, from, to)
}

// TransitionToTx flips a conversation to `to` from ANY state in `froms`,
// atomically inside tx. Every from->to pair must be legal; the single UPDATE
// admits whichever source currently holds. Returns false when the conversation
// sits in none of `froms` (a rejection, never an error). This is the guard for
// writers that may legally run from more than one state (e.g. checkout may
// confirm an order from BROWSING or CHECKOUT).
func TransitionToTx(ctx context.Context, tx pgx.Tx, conversationID string, froms []ConversationState, to ConversationState) (bool, error) {
	if len(froms) == 0 {
		return false, nil
	}
	for _, from := range froms {
		if !ValidateTransition(from, to) {
			return false, nil
		}
	}
	strs := make([]string, len(froms))
	for i, f := range froms {
		strs[i] = string(f)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE conversations SET state = $1, updated_at = CURRENT_TIMESTAMP
		 WHERE id = $2 AND state::text = ANY($3)`,
		string(to), conversationID, strs)
	if err != nil {
		return false, fmt.Errorf("domain: transition to %s for conversation %s: %w", to, conversationID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// execer is satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func transition(ctx context.Context, db execer, conversationID string, from, to ConversationState) (bool, error) {
	if !ValidateTransition(from, to) {
		return false, nil
	}

	tag, err := db.Exec(ctx,
		`UPDATE conversations SET state = $1, updated_at = CURRENT_TIMESTAMP
		 WHERE id = $2 AND state = $3`,
		string(to), conversationID, string(from))
	if err != nil {
		return false, fmt.Errorf("domain: transition %s -> %s for conversation %s: %w", from, to, conversationID, err)
	}
	return tag.RowsAffected() == 1, nil
}
