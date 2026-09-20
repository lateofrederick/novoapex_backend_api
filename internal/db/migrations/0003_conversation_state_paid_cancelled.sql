-- 0003_conversation_state_paid_cancelled: extend the conversation state
-- machine for the event-driven checkout lifecycle (event-outbox decoupling).
--
-- New edges (see internal/domain/state.go ValidTransitions):
--   INVOICING -> PAID       on payment success (payment-events)
--   INVOICING -> CANCELLED  on failed / expired checkout
--   PAID | CANCELLED -> BROWSING  explicit reorder reset
--
-- ADD VALUE appends to the enum's end (order here does not matter — code never
-- relies on enum ordering). The new labels are only consumed by later code,
-- never referenced in this migration, so running inside the migration
-- transaction is safe on PostgreSQL 12+.
ALTER TYPE "ConversationState" ADD VALUE IF NOT EXISTS 'PAID';
ALTER TYPE "ConversationState" ADD VALUE IF NOT EXISTS 'CANCELLED';
