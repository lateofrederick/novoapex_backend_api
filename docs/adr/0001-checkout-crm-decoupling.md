# ADR 0001 — Checkout/CRM decoupling with a transactional outbox

- Status: Accepted
- Date: 2026-09-16

## Context

Order confirmation, payment-link generation, profile-stats and follow-up
scheduling were all fused inside the CRM materialiser's
`crmHandleOrderLedger`. The conversation state machine never actually gated
order creation, and the only dedup was the order's `idempotency_key` (the
source WhatsApp message id, unique per message). Result: when the LLM re-emitted
`order_confirmed=true` on a follow-up after an order was already paid, a second
order and payment link were minted for the same conversation.

Separating "payment" from "CRM" was the requested direction, but payment cannot
run before the order exists: the Paystack reference *is* the order id, and the
amount comes from resolved line items. The real split is therefore *checkout*
(order + state) vs *CRM enrichment*, with payment/stats/follow-ups as reactions
to the order.

## Decision

1. **Checkout owns order creation.** A new `checkout` job creates the order,
   its items and the conditional stock decrement, flips the conversation
   `BROWSING/CHECKOUT -> INVOICING` (compare-and-set), and emits an
   `order.created` event — all in one transaction. The CAS transition is the
   one-order-at-a-time guard that fixes the duplicate-order bug.

2. **Events, not direct calls, fan out the order's side effects.** A
   transactional outbox (`domain_events` table) is written in the same
   transaction as the state change, so an event can never exist without its
   order. A dispatcher (`LISTEN/NOTIFY` + poll backstop) publishes PENDING
   events to static subscribers:
   - `order.created` -> payment-init, profile-stats, follow-up schedule;
   - `order.cancelled` -> restock.

3. **Consumers are at-least-once and idempotent** on a natural marker:
   payment-init on `orders.payment_url` (first-write-wins), profile-stats on
   `orders.stats_recorded_at`, follow-up schedule on an existence check, restock
   on `orders.stock_restored_at`.

4. **State machine closes the loop.** Added `PAID` and `CANCELLED`:
   `INVOICING -> PAID` on payment success, `INVOICING -> CANCELLED` on failure
   or expiry (atomic with the cancel + `order.cancelled` event), and
   `PAID/CANCELLED -> BROWSING` for an explicit reorder.

## Consequences

- The duplicate-order bug is impossible: `INVOICING` can't transition back to
  an orderable state, and the CAS transition is atomic with the order insert.
- Order creation and CRM enrichment fail independently; a profile-builder
  failure can no longer block a checkout.
- More moving parts: an outbox table, a dispatcher, and four consumers instead
  of one fused handler. This is offset by each unit being small and independently
  retryable.
- At-least-once delivery means every consumer must remain idempotent; the
  markers above are the contract. New consumers must add a marker or accept a
  documented dedup key.
