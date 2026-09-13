-- 0002_outbound_send_order: a monotonic insertion sequence for outbound
-- messages.
--
-- Senders persist rows in the order the customer should receive them (product
-- image before its caption reply, reply before the payment link), but the
-- outbound queue is consumed concurrently. The consumer serialises sends per
-- conversation and delivers pending rows in seq order. created_at cannot serve
-- as the key: rows written in the same millisecond tie.
ALTER TABLE outbound_messages ADD COLUMN seq BIGINT GENERATED ALWAYS AS IDENTITY;

CREATE INDEX outbound_messages_conversation_id_status_seq_idx
    ON outbound_messages (conversation_id, status, seq);
