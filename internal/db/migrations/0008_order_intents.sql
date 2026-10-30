-- 0008_order_intents: Ephemeral Cart / Order Intent lifecycle.
-- Guarantees that an order can only be created from an explicitly staged
-- intent awaiting confirmation. Prevents accidental duplicate orders and
-- stale re-orders across completed transactions.

CREATE TYPE "OrderIntentStatus" AS ENUM (
    'AWAITING_CONFIRMATION',
    'CONFIRMED',
    'CONVERTED',
    'EXPIRED',
    'CANCELLED'
);

CREATE TABLE order_intents (
    id                  TEXT NOT NULL,
    business_id         TEXT NOT NULL,
    customer_id         TEXT NOT NULL,
    conversation_id     TEXT NOT NULL,
    status              "OrderIntentStatus" NOT NULL DEFAULT 'AWAITING_CONFIRMATION',
    total_amount        NUMERIC(14,2) NOT NULL,
    currency            TEXT NOT NULL DEFAULT 'GHS',
    fulfillment_type    "FulfillmentType" NOT NULL DEFAULT 'DELIVERY',
    location_id         TEXT,
    delivery_address    TEXT,
    items               JSONB NOT NULL,
    order_id            TEXT,
    source_message_id   TEXT,
    expires_at          TIMESTAMP(3) NOT NULL,
    created_at          TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT order_intents_pkey PRIMARY KEY (id),
    CONSTRAINT order_intents_business_id_fkey FOREIGN KEY (business_id) REFERENCES businesses (id) ON DELETE CASCADE,
    CONSTRAINT order_intents_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE,
    CONSTRAINT order_intents_conversation_id_fkey FOREIGN KEY (conversation_id) REFERENCES conversations (id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX order_intents_active_conversation_key
    ON order_intents (conversation_id)
    WHERE status = 'AWAITING_CONFIRMATION';

CREATE INDEX order_intents_conversation_id_status_idx
    ON order_intents (conversation_id, status);

CREATE INDEX order_intents_business_id_idx
    ON order_intents (business_id);

CREATE INDEX order_intents_expires_at_idx
    ON order_intents (expires_at);
