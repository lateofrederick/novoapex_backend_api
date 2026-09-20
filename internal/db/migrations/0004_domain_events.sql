-- 0004_domain_events: transactional outbox for domain events (order.created,
-- order.cancelled, ...). Producers insert a row in the SAME transaction as the
-- state change it describes; a dispatcher publishes PENDING rows to asynq
-- subscribers and marks them PUBLISHED (or DEAD after repeated failures).
--
-- aggregate_type + aggregate_id identify the entity the event belongs to
-- ("order" / orderID); event_type names what happened. The unique index on
-- (aggregate_type, aggregate_id, event_type) is the emission backstop — at most
-- one event of a given type per aggregate, ever.
CREATE TYPE "EventStatus" AS ENUM ('PENDING', 'PUBLISHED', 'DEAD');

CREATE TABLE domain_events (
    id             TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    occurred_at    TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at   TIMESTAMP(3),
    attempts       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    status         "EventStatus" NOT NULL DEFAULT 'PENDING',
    CONSTRAINT domain_events_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX domain_events_aggregate_type_id_event_type_key
    ON domain_events (aggregate_type, aggregate_id, event_type);

CREATE INDEX domain_events_status_occurred_at_idx
    ON domain_events (status, occurred_at);
