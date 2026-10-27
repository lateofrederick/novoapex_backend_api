-- 0007_device_tokens: stores Expo push notification tokens per vendor device.
-- Multiple devices per vendor/business are supported.
-- Tokens are upserted on launch and purged when unregistered or stale.

CREATE TABLE device_tokens (
    id              TEXT NOT NULL,
    business_id     TEXT NOT NULL,
    owner_phone     TEXT NOT NULL,
    expo_push_token TEXT NOT NULL,
    platform        TEXT NOT NULL,
    device_name     TEXT,
    created_at      TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at    TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT device_tokens_pkey PRIMARY KEY (id),
    CONSTRAINT device_tokens_business_id_fkey FOREIGN KEY (business_id) REFERENCES businesses (id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX device_tokens_expo_push_token_key ON device_tokens (expo_push_token);
CREATE INDEX device_tokens_business_id_idx ON device_tokens (business_id);
CREATE INDEX device_tokens_owner_phone_idx ON device_tokens (owner_phone);
