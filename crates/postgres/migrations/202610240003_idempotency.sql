CREATE TABLE raddle_idempotency (
    tenant text NOT NULL,
    source text NOT NULL,
    key text NOT NULL,
    request_hash bytea NOT NULL,
    message_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant,source,key)
);
