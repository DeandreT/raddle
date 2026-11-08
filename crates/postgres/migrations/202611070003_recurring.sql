CREATE TABLE raddle_recurring (
    tenant text NOT NULL CHECK (octet_length(tenant) BETWEEN 1 AND 128),
    source text NOT NULL CHECK (octet_length(source) BETWEEN 1 AND 128),
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 128),
    incarnation uuid NOT NULL,
    definition_hash bytea NOT NULL,
    next_due timestamptz NOT NULL CHECK (isfinite(next_due) AND next_due <= '262142-12-31 23:59:59.999999+00'::timestamptz),
    interval_us bigint NOT NULL CHECK (interval_us BETWEEN 1000 AND 31536000000000),
    catch_up integer NOT NULL CHECK (catch_up BETWEEN 0 AND 256),
    envelope bytea NOT NULL CHECK (octet_length(envelope) <= 1048576),
    deliveries jsonb NOT NULL,
    cancelled boolean NOT NULL DEFAULT false,
    retry_after timestamptz CHECK (retry_after IS NULL OR isfinite(retry_after)),
    last_error text,
    PRIMARY KEY (tenant, source, name)
);
CREATE INDEX raddle_recurring_due ON raddle_recurring(next_due)
    WHERE NOT cancelled;
