-- The counter is transactional, not a sequence: its row lock is held until
-- commit. A later tenant position cannot commit before an earlier position.
CREATE TABLE raddle_event_tenants (
    tenant TEXT PRIMARY KEY CHECK (octet_length(tenant) BETWEEN 1 AND 128),
    position BIGINT NOT NULL DEFAULT 0 CHECK (position >= 0)
);

CREATE TABLE raddle_event_streams (
    tenant TEXT NOT NULL REFERENCES raddle_event_tenants(tenant),
    stream_id UUID NOT NULL CHECK (stream_id <> '00000000-0000-0000-0000-000000000000'),
    aggregate_contract TEXT NOT NULL CHECK (octet_length(aggregate_contract) BETWEEN 1 AND 128),
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    PRIMARY KEY (tenant, stream_id)
);

CREATE TABLE raddle_events (
    tenant TEXT NOT NULL,
    stream_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    tenant_position BIGINT NOT NULL CHECK (tenant_position > 0),
    event_id UUID NOT NULL CHECK (event_id <> '00000000-0000-0000-0000-000000000000'),
    envelope BYTEA NOT NULL CHECK (octet_length(envelope) BETWEEN 1 AND 1048576),
    PRIMARY KEY (tenant, stream_id, revision),
    UNIQUE (tenant, tenant_position),
    UNIQUE (tenant, event_id),
    FOREIGN KEY (tenant, stream_id) REFERENCES raddle_event_streams(tenant, stream_id)
);

CREATE TABLE raddle_event_checkpoints (
    projection TEXT NOT NULL CHECK (octet_length(projection) BETWEEN 1 AND 128),
    tenant TEXT NOT NULL,
    stream_id UUID NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    PRIMARY KEY (projection, tenant, stream_id),
    FOREIGN KEY (tenant, stream_id) REFERENCES raddle_event_streams(tenant, stream_id)
);
