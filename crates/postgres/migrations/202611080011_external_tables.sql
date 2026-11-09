CREATE TABLE raddle_external_sources (
    tenant text NOT NULL,
    source text NOT NULL,
    binding text NOT NULL UNIQUE,
    definition bytea NOT NULL,
    cursor uuid,
    high_water uuid,
    PRIMARY KEY (tenant, source)
);

CREATE TABLE raddle_external_inputs (
    sequence bigserial UNIQUE NOT NULL,
    tenant text NOT NULL,
    source text NOT NULL,
    input_id uuid NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint)=32),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash)=32),
    first_seen timestamptz NOT NULL,
    diagnostic_time text CHECK (octet_length(diagnostic_time)<=64),
    status text NOT NULL CHECK (status IN ('observed','quarantined','accepted','replayed','duplicate','resolved')),
    code text CHECK (octet_length(code)<=64),
    body text CHECK (octet_length(convert_to(body,'UTF8'))<=1048576),
    alias text CHECK (octet_length(convert_to(alias,'UTF8'))<=4096),
    body_bytes bigint,
    alias_bytes bigint,
    source_retained boolean NOT NULL DEFAULT false,
    retained_bytes bigint NOT NULL DEFAULT 0 CHECK (retained_bytes>=0),
    canonical_hash bytea CHECK (octet_length(canonical_hash)=32),
    replay_actor text CHECK (octet_length(replay_actor) BETWEEN 1 AND 128),
    replay_reason text CHECK (octet_length(replay_reason) BETWEEN 1 AND 512),
    replayed_at timestamptz,
    replay_hash bytea CHECK (octet_length(replay_hash)=32),
    PRIMARY KEY (tenant,source,input_id,fingerprint),
    FOREIGN KEY (tenant,source) REFERENCES raddle_external_sources(tenant,source),
    CHECK ((status IN ('replayed','resolved')) = (replayed_at IS NOT NULL)),
    CHECK ((status IN ('accepted','replayed','duplicate','resolved')) = (canonical_hash IS NOT NULL)),
    CHECK (status NOT IN ('replayed','resolved') OR
        (replay_actor IS NOT NULL AND replay_reason IS NOT NULL AND replay_hash IS NOT NULL))
);
CREATE UNIQUE INDEX raddle_external_accepted_identity
    ON raddle_external_inputs(tenant,source,input_id) WHERE status IN ('accepted','replayed');
CREATE INDEX raddle_external_quarantine
    ON raddle_external_inputs(tenant,source,sequence) WHERE status='quarantined';
