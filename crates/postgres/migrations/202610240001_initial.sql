CREATE TABLE raddle_quotas (
    tenant text PRIMARY KEY,
    max_pending integer NOT NULL DEFAULT 100000 CHECK (max_pending BETWEEN 1 AND 10000000)
);
CREATE TABLE raddle_jobs (
    id uuid PRIMARY KEY,
    sequence bigserial UNIQUE NOT NULL,
    source text NOT NULL,
    tenant text NOT NULL,
    destination text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('local', 'remote')),
    message_id uuid NOT NULL,
    envelope bytea NOT NULL CHECK (octet_length(envelope) <= 1048576),
    fingerprint bytea NOT NULL,
    partition_key text,
    status text NOT NULL DEFAULT 'ready' CHECK (status IN ('ready', 'active', 'completed', 'dead')),
    available_at timestamptz NOT NULL,
    expires_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 1000),
    generation bigint NOT NULL DEFAULT 0,
    owner uuid,
    lease_until timestamptz,
    last_error text,
    completed_at timestamptz,
    UNIQUE (source, tenant, destination, kind, message_id)
);
CREATE INDEX raddle_jobs_claim ON raddle_jobs(kind, available_at, sequence) WHERE status IN ('ready', 'active');
CREATE INDEX raddle_jobs_partition ON raddle_jobs(tenant, kind, destination, partition_key, sequence) WHERE status NOT IN ('completed', 'dead');
CREATE TABLE raddle_replay_audit (
    id bigserial PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES raddle_jobs(id),
    actor text NOT NULL,
    reason text NOT NULL,
    replayed_at timestamptz NOT NULL
);
