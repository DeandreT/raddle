-- Originals remain the canonical jobs: quota, deduplication and replay all see
-- their real state. Membership stores no duplicate payload or shadow outcome.
CREATE TABLE raddle_batches (
    id uuid PRIMARY KEY,
    tenant text NOT NULL CHECK (octet_length(tenant) BETWEEN 1 AND 128),
    destination text NOT NULL CHECK (octet_length(destination) BETWEEN 1 AND 128),
    group_key text CHECK (octet_length(group_key) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL,
    expires_at timestamptz,
    status text NOT NULL CHECK (status IN ('ready','active','completed')),
    owner uuid,
    generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
    lease_until timestamptz,
    available_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    matching_failures integer NOT NULL DEFAULT 0 CHECK (matching_failures >= 0),
    isolation_used boolean NOT NULL DEFAULT false,
    probing boolean NOT NULL DEFAULT false,
    CHECK ((owner IS NULL) = (lease_until IS NULL)),
    CHECK ((status = 'active') = (owner IS NOT NULL)),
    CHECK (NOT probing OR (isolation_used AND status = 'active'))
);
CREATE INDEX raddle_batches_claim ON raddle_batches(destination, available_at)
    WHERE status IN ('ready','active');
CREATE TABLE raddle_batch_members (
    job_id uuid PRIMARY KEY REFERENCES raddle_jobs(id) ON DELETE CASCADE,
    batch_id uuid NOT NULL REFERENCES raddle_batches(id),
    ordinal integer NOT NULL CHECK (ordinal BETWEEN 0 AND 255),
    coalesce_key text CHECK (octet_length(coalesce_key) BETWEEN 1 AND 128),
    probed boolean NOT NULL DEFAULT false,
    UNIQUE (batch_id, ordinal)
);
CREATE INDEX raddle_batch_members_batch ON raddle_batch_members(batch_id);
