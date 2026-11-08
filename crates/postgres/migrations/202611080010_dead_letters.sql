ALTER TABLE raddle_jobs DROP CONSTRAINT raddle_jobs_status_check;
ALTER TABLE raddle_jobs ADD CONSTRAINT raddle_jobs_status_check
    CHECK (status IN ('ready', 'active', 'completed', 'dead', 'discarded'));

DROP INDEX raddle_jobs_partition;
CREATE INDEX raddle_jobs_partition
    ON raddle_jobs(tenant, kind, destination, partition_key, sequence)
    WHERE status NOT IN ('completed', 'dead', 'discarded');

CREATE INDEX raddle_jobs_dead_letters ON raddle_jobs(tenant, sequence) WHERE status='dead';

CREATE TABLE raddle_discard_audit (
    id bigserial PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES raddle_jobs(id) ON DELETE RESTRICT,
    actor text NOT NULL CHECK (octet_length(actor) BETWEEN 1 AND 128),
    reason text NOT NULL CHECK (octet_length(reason) BETWEEN 1 AND 512),
    discarded_at timestamptz NOT NULL
);
CREATE INDEX raddle_discard_audit_job ON raddle_discard_audit(job_id);
