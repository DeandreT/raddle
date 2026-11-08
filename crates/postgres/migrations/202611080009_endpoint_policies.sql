CREATE TABLE raddle_endpoint_policy_pauses (
    destination text PRIMARY KEY,
    paused_until timestamptz NOT NULL
);
CREATE TABLE raddle_policy_attempts (
    job_id uuid PRIMARY KEY REFERENCES raddle_jobs(id) ON DELETE CASCADE,
    handling bigint NOT NULL DEFAULT 0 CHECK (handling >= 0),
    uncertain boolean NOT NULL DEFAULT false
);
