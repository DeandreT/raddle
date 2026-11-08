CREATE TABLE raddle_jobs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    tenant TEXT NOT NULL,
    destination TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('local','remote')),
    message_id TEXT NOT NULL,
    envelope BLOB NOT NULL CHECK(length(envelope) <= 1048576),
    fingerprint BLOB NOT NULL,
    status TEXT NOT NULL DEFAULT 'ready' CHECK(status IN ('ready','active','completed','dead')),
    available_at INTEGER NOT NULL,
    expires_at INTEGER,
    partition_key TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    max_attempts INTEGER NOT NULL CHECK(max_attempts BETWEEN 1 AND 1000),
    generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
    lease_until INTEGER,
    completed_at INTEGER,
    last_error TEXT,
    UNIQUE(source,tenant,destination,kind,message_id)
);
CREATE INDEX raddle_ready ON raddle_jobs(kind,status,available_at,sequence);
CREATE TABLE raddle_quotas (tenant TEXT PRIMARY KEY,max_pending INTEGER NOT NULL DEFAULT 100000 CHECK(max_pending BETWEEN 1 AND 10000000));
CREATE TABLE raddle_replay_audit (id INTEGER PRIMARY KEY,job_id TEXT NOT NULL REFERENCES raddle_jobs(id),actor TEXT NOT NULL,reason TEXT NOT NULL,replayed_at INTEGER NOT NULL);
