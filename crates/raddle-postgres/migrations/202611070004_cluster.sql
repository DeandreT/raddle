-- Namespace rows are the common lock root for membership and singleton agents.
CREATE TABLE raddle_cluster_namespaces (
    namespace text PRIMARY KEY CHECK (namespace ~ '^[a-z][a-z0-9_.-]{0,63}$'),
    configuration jsonb NOT NULL
);

CREATE TABLE raddle_cluster_members (
    namespace text NOT NULL REFERENCES raddle_cluster_namespaces(namespace),
    instance uuid NOT NULL,
    label text NOT NULL CHECK (octet_length(label) BETWEEN 1 AND 128),
    roles text[] NOT NULL CHECK (cardinality(roles) BETWEEN 1 AND 32),
    registered_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (namespace, instance)
);
CREATE INDEX raddle_cluster_members_expiry ON raddle_cluster_members(namespace, expires_at);

-- Never delete these rows on release or expiry: epochs must survive reacquisition.
-- Owners intentionally have no FK: a departed member must not delete its fence.
CREATE TABLE raddle_cluster_agents (
    namespace text NOT NULL REFERENCES raddle_cluster_namespaces(namespace),
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 64),
    required_role text NOT NULL,
    owner uuid,
    epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    lease_until timestamptz,
    PRIMARY KEY (namespace, name),
    CHECK ((owner IS NULL) = (lease_until IS NULL))
);
