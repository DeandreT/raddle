CREATE TABLE raddle_sagas (
    tenant text NOT NULL,
    kind text NOT NULL,
    correlation text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    revision bigint NOT NULL DEFAULT 0,
    state jsonb NOT NULL,
    completed boolean NOT NULL DEFAULT false,
    timeout_generation bigint NOT NULL DEFAULT 0,
    timeout_due timestamptz,
    PRIMARY KEY(tenant,kind,correlation)
);
CREATE TABLE raddle_conversations (
    tenant text NOT NULL,
    id uuid NOT NULL,
    destination text NOT NULL,
    responder text NOT NULL,
    reply_contract text NOT NULL,
    deadline timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','completed','cancelled','timed-out')),
    response bytea,
    PRIMARY KEY(tenant,id)
);
CREATE INDEX raddle_conversation_deadlines ON raddle_conversations(deadline) WHERE status='pending';
