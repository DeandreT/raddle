CREATE TABLE raddle_http_responses (
    scope_hash bytea NOT NULL CHECK (octet_length(scope_hash) = 32),
    key varchar(128) NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    owner uuid NOT NULL,
    status smallint CHECK (status BETWEEN 200 AND 399),
    body bytea,
    location text CHECK (octet_length(location) <= 8192),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (scope_hash, key),
    CHECK ((status IS NULL AND body IS NULL AND location IS NULL) OR status IS NOT NULL),
    CHECK (octet_length(body) <= 1048576)
);
CREATE INDEX raddle_http_responses_expiry ON raddle_http_responses(expires_at);
