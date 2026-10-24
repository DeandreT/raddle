# PostgreSQL durable execution

The application supplies a SQLx `PgPool` to `raddle_postgres::Store`. Explicitly
call `migrate` during a controlled migration step. The checksummed schema is not
changed implicitly by worker startup. PostgreSQL 14 or later is the supported
baseline; qualification must include the deployed version.

## Transaction ownership

`Store::begin` opens a transaction. `Scope::enroll` instead takes ownership of an
application's existing SQLx transaction. All repository queries use
`scope.connection()`. `stage` inserts resolved local/remote deliveries into the
same transaction. `commit` commits everything; dropping or rolling back the
scope commits nothing. A staging failure makes the scope rollback-only.

Do not issue manual transaction-control SQL through the borrowed connection.
Propagate application failures or call `mark_rollback_only`; swallowing an error
does not make its surrounding operation successful. PostgreSQL transactions do
not make external network side effects atomic.

Custody calls use a trusted source namespace and tenant, separate from the wire
envelope. These values must come from authentication/configuration, never
untrusted message headers. Reuse the same envelope and resolved delivery data
on duplicate custody; changing immutable content under an existing identity is
an error. Independent subscribers receive independent job records.

## Worker behavior

`Worker::handle` registers a contract and subscription with a borrowed SQLx
scope. Its closure returns `Box::pin(async move { ... })`; boxing is limited to
the heterogeneous registry boundary. The handler reports `Retry` or `Reject`;
formatted domain errors are never persisted. A successful result commits the
business transaction and inbox completion together.

Claims are bounded to 256 rows and use `SKIP LOCKED`. Expired claims get a new
generation. Before business writes, execution locks and validates the original
claim. That database lock fences recovery even when the lease time passes.
Workers apply a database statement timeout as well as a Rust future timeout.

Retries use bounded exponential backoff with deterministic per-message jitter.
Abandoned attempts count toward exhaustion. Expired/exhausted work becomes dead
and can be inspected without retrieving its payload. Replay requires a tenant,
actor, and reason and retains an audit record; already expired work is not replayed.

An optional partition key serializes unfinished visible work for that tenant,
destination, and delivery kind. It does not establish a global ordering between
uncommitted producers. Dead-lettering releases the key; replaying an older dead
item later is an intentional operator action, not strict historical FIFO.

`OutboxWorker` publishes outside database transactions and marks completion only
after broker acceptance. A crash after acceptance but before completion causes
republishing with the same message identity. Consumers must be idempotent.

Each tenant defaults to a finite 100,000 pending jobs; `set_capacity` changes that
budget. Admission serializes quota checks within each tenant's transaction.
Duplicate custody remains acceptable even when the budget is full. A completing
inbox record releases its slot atomically with its successors.

Completed records remain as deduplication tombstones. `prune_completed` removes
a bounded tenant-scoped page older than an explicitly supplied cutoff, excluding
records with retained replay audit. Deleting tombstones also ends their
duplicate-detection window; select retention according to broker retry horizons.

## Local qualification

The PostgreSQL tests are opt-in and allocate isolated test databases. Point
`DATABASE_URL` only at a disposable local server with database-create permission:

```sh
cargo test -p raddle-postgres -- --ignored
```

The suite exercises atomic business writes, duplicate conflicts, fencing,
lease loss, independent subscribers, delayed delivery, retries, replay, and real
subprocess exits before commit and after a simulated durable broker acceptance.
It does not substitute the simulated sink for real transport qualification.
