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
Once staging starts, dropping its borrowed future also makes the scope rollback-only.
SQLx may still drain an already queued statement when its connection is reused; that does
not restore permission to commit partially staged keys, jobs or skipped admission
checks. Start a fresh transaction when retrying interrupted staging.

Application-owned Read Committed, Repeatable Read, and Serializable transactions
are supported. Admission writes the tenant's quota row before counting pending
jobs, so an older fixed snapshot cannot admit work using a stale count. Under
Repeatable Read or Serializable, a concurrent admission can therefore return
PostgreSQL serialization failure (`40001`) through `Error::Database`. Roll back
and retry the entire application transaction, including its business reads and
writes; retrying only staging inside the same transaction is not safe.

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

A worker claims and expires work only for destinations registered through
`handle` or `handle_saga`. An empty registry does not consume unrelated queues.
Multiple workers sharing one destination must register its complete contract set;
an unknown contract within an owned queue is still quarantined. Destination
filtering happens in the claim transaction, before taking a lease or incrementing
attempts, so legacy workers and managed durable endpoints can share one store.

Claims are bounded to 256 rows and use `SKIP LOCKED`. Expired claims get a new
generation. Before business writes, execution locks and validates the original
claim. That database lock fences recovery even when the lease time passes.
An execution waiting for a pool connection holds no row lock and can lose its
lease to recovery. `Worker` and `OutboxWorker` treat a per-claim `StaleClaim` as
local ownership loss: they join healthy siblings and return normally so polling
can continue, without completing, failing, or changing the successor's claim.
Other errors, including database failures, still propagate after the current
batch joins. Batch return counts are observed claims, not successful business
executions or publications.
Workers apply a database statement timeout as well as a Rust future timeout.
Execution timeouts range from one millisecond through one hour; positive
sub-millisecond timeouts are rejected rather than disabling PostgreSQL's deadline
by truncating them to zero milliseconds.
Leases range from one millisecond through two hours. Claim and retry timestamps
outside the representable clock range return a typed error before changing
durable state; a terminal rejection does not require a future retry timestamp.

Retries use bounded exponential backoff with deterministic per-message jitter.
Abandoned attempts count toward exhaustion. Expired/exhausted work becomes dead
and can be inspected without retrieving its payload. Replay requires a tenant,
actor, and reason and retains an audit record; already expired work is not replayed.
Candidate selection independently excludes exhausted work, even if a locked row
was skipped by the preceding dead-letter sweep and unlocks before selection.

An optional partition key serializes unfinished visible work for that tenant,
destination, and delivery kind. It does not establish a global ordering between
uncommitted producers. Dead-lettering releases the key; replaying an older dead
item later is an intentional operator action, not strict historical FIFO.

`OutboxWorker` publishes outside database transactions and marks completion only
after broker acceptance. A crash after acceptance but before completion causes
republishing with the same message identity. Consumers must be idempotent.
Lease loss after publication has the same redelivery possibility; an accepted
publication is not proof that this owner completed the outbox record.
After its batch futures finish, `outbox.into_transport()` returns the owned
publisher so the application can close it explicitly before closing its broker
connection. Consuming the worker does not abandon a hidden background task.

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
lease loss, independent subscribers, delayed delivery, retries, replay,
fixed-snapshot admission, coordinated exhaustion/lookup races, and real
subprocess exits before commit and after a simulated durable broker acceptance.
It does not substitute the simulated sink for real transport qualification.
