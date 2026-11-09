# SQLite custody and poll boundaries

`raddle-sqlite` uses real application-owned transactions. `Store::open` selects
WAL, full synchronization and one pooled connection; `Store::new` uses the
application's pool and durability configuration. Schema migration remains an
explicit `Store::migrate` operation.

## Transactional staging

`Scope::stage` stages the complete resolved fan-out with the application's writes.
An error, panic or cancellation of a polled staging operation makes its retained
scope rollback-only. Dropping just the borrowed staging future cannot authorize
committing the destinations that happened to be inserted already. Drop the scope,
roll it back, or let `commit` return `Error::RollbackOnly`; retry the whole business
transaction rather than the unfinished tail of its fan-out.

This guard applies to staging, not arbitrary repository code. Propagate application
SQL errors or call `mark_rollback_only` when the application refuses its own work.
A wrapper cannot infer a swallowed business error or cancellation of an unrelated
SQL future.

Equivalent duplicate staging retains the original job identity. Immutable content
and delivery options must still match; conflicts do not replace an accepted order.
Existing tenant capacities, replay audits and explicit retention rules continue
to apply.

## Destination admission

`Worker::run_once` claims only destinations in its handler registrations. Its
expiry and attempt-exhaustion sweep has the same scope. An empty worker consumes
nothing; it does not dead-letter other workers' queues. Workers sharing one
destination must register that destination's complete contract set: an unknown
contract inside an owned queue is still rejected rather than silently forwarded.

`Store::claim` retains its explicit whole-kind behavior. Use `claim_for` when a
caller owns one destination:

```rust,no_run
use std::time::Duration;
use chrono::Utc;
use raddle_sqlite::{Claim, DeliveryKind, Error, Store};

async fn claim_orders(store: &Store) -> Result<Vec<Claim>, Error> {
    store.claim_for(
        DeliveryKind::Local,
        "orders",
        16,
        Duration::from_secs(30),
        Utc::now(),
    ).await
}
```

Other destinations' leases, attempts and expired jobs remain untouched. Partition
ordering continues to use the original tenant, kind, destination and key; scoped
admission does not combine queues or change stored routing fingerprints.

## Lease and polling outcomes

Execution admission, local completion, outgoing completion and failure settlement
all require the current generation, active custody and a live lease. A stale or
expired owner cannot release a successor's work. Expired local completion rolls
back its business transaction instead of recording successful handling.

The supplied time may be logical or ahead of the physical clock. Operations
advance that same caller clock across pool and SQLite writer waits, sampling
after the write transaction is acquired. They do not clamp it to wall-clock time.
Local completion also drains previously queued SQLite work before checking time.
Stored lease comparisons use millisecond precision.

Both `Worker::run_once` and `OutboxWorker::run_once` return whether they observed a
claim, **not** whether business handling or publication succeeded. Ownership loss
is local to that claim and returns `true`; subsequent polls can continue. Genuine
database and configuration errors still propagate. A database error is not itself
proof that a commit rolled back: reconcile persisted state before making an
operator recovery decision.

If an application swallows a staging error or cancellation and returns success,
`Scope::commit` still rolls back and reports `RollbackOnly`. The worker records a
bounded retry for that input instead of letting its confirmed rollback stop
neighboring polls. This classification is specific to confirmed rollback-only
completion; commit or rollback database errors are not converted into retries.

Successful jobs retain their completion state while failed jobs remain retryable
or quarantined. Audited replay targets failed custody only; it does not resubmit
successful neighbors. Outgoing effects remain at least once: remote publication
happens outside the write transaction, and uncertainty can require retry with the
same envelope identity. Receivers must retain their own duplicate protection.

## Execution limits

SQLite still has one active writer per database file, across tenants. The worker's
handler budget covers cooperative Rust futures and SQLite VM work through SQLx's
safe progress handler. It is not a hard bound on filesystem calls, native SQLite
functions or blocking application code. Keep writer acquisition and application
work bounded, preserve explicit shutdown ownership, and use an appropriate backend
when parallel writers are required.

These custody contracts do not install PostgreSQL-only application-framework
bindings into SQLite, implement distributed shards or establish another SQL
provider's behavior. Broader queue, tenancy, saga and interoperability contracts
remain separate.
