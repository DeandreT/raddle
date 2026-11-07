# PostgreSQL cluster membership and singleton agents

`raddle_postgres::cluster` coordinates explicitly registered process incarnations
and bounded, named singleton assignments. It owns no threads, tasks, heartbeat
loops or shutdown signals. Apply the crate's migrations explicitly before
connecting. All participants must use the same PostgreSQL database and namespace.

```rust,no_run
use std::time::Duration;
use raddle_postgres::{Scope, cluster::{AgentSpec, Cluster, ClusterOptions}};

# async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
let policy = ClusterOptions::new(
    "orders.production",
    &["worker", "observer"],
    vec![
        AgentSpec::new("cleanup", "worker")?,
        AgentSpec::leader("worker")?,
    ],
)?
.with_max_members(64)?
.with_timing(
    Duration::from_secs(60), // membership deadline
    Duration::from_secs(15), // assignment deadline
    Duration::from_secs(5),  // operation/query budget
)?;
let cluster = Cluster::connect(pool.clone(), policy).await?;
let node = cluster.register("orders-node-a", &["worker"]).await?;
cluster.heartbeat(&node).await?;

if let Some(lease) = cluster.acquire(&node, "cleanup").await? {
    let mut scope = Scope::enroll(pool.begin().await?);
    scope.enforce_agent(&cluster, &lease).await?;
    // Application SQL MUST use scope.connection(), not a separate pool connection.
    // Stage messaging effects in this scope too, when needed.
    scope.commit(chrono::Utc::now()).await?;
    cluster.release(&lease).await?;
}

// Call explicitly after joining/draining this process's application work.
cluster.leave(&node).await?;
# Ok(())
# }
```

## Identity, policy and bounds

Each `register` generates a fresh random UUID, returned by `Node::instance()`.
The display label is not identity: restarting with the same label creates a new
incarnation and cannot reuse the prior process's claims. Node and lease handles
have private fields, no public string-based constructors and no deserialization.
Membership/assignment diagnostics cannot be used as ownership capabilities.
These handles are an API misuse boundary, not database authorization: code with
direct SQL write access can bypass coordination. Use trusted application code
and appropriate database privileges.

Namespaces, roles and ordinary agent names are lowercase ASCII identifiers,
1–64 bytes, starting with a letter and otherwise using letters, digits, `_`, `-`
and `.`. Labels are 1–128 bytes without control characters. Policy has 1–32
allowlisted roles, 1–256 distinct agents and 1–1024 live members (default 128).
Members need at least one allowed role, and only the configured agent role may
acquire that agent. Namespace policy is persisted and conflicting role, agent,
capacity or TTL configuration is rejected, not silently overwritten. Changing
policy requires a coordinated application/database migration; this API does not
provide online policy editing.

Membership and assignment TTLs are integral milliseconds from 1 ms to two hours.
The operation budget is 1 ms–30 s and may differ between processes. Choose TTLs
comfortably above expected scheduling, network and database delays; millisecond
values are validation bounds, not a real-time performance guarantee.

`members(limit)`, `assignments(limit)` and `expire_dead(limit)` are bounded reads
or maintenance calls, not an unbounded cluster scan. Membership pages allow at
most 1024 rows, assignment pages at most 256. Registration removes expired
membership rows before checking capacity; assignment rows are bounded by the
configured set and are never pruned or recreated by this API.

## Database clock, heartbeats and recovery

Membership and lease expiry use PostgreSQL `clock_timestamp()`, including after
waiting for locks. No method accepts a caller wall clock for cluster validity.
Heartbeat updates only an unexpired member; an expired incarnation must register
again. A paused process cannot revive its old node or lease after the deadline.
Either an expired lease or an expired/missing owning member makes an agent
eligible for takeover. `expire_dead` is optional bounded housekeeping, not a
prerequisite for recovery.

Every acquisition increments a persisted `bigint` epoch. Release and graceful
leave clear ownership without deleting the agent row or resetting its epoch.
After overflow PostgreSQL rejects the operation rather than wrapping the fence.
Renewal and release require the matching namespace, owner UUID, epoch, live
membership and still-unexpired lease. Renewal does not heartbeat membership;
applications schedule both calls explicitly. A live assignment is not reissued
even to its current owner: retain the lease handle and renew it instead.

`acquire_leadership` acquires the reserved `_leader` named agent defined by
`AgentSpec::leader`. This is database-backed singleton leadership, not a separate
consensus election or a claim of availability without the database.

## Transaction-held fences

`Scope::enforce_agent` and its alias `validate_lease` verify the capability before
business work and retain database locks until **that same scope** commits or
rolls back. An old owner cannot validate after takeover. A successful validation
can finish its database transaction even if the wall-clock lease expires during
the transaction: takeover waits until the protected transaction ends. This
serializes the database effects before the next epoch's effects; it does not
promise that application code stops running at the deadline.

All membership and agent mutations lock their namespace root before member and
agent rows. A protected scope holds that root, deliberately serializing all its
namespace's agents and temporarily delaying heartbeats. Keep protected work
short and do not call coordinator methods through another connection while
holding a scope's fence. Scoped validation uses `NOWAIT` on the root, so
contending scopes fail with SQLSTATE `55P03` instead of waiting with a reversed
agent or cross-namespace lock order. Namespace tuple-version writes ensure that
an old Repeatable Read/Serializable snapshot fails with `40001` rather than
validating pre-takeover data. Retry the **whole transaction**, never just a
failed statement.

Failed, timed-out or cancelled validation marks the scope rollback-only. As with
other scope APIs, propagate application errors or call `mark_rollback_only`.
An application must await successful validation before doing protected work.
Using a different SQLx transaction, bypassing the API with direct cluster-table
writes, or performing work before validation is outside this guarantee.

Database fencing does **not** protect arbitrary HTTP calls, file writes, broker
publication or other external actions. Use destination-supported fencing or
idempotency for those effects; a database lease alone cannot retract an action
from a paused old process.

## Deadlines, cancellation and shutdown

Every coordinator operation has a caller-side Tokio deadline, including pool
acquisition. Each transaction sets LOCAL statement, lock and idle-transaction
timeouts to the operation budget; server work remains bounded if the Rust future
is cancelled. Scoped validation leaves these LOCAL deadlines in effect for the
remainder of the protected application transaction. They are per-statement and
idle limits, not a total multi-statement business-transaction deadline; callers
must also bound their entire scope lifetime. Pool/connection startup follows
the application's SQLx pool configuration.

Dropping or cancelling a coordinator call or handle does not stop other members,
release all assignments or shut down application tasks. Dropped in-flight
transactions roll back through SQLx. Cancellation/connection failure around
commit can have an uncertain outcome: reconcile via bounded diagnostics and
retry safe ownership operations; do not assume an external side effect can be
repeated. If a timed-out acquisition committed without returning its handle,
the assignment is recovered through explicit node leave or normal expiry.

For graceful shutdown, stop scheduling this node's agent work, join or roll back
its outstanding scopes, release individual live leases if desired, then call
`leave`. `leave` removes only that node and releases its assignments, retaining
all fencing epochs. Unexpected process exit relies on database rollback plus
membership/lease expiry; it never requires a whole-cluster stop.

## Disposable-database checks

The opt-in `raddle-postgres` integration test target `cluster` uses SQLx-created
isolated databases under `DATABASE_URL`. Run against a disposable PostgreSQL
server whose account may create test databases:

```sh
cargo test -p raddle-postgres --test cluster -- --include-ignored
```

The tests exercise independent connection pools, concurrent singleton and leader
claims, database-clock heartbeat/expiry, takeover, retained epochs, stale-owner
rejection, transaction commit/rollback, snapshot fencing and bounded contention.
An abrupt subprocess-exit check verifies rollback of fenced business work and
restart with a fresh incarnation and higher retained epoch.
Controlled test-only SQL expires rows in each test's own isolated database;
there is no caller-clock override in the production API.
