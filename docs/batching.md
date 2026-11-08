# Receive-side batching

`raddle-batching` assembles originals into actual handler inputs. A page of
independently executed job claims is not a receive-side batch. Applications own
admission, clocks, consumer registration, assembly calls, execution tasks, joins
and transport settlement. Constructing an assembler or routing plan changes no
existing runtime behavior.

## Typed volatile assembly

Create an `Assembler<T>` per listener, message type and independent consumer.
`Options` defaults to 100 originals, a 250ms first-member wait and 10,000 pending
originals. Bounds are 1..=256 originals, 1ms..=1h wait, and size..=100,000 pending.
The capacity includes outstanding batches, not just the accumulation buffers.
Full admission returns ownership of the original. Flush a full bucket before
admitting more to that bucket. Methods require exclusive access; applications
can use their own mutex for concurrent first use without a lazy background task.

`BatchKey` contains authenticated tenant and optional trusted group. Absent group
explicitly means an ungrouped lane; it is not inferred from a header. Keys and
coalescing keys are bounded. Call `flush_due(now)` on arrival and clock advance;
empty intervals produce no batch. `drain(now)` flushes accepted work synchronously
without waiting on an unbounded handler shutdown. Join and settle execution tasks
under the application's deadline afterward.

`Batch::members` retains every original context: wire id, delivery/custody id,
attempt, sent time and expiry. `input` gives last-value-wins survivors in original
survivor order. For A1, B1, A2, input is B1, A2 while all three originals remain
participants. Transform these borrowed values into any application-owned custom
shape. No `Any` message bag or synthetic serialized batch payload is involved.

Pending tracking includes accumulation and owned execution/retry batches.
`accumulating` excludes execution. Retain an owned batch through retry and until
settlement; individually expired originals returned by flushing also retain a
pending token. Dropping either token releases **volatile tracking only**, not a
broker acknowledgement or a durable commit. A process exit loses this volatile
state. If an external receiver already settled arrivals, its accumulation window
cannot be recovered; use canonical durable ingress instead. Delivery identities
must be unique among a caller's outstanding volatile work; the assembler rejects
duplicates in its current accumulation buffers, not historical redelivery.

An optional `Registry` constructs observed assemblers and samples pending totals
by actual Rust type and listener. In-process-origin assemblers contribute to type
and total counts but not listener counts. Outstanding batch/expiry tokens remain
observable when their assembler is dropped; cancelled accumulation is removed.

Expired originals are refused/shed individually before assembly. After assembly,
whole-batch expiry is the latest original expiry only if every original is
bounded. A partially expired assembled batch may run all its originals; an
unbounded original prevents whole-batch expiry.
PostgreSQL checks whole-batch expiry both when claiming and before business
execution, including a delay between claim and execution.

## Registration and queue ownership

`RoutePlan::build` validates named destinations and returns deterministic startup
diagnostics. In classic mode, a direct consumer shadows the batch registrations;
`assert_no_conflicts` rejects this at startup instead of emitting `Shadowed`.
Separated mode returns the direct destination and every independent batch
destination. Stage that fan-out in one canonical ingress transaction; each
destination retains its own attempts and successful state. Destination names are
explicit (a `-batch` suffix is a useful convention, not an implicit queue).

Every executable batch queue is currently `ExecutionQueue::Independent`.
This is an explicit opt-out from sequencing against unbatched destinations.
`assert_sequenced` rejects it at startup. Batch grouping is preserved for custom
shapes but does not secretly route unbatched messages into the same partition.
Do not register ordinary workers against a dedicated batch destination. The
ordinary workers in a mixed application must use endpoint-scoped claims, not an
unfiltered global job claimer that can take not-yet-assembled batch arrivals.
PostgreSQL adapter serializes unfinished batches within a tenant/group/destination
lane, not across queues or different groups. It does not promise historical FIFO
between delayed or uncommitted producers.

The plan is an explicit integration API, not automatic handler discovery or
installation into `raddle-messaging`. Multiple batch-only registrations use
independent destinations in either combination mode.

## PostgreSQL custody and execution

Enable `raddle-batching/postgres`. Apply `raddle_postgres::Store::migrate`, which
includes batch membership, then construct `BatchStore` with the **same** SQLx pool.
Decrypt, validate, deserialize/upcast and resolve trusted routing **before**
`Scope::stage_received`; encryption nonces must never become inbox identity.
Continue using canonical admission and the tenant budget configured by
`Store::set_capacity`. `Options::capacity` applies only to the volatile assembler.

1. Stage originals as local jobs addressed to dedicated batch destinations.
   For group batching, put the trusted group in `Delivery::partition_key`.
2. Drive `assemble` for each configured tenant/group/destination. It holds a
   transaction-scoped lane lock, sheds unassembled expired originals in bounded
   pages, and aggregates only when size or first-original availability plus wait
   is reached. Restart does not restart this timer. The deterministic coalescing
   callback sees canonical envelopes; its bounded key is persisted with membership.
   Return `Error::InvalidMember` for an order-specific preparation refusal; malformed
   envelopes and invalid key bounds also quarantine only that original. A failed
   preparation quarantine preserves ready custody and postpones that original by
   the wait interval so other originals can continue. Other callback errors are
   shared prerequisite failures. Do not classify invalid order data as a shared
   configuration error.
3. Drive `run_once` with a typed error matcher, lease, execution timeout, retry
   timestamp and optional `Isolation::After(N)`. Its handler receives all original
   contexts and the application's SQLx `Scope`. `BatchView::decode<M>` supplies a
   concrete owned typed projection without transferring custody; build a custom
   typed shape from its coalesced input. Return `Resolution::committed` only after
   performing the selected business effects in this scope.
4. A named `Resolution` identifies delivery ids, not array positions or wire ids
   that may be shared by independent custody records. Unnamed originals retry.
   Naming any coalesced original expands its outcome to the entire key. Conflicting
   outcomes are rejected before settlement. Do not perform business effects for
   originals marked Retry/Poison. A named committed survivor means its writes are
   in this transaction, not that an unverified external effect somehow rolled back.
5. Commit acknowledges no broker implicitly. A receiver may settle after original
   inbox admission only when the inbox owns retry custody; the originals themselves
   remain pending until batch settlement. Connect broker-specific settlement
   explicitly in the application.

The batch relation stores membership/ordinal/coalescing/probe metadata, **not**
another payload or shadow copy of job outcomes. Originals stay active with an
internal infinite job lease while their batch row supplies the finite fencing
lease. Ordinary job claimers cannot steal these assembled originals after a crash.
Canonical quota/status/replay views continue to see every pending original.
Completed originals remain deduplication tombstones and dead originals are the
canonical quarantine records. Normal canonical tombstone pruning removes their
membership by foreign-key cascade. Batch metadata currently has no automatic
retention sweep; applications must retain it (it contains no message payload).

`TrackingSession` observes an explicit bounded set of original delivery ids in one
trusted tenant and waits for canonical terminal settlement, not handler return.
It distinguishes completed and dead originals, remains pending through retries
and uncommitted transactions, and treats missing/pruned identities as an error.
The caller supplies a finite total deadline and polling interval; cancellation
changes no custody. Assembly/workers must be driven independently. PostgreSQL
`pending` counts durable custody by tenant/destination; the volatile registry's
Rust-type/listener statistics are separate and are not reconstructed from the DB.

`execution` validates owner/generation/unexpired lease and holds the batch and
    original row locks before business writes. Those locks fence takeover during a
live transaction even if the clock passes its lease. SQL statement and lock
timeouts bound database waits; the runner separately bounds the handler future.
Fence checks refresh effective time after pool acquisition and row-lock waits:
the caller's logical UTC origin is advanced by monotonic elapsed time, not replaced
by the host's wall clock. Waiting cannot revive an expired lease or whole batch.
Cancellation drops the scope without running a hidden failure continuation.
Explicit rollback precedes opaque retry, timeout handling or probing. An error
while committing is not proof of rollback: reconcile original canonical identities
and never blindly repeat nontransactional effects. `Scope` conservatively checks
outgoing admission before batch settlement, without credit for the batch's future
released slots; reserve quota headroom when staging successor effects from a
batch handler. A caller using lower-level
`Execution` instead of the runner also owns a total transaction deadline.

## Poison isolation and reduced recovery

A successful batch settles **all** originals, including coalesced-away inputs.
Named Commit/Poison/Retry decisions commit healthy effects and preserve only
failed originals for reduced replay. Quarantine writes use a savepoint per
coalesced unit. If persistence of that poison unit fails, all its originals stay
pending, `SettlementReport::quarantine_failed` names them, and unrelated healthy
effects can still commit. A broken shared connection/transaction is a shared
prerequisite failure; no terminal success is fabricated.

After N matching concrete opaque handler failures, `Isolation::After(N)` enters
at most one persisted size-one sweep per assembled batch. Abandoned claims do
not count as matching domain failures. A coalesced key is one
input unit with all original custody participants. Each probe has its own business
transaction. A failed probe rolls back before its Retry disposition is recorded;
successful probes commit and are excluded from future execution. Persisted probe
progress resumes after process death. Once the sweep ends, later attempts execute
the reduced remainder normally and cannot recursively trigger another sweep.
If recovery exhausts the last unprobed unit, it closes probing and resumes the
already-probed remainder as a normal reduced retry rather than consuming claims
without invoking the handler.
Timeouts retry without assuming a domain-error isolation match. A claim is bounded
to 256 input units; individual attempts also remain bounded by original job budgets.

An exhausted participant terminalizes its entire persisted coalesced unit: an
older value can never be promoted when the last-value-wins survivor exhausts its
smaller budget. Claim-time exhaustion and expiry quarantine use a savepoint per
unit. Failed persistence retains active, batch-owned original custody with a
pending-quarantine classification; those units are excluded from handler input
and do not consume execution attempts. Healthy units continue. Quarantine-only
work is postponed by 250ms, and due batches are selected by availability before
creation time, so an older repeatedly failing unit cannot starve a later due batch.
Only a committed canonical dead record is terminal; a pending classification is
never reported as successful processing or completed quarantine.

An operator can use canonical audited `Store::replay` for dead originals. Once the
old batch is terminal, assembly detaches only the replayed ready originals and
creates a new batch; completed siblings never reappear. Do not rewrite immutable
payload under an existing inbox identity: a correction to business rules/reference
data may retry that original, while changed wire content needs a new identity.

## Verification and integration boundaries

Portable tests use deterministic timestamps for size/time/empty flush, bounded
admission/drain, tenant/group split, concurrent first use, custom shapes, original
contexts, coalescing, expiry and routing assertions. PostgreSQL tests use isolated
SQLx databases and deliberate subprocess exits during accumulation, execution,
reduced replay and one-time probing. Fault tests cover first/middle/last poison,
failed DLQ persistence, canonical failed-only replay, leases and handler deadlines.

```sh
cargo test -p raddle-batching
cargo test -p raddle-batching --features postgres
# DATABASE_URL must identify a disposable local server with database-create rights:
cargo test -p raddle-batching --features postgres -- --ignored --test-threads=1
```

Remaining contracts are explicit: no automatic batch registration in application
routing, no shared partition sequencing with unbatched destinations, no
broker-specific acknowledgement bridge, and no durable
assembly adapter for SQLite or other stores. PostgreSQL pending views are scoped
by tenant/destination, not a payload-type metric. These APIs do not claim that
the full upstream batch-handler family or transport end-to-end parity is closed.
