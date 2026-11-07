# PostgreSQL event streams

`raddle_postgres::events` stores immutable versioned wire `Envelope` values in
tenant-scoped streams. Run the normal explicit `Store::migrate` step first. A
stream has a non-nil UUID and an immutable aggregate contract and positive epoch.
Tenants come from trusted application context, never event headers. Aggregate
epochs are separate from event wire schema versions.

## Append and rehydrate

Create `EventStream::for_aggregate::<A>(tenant, id)` or validate an explicit
contract with `EventStream::new`. `Scope::append_events` accepts 1–256 envelopes
and either `ExpectedRevision::NoStream` or `ExpectedRevision::Exact(revision)`.
There is no unconditional append. Reusing an event UUID anywhere in the same
tenant conflicts even when its contents match: append is not a deduplication API.
For retries retain the original envelopes and reconcile stream revisions and
identities after an uncertain commit.

`append_messages` encodes typed `WireMessage` values and allocates new UUIDs. Use
`Envelope::with_identity` and `append_events` when the application needs stable
identities across retries. Append validation, serialization, concurrency, and
database failures make the entire scope rollback-only. Business queries use
`scope.connection()`; outbox delivery uses the existing `scope.stage` API. All
three effects commit together, including when `Scope::enroll` owns an existing
application SQLx transaction. No network side effect belongs in this transaction.
Cancelling a polled event operation also makes a borrowed scope rollback-only;
the caller cannot accidentally commit partially executed SQL after dropping that
future. Dropping an owned `Store::project_stream` future rolls back its scope.
Use PostgreSQL statement/lock timeouts alongside Rust deadlines: dropping a
future does not promise immediate cancellation of an in-flight server query.

`load_events` reads an exclusive stream revision in pages of 1–256. Each record
contains its exact stream revision, tenant position, and original envelope.
`Aggregate` defines a default initial state and a pure, fallible `apply` method.
Its `Event` is a versioned `WireMessage`; a tagged enum can express several event
variants under one stable contract. `rehydrate` folds a fixed observed stream head
in bounded pages and returns state with its exact revision. Register explicit
one-version upcasters in `Codecs`. An unknown contract, unsupported schema,
missing upcaster, failed application, or revision gap fails the load; none is
silently treated as a processed event.

## Ordering and transaction isolation

Stream revisions are contiguous and ordered within their tenant/stream. A
separate **transactional tenant counter row** serializes all appends for one
tenant until commit or rollback. Tenant positions are allocated by updating that
row inside the same transaction. A producer cannot commit a higher position
while an earlier position is uncommitted. Rolled-back allocations roll back too.
This avoids the lost-event gap of sequence/`BIGSERIAL` high-water cursors, whose
values can be allocated before another producer commits.

`tenant_events` offers bounded, commit-ordered pages for **one tenant**. There is
no global cross-tenant cursor or ordering promise. Hosts iterate their configured
tenants independently. A tenant position must never be used as another tenant's
cursor. Keep transactions short: a slow append intentionally holds back later
appends in that tenant. Applications that write several tenants in one transaction
must acquire their tenant locks in a consistent order. The counter tuple is
written, not merely locked, so concurrent writes against an old Repeatable Read
or Serializable snapshot fail with a database serialization error instead of
accepting stale stream state. Retry the whole business transaction with a fresh
snapshot, never just the append call.
Use a consistent lock order across event append, outbox admission, projection
checkpoints, and application rows too; a database deadlock requires a whole
transaction retry.

## Durable projections and failure isolation

`Store::project_stream(name, stream, limit, callback)` applies one bounded page
and commits it. `Scope::project_events` provides the same operation inside an
application-owned scope. Its callback receives the recorded event and the same
mutable SQLx scope; projection business writes, staged outgoing messages, and
checkpoint updates share one transaction. The checkpoint key is
**projection name + tenant + stream UUID**, not just an event sequence.
Concurrent projectors serialize on that checkpoint, preventing double
application of a committed page. A callback that returns success after explicitly
marking rollback-only still cannot commit its checkpoint.

A failed callback rolls back the entire current page and retains the previous
successful checkpoint. Retrying starts from that checkpoint. Events later in the
same stream cannot bypass a failed predecessor; no implicit poison-event skip
policy exists. The host calls other streams in **separate scopes**, records the
failed stream's error, and continues unrelated streams and tenants. Do not put
independent streams into one application transaction when failure isolation is
required. The host owns stream discovery, scheduling, retry policy, deadlines,
and shutdown; this API starts no background worker.

Rebuild a view using a new explicit projection name and separately owned view
state. No hidden checkpoint reset or unrestricted rewind API exists. This is a
small PostgreSQL event persistence and projection facility, not a complete event
database product: it does not supply snapshots, event retention/deletion,
automatic projection deployment, cross-stream atomic projection ordering, or
exactly-once external delivery. Outbox transport remains at-least-once.
