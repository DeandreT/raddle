# PostgreSQL external-table listeners

An application can insert messages into its own PostgreSQL table in the **same
database** as the Raddle message store. An explicitly polled `external_tables::Listener`
moves valid inputs into canonical durable inbox custody. Existing `Worker`
handlers, retries, ordinary dead letters and audited replay then apply.

This is a native PostgreSQL implementation, not a .NET integration or a claim
about SQLite, SQL Server, MySQL or Oracle support.

## Configuration and ownership

```rust,ignore
use raddle_postgres::{Store, external_tables::{Aliases, Config, Listener, Options}};

let mut config = Config::new("authorized-tenant", "legacy-export", "exports", "incoming", "orders");
config.body_column = "body".into();
config.timestamp_column = "added".into();
config.default_alias = Some("order".into());
// For mixed types instead, or with an explicit null/empty fallback:
// config.alias_column = Some("message_type".into());
let mut aliases = Aliases::default();
aliases.register::<PlaceOrder>("order")?;
let options = Options { control_source: false, ..Options::default() };
let listener = Listener::new(Store::new(pool), config, aliases, options)?;
listener.validate_source().await?;
let report = listener.poll_once(chrono::Utc::now()).await?;
```

The application supplies the pool, tenant, custody source, destination and delivery
policy. These bindings are trust assertions by application code, not authentication
performed by the listener. Body fields and headers cannot choose another tenant,
destination, reply authority or delivery policy. One physical source table is
bound to one tenant/source; competing bindings are refused.

The fixed PostgreSQL columns are a UUID primary key, JSONB application body,
diagnostic `timestamptz`, and optional `varchar` type alias. Schema, table and
**all column names** are independently validated and quoted, including embedded
quotes, Unicode and whitespace. Identifiers are nonempty, NUL-free and at most
63 UTF-8 bytes. The native defaults are `id`, `json`, `timestamp`, and no alias column.
There is no arbitrary SQL fragment/type/predicate configuration. Ordinary unbounded
JSONB and VARCHAR columns are supported; source length constraints are not required.

Without an alias column, configure one registered default type. With an alias
column, every nonempty alias must match an explicit typed registration. Null/empty
aliases use a default only when configured. Unknown nonempty aliases never fall
back. There are at most 256 registrations, with aliases up to 4096 UTF-8 bytes;
each performs real typed JSON conversion
before constructing a validated canonical envelope. Protected `raddle-aes` root
frames and canonical/selected wire-root frames are refused before permissive model
decoding. There is no implicit decryption or transparent wire-envelope unwrapping.
An unwinding application model projection is classified as a row-local decoder
failure before custody; process-aborting application code cannot be contained.

The source UUID is the canonical message identity. Native canonical creation time
is the first **durably observed** time, persisted before custody admission and reused
through refusal, restart and corrected replay. Source timestamps are preserved for
diagnostics only; they do not establish business dates, scheduling or eligibility.
Receipt times passed to `poll_once` affect delivery availability, not immutable
canonical identity. Use a trusted clock and preserve configured delivery policy.

The documented upstream option defaults are retained: ten-second polling, batches
of 100, source control enabled and advisory key 12000. They are configurable. Source
control enables **only** explicit `provision_source`; constructors, polling and Host
registration never perform source DDL. Disabled control refuses provisioning without
opening a connection. Explicit provisioning creates the source schema/table when
missing and validates existing shape; it never automatically alters or drops drift.
Run `Store::migrate` separately for checksummed framework-owned migrations.

## Atomic custody and fairness

Each poll owns one checked-out connection, including its PostgreSQL session advisory
lock. Independent per-row transactions reselect using `FOR UPDATE SKIP LOCKED` and
commit canonical `Scope::stage_received`, an accepted receipt and **exactly one**
source deletion together. Deletion/receipt/staging failures roll everything back.
A failed Scope is rolled back, never reset or reused for the next input.

Session locks are explicitly unlocked on normal completion. The connection is
marked close-on-drop immediately after acquisition and is closed rather than
returned to the pool, including cancellation and uncertain lock/unlock outcomes.
There is no second-connection capacity requirement. Statement, lock, pool-acquisition
and whole-operation deadlines are finite. Deadline/commit transport failures cannot
establish whether an already submitted commit succeeded; reconcile by input identity.

Discovery uses a durable UUID cursor and finite sweep high-water mark. Cursor
advancement is independent of row custody, not acknowledgement. A refused first
row cannot monopolize the next page, and a stream of new arrivals does not extend
the current sweep indefinitely. Discovered keys are hints: actual source selection
and movement occur in the row transaction. Row-local failures leave the input safely
retryable and do not fail healthy siblings. Shared schema/configuration/database
prerequisite failures remain errors.

Accepted receipts retain original input and canonical/delivery fingerprints even
after ordinary completed-job pruning. Equivalent reinsertions do not add work;
changed content under an accepted UUID becomes a separate failed occurrence and
cannot replace the successful identity. Diagnostic timestamp changes alone do not
change the accepted application content. External network side effects still need
their own idempotency; PostgreSQL custody does not make them transactional.

## Allocation and raw poison

SQL guards every producer-controlled variable-width projected field **before**
SQLx receives it. It returns full JSONB text/alias only within configured transfer
bounds; otherwise it returns bounded lengths, fingerprints and diagnostic metadata.
Built-in PostgreSQL SHA-256 uses unambiguous JSON-array framing over complete stored
values, including a tag distinguishing SQL NULL from JSON null, without installing
an extension. JSONB evidence is the database's stored
representation, not the producer's original JSON spelling.

These guards bound application transfer, **not** PostgreSQL detoasting, rendering,
hashing, scan cost or server memory. Database deadlines bound blocked/expensive
statements but do not constitute a hard server-memory guarantee. Envelope validation
also runs after typed conversion and can refuse an envelope whose metadata plus
body exceed canonical limits.

Unknown types, bad typed shapes and unsupported framing are not valid canonical
envelopes. A bounded complete raw snapshot can be quarantined and its source row
deleted in the same transaction. Oversized inputs instead acquire bounded metadata
and an **in-place reference**: the complete original source row remains untouched
and excluded from ordinary polling. Nothing is truncated and then deleted.

Caller-configured quarantine row/byte budgets are serialized transactionally.
Resolved raw evidence/audits still consume those budgets. Full budgets or failed
quarantine writes leave source rows intact and visibly deferred while the cursor
continues to healthy current/new work. There is no automatic eviction, discard,
retention cutoff, payload purge or receipt pruning.
Redelivery of a completely snapshotted pending input reuses its quarantine, rather
than consuming another snapshot slot or silently bypassing operator recovery when
type registrations change. Diagnostic-only changes do not create a new raw input.

## Two distinct recovery paths

**Canonical handler failures:** inspect through existing `Store::search_dead_letters`
and replay the failed delivery using `replay_dead_letter_bounded` with tenant,
operator and reason. The canonical identity/content stay immutable. This is the
ordinary external-table handler-to-DLQ/replay path.

**Raw pre-custody failures:** `search_quarantine(after, limit)` returns at most ten
failed occurrences inside the listener's bound tenant/source. The application must
separately authorize private payload access and operator actions. Pass one occurrence
to `replay_quarantined`, with an optional bounded `Correction`, actor and reason.
Recovery projections also enforce the listener's **current** body and alias limits,
including snapshots written with larger limits before a restart. Search returns
bounded metadata with `snapshot_available = false` when a complete snapshot cannot
be returned; the stored original is not truncated or changed. Replay without a
correction returns `Unavailable` for that item and leaves it unresolved. An explicit
small correction selects no old snapshot body/alias and can proceed under the normal
identity, audit and admission checks without raising the transfer limits.
Known type resolution, protected-frame checks and canonical validation still apply.
The original UUID and persisted first-observation time remain stable.

Replay locks one occurrence and atomically commits canonical custody, successful
correction audit and terminal resolution. The original fingerprint/snapshot remain;
the configured physical table/column binding must still match its persisted source.
Type registrations are not frozen into that structural binding: an application can
add a registered contract and explicitly replay its failed input without resetting
accepted receipts. Existing accepted identities and delivery facts remain immutable.
The correction fingerprint, actor, reason and time are retained. Repeated/concurrent
replays observe resolution without another delivery. A correction matching an
already accepted identity reconciles without restaging healthy work; another
correction conflicts rather than replacing success. Invalid corrections, admission
refusal, cancellation and failed audit writes leave the occurrence unresolved.
Redelivery of a completely snapshotted original that was already corrected is
deduplicated against that retained correction audit, not corrected or executed again.

For in-place references, replay locks and rechecks the complete original fingerprint.
Missing/changed rows conflict for that occurrence. An explicit bounded correction
can gain canonical custody, but its oversized original remains at source as retained
evidence, excluded from ordinary polls even after resolution. This API does not
silently choose when to destroy an original too large to copy safely. External
writers can independently mutate/delete their rows; fingerprints detect changes,
not prevent them. No preservation guarantee is made against those writers.

## Caller-owned hosting

Register `raddle_host::postgres_external_table` through `Host::service` using the
application's `Arc<Listener>` and restart policy. It performs no migration or
provisioning. Nothing starts until `Host::run` is polled. Readiness follows an actual
successful source poll, not registration or advisory contention. Ordinary row-local
refusals do not fail the Host. Cooperative shutdown drains the bounded current
round; forced shutdown aborts it and closes the connection, leaving uncommitted
source work recoverable. Canonical workers are separately registered services.
