# Native streaming transport contracts

`raddle-nats`, `raddle-redis` and `raddle-mqtt` expose protocol-specific broker
operations. They do not equate a socket write, broker acceptance, committed inbox
custody, completed business handling, or an external side effect. Only confirmed
publisher types implement `raddle::transport::Transport`.

All connections, physical destinations and tenant/client lookup keys come from
application configuration. A received tenant header is application data, not an
authorization decision. `Clients` registries refuse missing bindings rather than
falling back to another tenant's connection. Topic/subject prefixes provide soft
separation, not broker authorization: use separate accounts, database credentials
or broker ACLs where security isolation is required.

Held receipts expose `decode_with` for application-owned raw/foreign mappers;
their resulting envelope still passes identity/schema/header/byte validation,
and a failed mapper does not surrender native settlement. JetStream stream
positions and Redis stream/group/entry IDs can seed deterministic foreign
identities scoped by the trusted client/tenant. MQTT requires an application or
device identity: packet IDs and payload hashes are not interchangeable with one.
These hooks are not built-in CloudEvents, MassTransit or NServiceBus codecs.

Public I/O has finite deadlines. Receive pages and protocol windows are bounded
to at most 256 members. A malformed member retains its private native receipt and
does not end a receiver. Callers must process each independent member inside its
own failure boundary and retain successful-member state when retrying failures.

The native clients select Rustls' ring support without installing or changing a
process-global crypto provider. An application that also enables another Rustls
provider must select its provider at application bootstrap or supply native client
TLS configurations with an explicit provider. This is an application-owned security
decision, not an implicit global override by an adapter.

## NATS

`Core` has `send`, `request` and wildcard/queue `subscribe` operations. Explicit
application-trusted dynamic destinations use `send_subject` on Core or JetStream;
the confirmed `Transport` interface continues to accept only configured aliases.
`send`
flushes commands; it does not promise a subscriber exists, a retained copy, or
delivery confirmation. `Core` deliberately does **not** implement `Transport`.
Requests distinguish the protocol's no-responder response. Replies are restricted
to the original NATS inbox, not arbitrary received endpoint URIs. Caller-supplied
`ConnectOptions` own TLS/authentication and event diagnostics, including slow
consumer reporting; subscription capacity bounds Core's best-effort buffer.

`Core::close` requests SDK drain rather than pretending the SDK exposes a task-join
handle. A host requiring completed socket cleanup must observe the SDK's Closed
event through its configured callback before disposing dependent resources.

`JetStream` waits for the actual correlated PubAck. `Receipt` exposes stream,
sequence, domain and duplicate facts. **An unscoped publisher does not populate
Nats-Msg-Id or promise broker retry deduplication.** `with_dedup_scope(Source)`
explicitly binds an application-owned client/tenant/endpoint namespace. Its native
key hashes length-framed trusted scope, destination subject and logical message
identity; an Envelope UUID is never trusted tenant authority. Inside that scope,
the default logical identity is the envelope ID, a `PublishOptions` key overrides
it, and an explicit Nats-Msg-Id logical key has highest precedence. Explicit keys
without a configured trusted scope are refused, and mixed-case/multiple identity
headers cannot bypass that policy. Named client registration verifies a scoped
publisher's client/tenant binding. Conditional stream and
sequence checks remain native. A single scoped publisher cannot represent extra
tenants merely because their payloads or headers say so: choose a configured
publisher with trusted application context.
A failed socket/deadline leaves an uncertain outcome, not proof the broker
rejected a write. Retrying retains the
same identity, and broker deduplication lasts only its configured window.
Different trusted namespaces sharing a physical stream do not suppress each
other's identical UUIDs. A duplicate PubAck proves prior key acceptance, not
payload equality; reuse a logical identity only for the same logical publication.

Stream management takes the native typed stream configuration, preserving
Limits, Interest and WorkQueue retention as distinct policies, along with storage,
replicas, mirrors, sources, placement and domain-specific API addressing.
`Provision::Verify` never creates or updates. `Create` refuses incompatible
existing properties; `Update` authorizes changes to an application-owned resource.
Management operations are not multi-resource transactions and do not undo
partially accepted declarations or delete external resources.
Verification normalizes native compression defaults and ignores broker-generated
`_nats.*` resource metadata; application configuration cannot overwrite those
reserved keys. Application-owned metadata remains bounded and compared.

Pull consumers use explicit acknowledgements and finite MaxAckPending. Native
delivery position survives envelope decode failure and redelivery. Durable names
resume broker progress; unnamed consumers are ephemeral. `HeldMessage::complete`
double-acknowledges **after** the application commits inbox custody or handling.
`retry` sends NAK; `terminate_without_retention` deliberately retains no DLQ copy.
`dead_letter` confirms the raw copy at an allowlisted destination before TERM.
A crash between copy and TERM can duplicate the copy; its deterministic native
dedup key only protects the broker's configured deduplication window.

Each pull uses a fresh SDK inbox and collects for at most half AckWait. Its
receipt deadlines start conservatively before that request, not when an SDK
buffer happens to be read. Empty pulls repeat within the caller's total wait;
a nonempty partial page does not wait through another pull. Thus a long wait for
the first arrival does not consume that delivery's entire lease, and sparse pages
return before their earliest possible expiry. A scheduler/OS stall may still leave
no usable members; expired copies stay unacknowledged for broker redelivery.

`progress` requests an AckProgress confirmation before extending the local deadline,
bounded by the original extension ceiling. Applications must renew queued **and**
running receipts. No-responder/status replies, cancellation and uncertainty invalidate
the local receipt instead of extending it. Expired receipts cannot be settled locally.
NATS acknowledgements
are not an atomic application-processing fence: another consumer can redeliver
during network uncertainty, and external side effects are not exactly-once.
`push_receiver` provides a separate bounded flow-controlled source for buffered or
durable inbox custody, including SDK heartbeat handling. Its `PushDelivery` does
not expose native execution-lease admission: an SDK-buffered receipt can already
be older than AckWait. Use pull receipts for conservative lease admission. No
automatic lease worker, partition-owner service or push-consumer service is started
by this adapter.

Native scheduling requires a 2.12+ server, an enabled Limits-retention stream and
capture of both target and per-envelope scheduling subjects. Otherwise `schedule`
returns `RequiresDurableScheduler` **without publishing anything**; the caller
must persist a fallback schedule. Native receipt means the broker stored a
scheduling entry, not that it fired. Wildcard pulls skip schedule controls instead
of dispatching them as application messages. NATS 2.14.7 is the recommended
qualification target for the scheduler/purge fixes.
Scoped schedule control subjects and dedup keys include the trusted namespace.
Without a configured scope, each control gets a fresh nonce and no broker dedup
key, so unrelated equal UUIDs cannot replace each other's schedules; retries can
then create extra controls. `ScheduleOptions` selects the validated subject suffix
(default `.scheduled`). DLQ keys use the held receipt's trusted source and native
stream/consumer/position, not an unscoped Envelope UUID.

## Redis Streams

`Broker::send` reports the actual XADD stream ID. Broker acceptance does not imply
a replica acknowledgement, disk fsync or completed handling. An uncertain retry
can create a second Redis entry; the application envelope keeps its original ID.
URI database selection and frozen route keys remain native Redis configuration.

Startup checks actual `COMMAND INFO XACKDEL` support and effective eviction/AOF
settings. `Durability::AppendOnly` requires `noeviction` and AOF; `FsyncAlways`
also requires `appendfsync always`. `Volatile` explicitly accepts in-memory broker
custody. AOF everysec can lose recent acknowledged writes; even always cannot
promise survival of hardware failure, rollback or asynchronous failover. The
checks require CONFIG/COMMAND permissions and fail closed if those facts cannot
be verified. `AckMode::DeleteWhenAcknowledged` uses **native atomic XACKDEL ACKED**,
not XACK then XDEL, and therefore requires Redis 8.2+ command support. ACKED
retains the body until every consumer group has acknowledged it.

Receivers create groups only with `Ownership::Owned`; `External` verifies an
existing group and never resets its position. New group start position does not
change resumed group progress. Each receiver owns a unique physical consumer ID.
`receive` uses XAUTOCLAIM for idle pending entries and alternates pending/new
priority. Even one-member pages let new valid work pass a repeatedly failing
pending entry; this fairness policy is not strict original delivery ordering.
Use a dedicated receive connection when blocking: a shared Redis multiplexer can
head-of-line block unrelated commands. The page, native outstanding window and
total call deadline are separate from handler parallelism.

Settlement and progress are Lua operations that check the current XPENDING
consumer **and delivery generation**, then perform the native action atomically.
Stale owners cannot acknowledge entries reclaimed by another receiver, including
same-consumer ABA generations. `progress` resets native idle time with XCLAIM
JUSTID and has a one-hour extension ceiling. Dropping/retrying an entry leaves it
pending for reclamation. `pending` exposes native identity/owner/idle/attempt views.
DLQ copying records raw body/source/attempt/reason and succeeds before source
acknowledgement in the same script; a failed copy cannot acknowledge the source.
Redis scripts are atomic against concurrent clients, but do not roll back earlier
writes if a later command errors. An uncertain DLQ retry can duplicate a copy.

`schedule` stores stable identity/body/target/time in native sorted-set/hash state.
`release_due` is a bounded caller/host-owned pass. It performs XADD before removing
each due member, continues after a bad target, and reports retained failures.
Failures receive a bounded retry delay so even one-member passes cannot keep
selecting the same poison entry ahead of unrelated due work.
Identity/content/time/target conflicts are refused; completed scheduling identities
remain indexed so uncertain scheduling retries do not duplicate completed work.
The five-key format includes a `:state` hash with registering/scheduled/complete
phases. A failed registration repairs missing membership on an identical retry;
metadata alone is never proof of scheduling or completion. Release records the
completion phase before removing membership. Unmarked legacy metadata is refused
as ambiguous rather than silently replayed or accepted; use a fresh trusted namespace
or an application-owned migration that verifies outstanding and completed identities.
The application owns index retention. On Redis Cluster, all scheduling keys and
target streams must share the caller-selected hash slot. No hidden scheduler
task, broad cleanup policy, or cluster-wide cross-slot transaction is implied.

Application-owned multiplexers can be passed to `from_connection`; the adapter
does not close or reauthenticate another owner's connection. A token-refresh
connector can supply a replacement multiplexer and revalidate startup policy.
This is not automatic token acquisition or an authentication refresh daemon.

## MQTT 5

`Session` owns a rumqttc **v5** event loop. Calls poll it directly; no worker is
spawned. A host must keep calling receive/other operations often enough for
keepalive and flow progress, or own a supervised service that does so. Shutdown
drains a finite disconnect operation; dropping the session stops all polling.
Do not hide an event loop behind an unjoined task.

QoS0 returns `Acceptance::Unconfirmed`. QoS1 requires the matching positive PubAck;
QoS2 requires the matching positive PubComp, not merely PubRec. The broker's
NoMatchingSubscribers positive reason still means protocol acceptance, not a
consumer exists. `Publisher` serializes polling with lock wait included in the
total operation deadline and refuses QoS0 as a confirmed `Transport`. A cancelled
or timed-out write leaves `Session::is_unresolved()` true. A refused QoS2 exchange
also invalidates the current client exchange state, unlike an ordinary negative
QoS1 PubAck which permits unrelated follow-on publications. Replace an unresolved session;
do not let stale packet IDs confirm another publication. Reuse the original
envelope identity when retrying an uncertain publication.
Receive-side native connection errors, server Disconnect and unexpected repeat
ConnAck also fail closed. The session must be replaced; an old held receipt can
never be carried into an automatically reconnected broker session with reused
packet IDs. Ordinary idle receive deadlines do not poison a healthy connection.
Explicit drained credential refresh rotates the local receipt namespace as well.

Static aliases, explicit application-trusted dynamic topics, wildcard subscriptions,
bounded MQTT user properties, reply-topic aliases/correlation, native expiry and
retained clearing are supported. Clearing uses a zero-byte **retained** acknowledged
publication. Received response topics must exactly match the application's reply
alias before `respond` sends. MQTT properties and DUP are not authenticated tenant
claims or stable cross-session deduplication identities; the validated envelope
provides that application identity.

Manual source acknowledgements follow the caller's custody/handling boundary.
QoS0 cannot offer such an acknowledgement and is refused. Private handles can be
completed only by their originating session. `quarantine` confirms a retained raw
snapshot under `<trusted-route>/.quarantine/<receipt-token>` before source
acknowledgement and returns that topic. No existing subscriber is required: a
later wildcard subscriber can inspect/replay the poison. Snapshots carry a nonempty
versioned binary frame (`application/vnd.raddle.mqtt-quarantine.v1`), even for an
empty original payload: publishing zero retained bytes would delete the evidence.
`QuarantineSnapshot::from_slice` bounds and exposes the exact original bytes for
inspection/replay; do not replay the frame itself. The application owns
replay and retention cleanup using `clear_retained_topic` with the trusted returned
topic; received topic claims do not authorize clearing. Repeated attempts on one
held receipt use the same topic, while crash/redelivery can create another snapshot.
MQTT has no native DLQ transaction, so interruption between these steps can duplicate
the copy; broker acceptance is not a disk-fsync or database commit guarantee. Broker
persistent-session queues and retained snapshots have different lifetimes from
a committed database inbox. Protocol QoS2 does **not** establish exactly-once
business handling or end-consumer completion. This API does not claim Wolverine's
NativeAck or global-sharded MQTT endpoint modes.

TLS, WebSocket and initial authentication options are supplied through MqttOptions.
Numeric loopback is required for plaintext. Callers select distinct stable protocol
client IDs; `Clients` refuses duplicate IDs across its registrations. Persistent
redelivery requires appropriate clean-start/session-expiry and broker persistence.
`refresh_credentials` reconnects a drained session with a caller-refreshed JWT/OAuth
password and optional authentication method. It does not acquire tokens or perform
in-place MQTT AUTH challenge/re-authentication exchanges.

## Framework boundary and qualification

These are native protocol adapters, not an automatic MessageBus endpoint runtime.
Local+remote additive fanout, full mapper families/foreign identity derivation,
mode-specific DB inbox/outbox wiring, automatic queued/running renewal, global
partition ownership and drain-before-handoff require the owning messaging runtime.
The adapters expose the underlying custody operations without pretending that a
returned envelope is a database commit receipt. No .NET wire/package compatibility
is inferred from successfully exchanging raddle envelopes.

See [fixture procedures and licensing](../conformance/streams/README.md) for actual
broker testing. The default Cargo test run intentionally skips tests requiring
explicit disposable broker endpoints. Cluster failure, account/TLS/auth rotation,
process-crash windows and foreign-counterpart qualification must be established
separately before claiming those operational properties.
