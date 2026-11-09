# Buffered native grouping

Native grouping is opt-in execution inside a process. It does not establish
exclusive cluster ownership, broker acknowledgements, durable ordering or a
foreign runtime's wire identity. A logical group is separate from a transport
partition key and from PostgreSQL/SQLite `Delivery.partition_key` predecessor
ordering.

Run the complete [in-memory example](../crates/messaging/examples/buffered_grouping.rs)
with `cargo run -p raddle-messaging --example buffered_grouping`. It composes the
real host, handlers, resolver and four admission paths, and waits for bounded
execution receipts before shutdown. It needs no broker or database.

```rust
use raddle::grouping::{GroupId, DispatchOptions};
use raddle::sequencing::SlotSet;
use raddle::routing::EndpointAlias;
use raddle::wire::WireMessage;
use raddle_messaging::{LocalQueue, QueueSettings, QueueMode};
use raddle_messaging::routing::{RoutingConfig, GroupingRule, MessageSelector};

# fn configure<M: WireMessage>() -> Result<(), Box<dyn std::error::Error>> {
let slots = SlotSet::new("orders", 7)?;
let (queue, runner) = LocalQueue::partitioned(
    QueueSettings { mode: QueueMode::Buffered, capacity: 128, parallelism: 4 },
    slots,
)?;
let mut routing = RoutingConfig::default();
// Declare concrete contracts and register their actual receiving handlers before
// freezing the bus. Family membership is explicit native metadata, not reflection.
routing.grouping().rule(GroupingRule::properties(vec!["orderId".into()])?)?;
routing.grouping().rule(GroupingRule::tenant())?;
// A topology contributes routes through the same compiled publication resolver.
routing.topology(
    "order-topology", MessageSelector::family("orders")?, vec![], vec![],
    vec![EndpointAlias::try_new("orders")?],
)?;
let explicit = DispatchOptions {
    group: Some(GroupId::new("order-42")), partition_key: None,
};
// builder.local("orders", queue); builder.handle(...); build_routed(routing);
// runner.attach(&mut host, "orders"); then poll host.run().
// bus.send_with_options(&message, trusted_origin, explicit).await;
# let _ = (queue, runner, explicit);
# Ok(()) }
```

## Resolution

`send`, `publish`, `send_to`, their additive `_with_options` variants and canonical
JSON `receive` all use the same compiled resolver before buffer custody. The
explicit destination bypasses publication selection, not grouping/protection or
capability checks. Configuration starts no tasks. Existing ordinary queues and
default envelopes retain their legacy behavior and encoded bytes.

The bus's Invoke/scoped paths refuse a grouped contract rather than running it
outside its buffered slot owner. The independent low-level caller-polled Runtime
is unchanged and does not interpret grouping configuration.

The first match wins: explicit/stamped group, the unique matching topology's
ordered rules, then additive application rules. A topology miss falls through;
it never hides global inference. Exclusions are checked before topology inclusion.
More than one matching topology defining rules is rejected for handled contracts
at build, and for publish-only contracts at send. Runtime no-match/explicit group
cannot bypass that ownership check. Concrete/family/predicate exemptions append;
they bypass only local sequencing, not stamping or tenant binding.

Property rules distinguish absent (fall through), present null (matched empty)
and empty string (matched empty). Property names are tried in declaration order.
Strings retain their spelling; other JSON values use compact native JSON text,
not arbitrary foreign `ToString()`. Custom and typed accessors must be synchronous,
pure and bounded; panics become input-local failures. Known protected framing is
refused before typed, property or custom access. Explicit decryption/canonical
ingress must finish first. Payload/header tenant values never authenticate origin.

`GroupingRule::inferred` registers an application's native saga or stream/aggregate
identity accessor. `sequenced` registers its order accessor; a missing order
derives a stable independently distributed group from tenant/message ID, not the
shared empty group. A genuinely absent group distributes using the message ID
without inventing a logical identity. No CLR/Marten inference is implied.

## Execution and cancellation

One `Arc<SlotSet>` is the actual owner. Same-group jobs never execute concurrently;
different groups on different slots can overlap up to queue parallelism. Hash
collisions serialize unrelated groups deliberately. The versioned native hash
uses tenant/group/domain; it is not a foreign hash-compatibility promise. Reusing
the same slot owner across queues shares process-local exclusion; identical
counts/hashes on independent owners do not. Multiple processes remain independent.

Exempt messages can overlap nonexempt messages with the same group when capacity
permits. Only exempt types with no ordering requirement relative to those writers.
Inline partitioning fails during construction. Raw `submit` on a partitioned
queue is refused because opaque futures have no group identity: use the canonical
envelope submission API or the application bus.

Waiting capacity covers channel contents, pending jobs and reserved send permits
together. Busy-slot waiters do not occupy handler concurrency. Accepted runnable
cold-slot and exempt work can pass a hot-slot backlog; work not yet accepted can
still face ordinary finite-buffer backpressure. No state grows per historical key.

A handler sending recursively to its own full slot backlog also backpressures:
waiting work cannot run until that handler releases the slot. Bound or cancel
that send instead of waiting indefinitely; an abandoned pending send enqueues
nothing, and shutdown closes waiting admission while draining accepted jobs.

Pause retains accepted work without starting new handlers. Shutdown closes
admission and drains accepted buffered work even when paused, including sends
through previously approved permits. Completion requires receiver exhaustion,
no pending jobs and joined active handlers. A waiting caller can cancel without
enqueueing; once accepted, the host owns the work even if its sender disappears.
Panic/failure/timeout releases execution ownership and does not cancel siblings.
Ordered effects must remain inside the handler future; detached tasks or external
effects are not fenced by an in-memory slot. Start the receiving owner before
relying on drain; stopping a host before its services start is not a running
receiver's graceful shutdown.
Forced host-grace expiry cannot promise completion or undo external side effects.
Buffered acknowledgement precedes execution; process death can lose buffered work.

## Wire compatibility and unavailable profiles

The optional versioned `dispatch` envelope field retains resolved-none separately
from unresolved input and empty group. `EnvelopeMetadata` and `QueueSettings`
literals are unchanged. `metadata()` is still the legacy metadata subset; codecs
requiring dispatch must use the explicit companion construction path. `rebody`
and correlation preserve dispatch. Projections retain supplied/stamped group and
outgoing key before final-branch preparation. Changed dispatch changes new content
fingerprints; dispatch-absent content fingerprints are byte-identical to legacy.
Old readers reject new dispatch-bearing envelopes: upgrade readers before opt-in
emission; do not assume transparent mixed-version operation.

This API currently supports **partitioned buffered canonical JSON receivers**.
Durable staging (including direct PostgreSQL/SQLite staging), policy queue
admission, atomic returned intents, remote/native-ack destinations, selected
serialization/body-only/CloudEvents encoding and encryption of dispatch-bearing
envelopes refuse it explicitly. They never silently discard metadata or pretend
to provide ordering. Grouping configured against unsupported routed endpoints
fails before admission. Ungrouped uses of those APIs are unchanged. Atomic
returned-intent refusal also applies to children of a dispatch-bearing context;
in-memory queues cannot join an SQL commit.

The propagation registration requests a policy for returned children; building
with it is refused until prepared returned custody exists. Dispatch-bearing
atomic returned work remains refused. Explicit outgoing keys
remain independent metadata and never populate a SQL predecessor key. Applications
must not treat buffering as durable successful-order retention or automatic retry.

Prepared durable manifests/preclaim slot admission, batch/unbatched shared custody,
concurrent policy execution, broker native-ack ownership and all ten global shard
transports require their own integrations and qualification. Local slot execution
does not certify any of those guarantees or literal .NET compatibility.
