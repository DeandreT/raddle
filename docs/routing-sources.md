# Declared native publication routing

`raddle_messaging::routing::RoutingConfig` opts an application into layered
publication routing through `BusBuilder::build_routed(config)?`. This fallible
build freezes source results, delivery plans and readable `bus.explain::<M>()`
explanations together. It starts no tasks, opens no connections and provisions
nothing. Send, Publish, canonical ingress and returned durable intents consume
that same snapshot; diagnostics never rerun discovery callbacks.

## Concrete declarations and sources

Declare messages with `MessageDeclaration::of::<M>(MessageRole::Outgoing)?`,
`Handled` or `PublishOnly`, then `config.declare(declaration)?`. Optional
`.module("orders::events")?` and `.family("order-events")?` labels are explicit
native selection metadata. They do not replace concrete stable contract names
or positive schema versions, and they do not discover Rust types or CLR assemblies.
Duplicate declarations are rejected, including conflicting concrete ownership.
Declare every publication-only contract needed in the startup topology.

`MessageSelector::exact::<M>()`, `family`, `module` and `predicate` select only
declared messages. Module inclusion means exact label or `label::descendant`,
not a sibling textual prefix. Predicates inspect declared metadata, not payloads,
origin or an application service locator. `config.rule(id, description, selector,
targets)` adds an explicit rule to configured `EndpointAlias` targets.

Custom `RouteSource` callbacks receive one declaration and return at most 16
configured aliases. Register them with `config.source` or `config.convention`.
Sources/predicates must be pure, deterministic startup functions. Callbacks are
evaluated for validation even when precedence suppresses their targets. Failure
or panic rejects startup; invalid suppressed receiving targets cannot hide behind
a higher-priority source. No source can create a receiving owner or broker resource.

Limits are 1,024 contracts (including legacy routes), 256 bus endpoints, 32 total
rules/sources/transforms, 16 family labels per declaration, 16 outputs per source,
16 effective deliveries per original contract and 16 projection graph vertices
per path. Cycles are rejected even when their routes are suppressed. IDs follow
the existing bounded subscription-name syntax; descriptions are 1–512 bytes and
module labels at most 256 ASCII identifier/`::` bytes.
Built-in IDs `transforms`, `explicit`, `local`, `legacy-explicit` and `direct`
are reserved. Explanations include a readable source-description map for selector
rules and typed projections as well as ordered stage decisions.

## Precedence and continuation

Effective order is custom sources, typed transforms, explicit bindings/rules,
conventional locals, then external conventions. There is no implicit agent-command
or transport-listener discovery subsystem.

Each source first contributes its routes. Non-pass-through contribution counts
accumulate **before** delivery deduplication. A non-additive source stops the chain
when that accumulated count is nonzero, even when the source itself found nothing.
Thus an ordinary `SourceOptions::ADDITIVE` custom route stops at the next empty
terminating built-in. `PASS_THROUGH` adds routes without influencing that check.
`TERMINATING` stops immediately when accumulated blocking contributions exist.
No-match sources do not manufacture routes. Transforms whose destination contract
has no effective routes contribute nothing, allowing explicit/local fallback.

Transforms and explicit routing terminate. `LocalRouting::Terminating` is the
default; `Additive` continues into conventions and `Disabled` omits conventional
locals. Disabling locals never disables explicit routes to a local endpoint.
Conventions are additive. Identical alias/projection paths deliver once, in first
occurrence order, retaining all contributing source IDs in their explanation.
Different projection paths are different deliveries, even at the same alias.
Explanations describe routing, not successful conversion, custody or processing.

## Legacy and receiving ownership

`build()`, `route::<M>()`, ordinary `handle`/`scoped`, `HandlerSet::register` and
`sticky_handler` retain their existing behavior. Exact routes are explicit even
when their endpoint is local. Legacy handler-created routes remain pinned in
that same explicit fan-out. Adopting `build_routed` does not silently turn a
legacy separated remote+A/B topology into remote-only delivery. Runtime families
retain their concrete request/reply/error contracts; they are not wire family IDs.

Only `config.local_route::<M>(alias)` and new `register_routed` /
`sticky_handler_routed` registrations create conventional-local provenance.
Configure sources and referenced aliases before routed handler registration.
Install pre-built overrides before adding rules for receivers that a subsequent
HandlerSet will create; each routed operation validates its complete current snapshot.
The routed APIs clone both builder and configuration and swap them only after
preflight succeeds. Selected classic/sticky/separated chains use the existing
engines, policies, endpoint deadlines and explicit returned queue owners.
Successful registrations retain prior validated receivers for the same concrete
contract/version; later independent pre-built overrides do not replace them.
All candidate receiving targets are considered, including currently suppressed
ones. Separated targets need an explicit group binding or pre-built override;
they never gain a duplicate global chain. A later unbound receiver fails final
build. Keep and attach every returned runner/owner to the application host.

Plain manually configured durable endpoints remain application-owned: register
their actual typed handlers separately and bind the frozen bus when those
handlers return contextual work. A bus alias is not proof of an external
receiving worker. No generic transport or policy registry is inspected.

Invoke is intentionally different from publication: it remains local-first, then
requires exactly one legacy exact remote alias. The operation-specific invocation
view is frozen in the same resolver and visible in explanations. Selectors and
projections do not change RPC cardinality, concrete replies or selected-RPC codecs.
`send_to(alias, message, origin)` explicitly selects a configured alias without
publication selectors/projections; untrusted endpoint URIs are not accepted.

## Plaintext typed projections

`config.transform::<From, To>(id, version, map)?` registers a pure synchronous
`From -> Result<To, RoutingError>` publication conversion. Declare both contracts
first. This is not upcasting, transparent forwarded RPC, a Marten `IEvent<T>`
identity, or a generic encryption/selected-body composition facility. Failure or
panic belongs to that input. Independent destination admissions still continue;
a returned transactional unit instead fails atomically before commit.

Projection inspects the actual canonical payload **before** deserializing `From`.
Root `format` strings in the reserved `raddle-aes` namespace are refused even if
the typed model would silently discard encrypted framing. The same check applies
to projection outputs. Explicit decrypting ingress may return correctly decrypted
canonical plaintext for projection. This marker guard neither decrypts nor
recognizes every possible external encrypted format.

Native projected UUIDs use a fixed v1 derivation: SHA-256 over
`raddle.publication-projection.v1\0`, followed by u32-big-endian-length-prefixed
authenticated source, tenant, original UUID bytes, original contract bytes and
original version bytes; then each projection's ID bytes, version bytes, target
contract bytes and target version bytes in path order. Use the first 16 digest
bytes, set UUID version 8 and the RFC variant. All integer fields are big-endian.
Trusted namespaces prevent independent roots converging on one egress from
colliding merely because their UUIDs match. This is identity separation, not
physical transport authentication. Bump a projection version when its semantics
change; never mutate persisted child jobs to apply a new projection.

Creation time is retained. Existing correlation/causation are retained; missing
ones reference the original envelope ID. Contextual outgoing work is already
correlated/caused by its executing parent, matching ordinary scoped staging.
Only allowlisted application headers propagate. Native RPC reply destination,
request token and reply-mode headers never propagate through a projection, even
if mistakenly allowlisted. No payload/header supplies trusted tenant or source.
Unchanged routes keep the original canonical envelope, including its identity.

## Durable intent and failure boundaries

`intent` still returns one `Outgoing`. A mixed original/projected plan requiring
several envelopes returns `RoutingError::MultipleEnvelopes`; it never drops a
branch. Use `intents` or `context.returned_many`, wrapping each result in
`Effect::Outgoing` under the existing 256-effect budget. Volatile Local/Policy
destinations are rejected rather than represented as SQL-atomic deliveries.

The existing scoped/composition engines stage the resolved envelopes/destinations
with business changes in the same transaction. Independent persisted destination
state retains completed siblings. Durable retry uses those saved bytes and routes,
not source callbacks or conversions. Lost acknowledgements remain uncertain;
receiver deduplication uses original retained identities. Volatile fan-out has no
persisted failed-only retry guarantee and resending it may repeat successful work.

## Native TCP walkthrough

Build `cargo build -p raddle-messaging --example routing_node`. Run
`routing_node receiver`, then `routing_node sender PORT` with its printed port.
The sender performs explicit-destination and publication-rule delivery. The
separate receiver enters actual `MessageBus::receive` and reports its business
handler's observed ID, contract and body. Send `stop` to receiver stdin to join
its owned listener. This loopback walkthrough provides neither peer authentication
nor durable custody. The PostgreSQL routing fixture separately exercises committed
returned work, failed-destination recovery and lost-ACK deduplication.

These are native RO02 and declared-metadata RO03 behaviors against Wolverine
V6.62.0's pinned public routing contract. Native TCP evidence does not qualify
other providers, .NET wire compatibility, literal CLR discovery, Marten wrapper
identity or a whole routing/compliance family.
