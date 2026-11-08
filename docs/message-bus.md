# Application message bus

`raddle-messaging` is an explicit application layer above the low-level runtime.
It does not change `Runtime::invoke`, `Runtime::publish`, or the caller-driven
`cascade` API. In particular, an ordinary low-level invocation still returns
effect intent without executing it.

## Routing and invocation

`BusBuilder` freezes configured local, PostgreSQL-durable and confirmed-transport
endpoints and exact wire-contract routes. No runtime scanning, service locator,
reflection, arbitrary object interpretation, or generated code is involved.

`MessageBus::invoke` checks the typed local runtime first. Only a missing local
registration can fall back to remote request/reply. Remote invocation requires
one configured remote destination and an enabled `RemoteInvoker`; zero or
multiple routes are errors. Local domain errors retain their concrete types.
Remote negative acknowledgements are `BusError::RemoteFailure`, not successful
responses or reconstructed remote exception objects.

The default total invocation budget is 60 seconds. A global timeout,
`message_timeout::<M>`, and a per-call override have increasing precedence.
Remote response waiting defaults to five seconds and is also bounded by the
total budget. Admission and middleware count toward the deadline. Dropping a
low-level/remote invocation cancels its future, not remote business work.

`Send` requires a route; `Publish` without routes succeeds. Both use the same
route registrations as `intent`. Buffered sends return after bounded memory
custody, not handler completion. Inline sends finish processing before return;
durable sends return only after committed inbox custody. Remote sends await
transport confirmation, not remote handler completion. Fanout admissions are
independent: an admission failure cannot recall an already accepted sibling.
Use transactional outgoing intent when all-or-nothing SQL custody is required.

`MessageBus::receive` requires an explicit `IngressDecoder`. Decrypt, validate
and upcast inside that boundary, preserving the original envelope identity and
canonical content, before routing or any durable comparison/write. `JsonIngress`
is only a bounded plain-JSON decoder; encrypted ingress requires a decrypting
adapter. Origin is supplied independently and never decoded from ciphertext or
headers. Direct `dispatch` expects an already canonical application envelope.

Durable admission uses `Scope::stage_received`: concurrent/later retries retain
the original arrival time and compare canonical content plus other delivery
options, not randomized ciphertext or a new receipt timestamp. A changed body
still conflicts. Expiration comparisons use PostgreSQL timestamp precision.
The existing `Scope::stage` retains its strict immutable scheduling fingerprint.
The listener owns acknowledgement and must not acknowledge failed decode/custody.

## Named local queues and host ownership

`LocalQueue::new` returns a cloneable sending handle and one `QueueRunner`.
Attach the sole runner to `Host`. Construction and registration start no tasks.
Memory capacity and execution parallelism are separate bounds; parallelism one
is sequential. Each queue has its own admission, pause/resume and counters.
Pause retains accepted buffered jobs; resume is idempotent. Buffered shutdown
closes admission and drains accepted jobs, including when paused. Inline calls
remain caller-owned and must be joined by their listener/application. A handler failure
does not stop healthy siblings. Buffering is volatile and process exit can lose
accepted work. A failed receiver is not silently rebuilt; host supervision
reports failure rather than manufacturing a fresh empty queue.
Closing buffered admission also waits for previously admitted channel reservations
to send or release their slots. An empty buffer alone is not a completed drain;
active work continues to be polled while those reservations are outstanding.
Explicit cancellation is counted separately from successful jobs and handler
failures; a message timeout does not cancel its host or healthy sibling attempts.

Durable queue aliases use `BusBuilder::durable` and a matching
`DurableEndpoint`. Register its typed handlers before attaching it to `Host`.
It claims only its own subscription, and pause/resume never creates another
receiving owner. Endpoint execution and memory-queue admission are independent.
The SQL worker keeps successful inbox completion across failed sibling retries.
Malformed persisted envelopes or origins quarantine only their delivery. Lost
ownership is not a successful acknowledgement. Failure settlement requires an
unexpired lease and cannot release a successor's lease. Other store failures are
reported after healthy claims in the current batch finish. `run_once` counts
observed claims, not completions.
Host stop cancels unfinished durable pre-commit work, joins cleanup, and leaves
the uncompleted lease recoverable. This is not cluster ownership, leader election
or broker-native acknowledgement orchestration.

## Context and resources

Authenticated ingress explicitly supplies `TrustedOrigin`. Creating that value
is the application's trust assertion, not authentication performed by raddle.
`MessageContext` carries that origin separately from the original envelope,
untrusted headers, correlation, sampled UTC time and cooperative cancellation.
Executions through a bus also expose that actual typed bus in context;
`context.returned(message, required)` resolves child intent using its routing and
header policies without dispatching before commit. Bind the same frozen bus on
a `DurableEndpoint` with `message_bus` when its handlers need contextual routing.
There is no conversion from diagnostic `ExecutionContext` into authentication.
Binding a header can return Missing or Invalid; it never authorizes a tenant.
Outgoing propagation copies only allowlisted headers, preserves explicit
overrides and adds correlation/causation. A propagated identity-looking header
still is not a trusted origin.

`ScopedHandler<M>` has associated `Resources`, `Reply`, and `Error` types.
`BusBuilder::scoped` registers it on a buffered endpoint and returns a typed
`ScopedRegistration`. Routed Send/Publish and `MessageBus::invoke_scoped` use the
same handler, resources and effect interpretation. `invoke_scoped` preserves the
concrete reply/error types without reflection. Ordinary `MessageBus::invoke`
is the local-runtime/remote-wire request path; it does not reinterpret arbitrary
legacy runtime reply types as transactional effects.
`create` constructs new resources for each attempt using the same SQL session
as later phases and effects. Typed loaded state lives in `Resources`; shared
singleton dependencies are ordinary explicit fields. `dispose` asynchronously
consumes resources. `MessageBus::execute` requires a separate buffered execution
queue so the host, rather than an abandoned caller, drives Finally/disposal.
Dropping the caller signals cancellation without dropping host-owned cleanup.
Each scoped execution derives its own child cancellation signal. Caller/host
cancellation flows down to every linked child, but completion, a domain failure,
timeout or abandonment of one execution never cancels its parent or siblings.
Normal completion disarms the abandoned-caller guard rather than cancelling an
already completed execution's signal.
Partially completed construction owns its own cleanup until it returns Resources.
Process death or forced expiry of the host's shutdown grace cannot guarantee
asynchronous cleanup; Rust Drop is the final fallback.

## Phases and transaction results

The normal order is:

1. Create, Before, Load, Validate, Handle.
2. Execute returned Inline/External effects with the same resources and SQL scope.
3. After, PostProcess, then stage all returned outgoing intent.
4. Commit business writes, inbox completion and child inbox/outbox jobs together.
5. Confirmed outbox flush, then AfterCommit without the completed transaction.
6. Finally, then asynchronous disposal.

After resources are constructed, pre-commit failures roll back, call OnException,
then Finally and disposal. Failed construction has no returned resource scope;
the factory owns cleanup of partial construction.
The deadline applies to pre-commit work, including queued admission time.
Once a SQL commit has begun, execution joins its actual outcome rather than
reporting cancellation as if rollback were certain. SQL session acquisition,
cleanup and commit can delay the final response beyond the handler deadline;
expired attempts do not enter the handler after the session becomes available.
SQL commit I/O
failure can be uncertain; reconcile using message identity.

`Committed<R>` separates the successful root from `post_commit` failures.
Neither flush failure nor an AfterCommit/disposal error replays the committed
parent. `ConfirmedFlush<T>` uses the normal fenced outbox worker, then verifies
that every remote receipt job completed before permitting AfterCommit.
`LocalOnlyFlush` rejects remote jobs instead of pretending persistence is broker
confirmation. A flush pass is bounded; pending jobs remain independently
retryable. Local children are never awaited by the parent.

AfterCommit callbacks themselves are not persisted or resumed after a crash or
pending flush. Put required recoverable work in returned durable outgoing intent,
not only in a callback. OnException is an observational typed failure hook, not
a general exception filter, recovery/swallow policy or retry-action graph.

## Explicit returned work

`HandlerOutput` separates the reply from `Effects<Resources>`; a reply is not
also published. `Effect::Outgoing` stages independent child jobs atomically.
`Effect::Inline` executes in the root transaction and fails the root on error.
`Effect::External` makes the nontransactional risk explicit: rollback never
reverses a network or file action. External actions need their own idempotency.

`Effects::returned` interprets explicit optional/single/tuple/vector/iterator
wrappers, bounded to 256 effects. `Effects::from_stream` collects a fallible async
stream under cancellation and the same bound before any effect executes.
There is no partially acknowledged stream: a collection failure rolls back the
root. This is effect-return enumeration, not bus input/output streaming.

`StorageAction<T>` distinguishes Insert, Update, Store, Upsert, Delete and Nothing.
Store means update-existing, not implicit upsert. `Effect::storage` executes an
explicit typed repository adapter using the enrolled session; Nothing does not
call the repository. Repository adapters must implement or reject each operation
and enforce their own affected-row/concurrency rules. This is not an ORM or
automatic support for every storage provider.

`MessageBus::intent` resolves the same application routes as Send/Publish and
rejects buffered/inline destinations, which cannot participate in SQL commit.
Custom explicit `Outgoing` values are also supported for scheduled or
destination-specific `Delivery` settings. Their correlation/causation is set to
the originating context when staged. They must use the same PostgreSQL store;
there is no distributed transaction spanning independent stores.

## Native transport request/reply

`ReplyHubInvoker<T>` implements `RemoteInvoker` over the public confirmed
`Transport` contract. Configure aliases, an expected authenticated responder per
alias, a bounded waiter capacity and a reply endpoint. The application owns its
listener and passes independently authenticated replies into `accept_reply`.
The expected tenant, responder, reply contract and fresh per-attempt nonce must
all match. Dropping a wait releases capacity; late and duplicate replies are
ignored. A new attempt gets a new nonce even if its business message ID is reused.
Positive replies must also contain a valid inner envelope with the requested
contract name before the waiter is consumed. Malformed or wrong-contract inner
replies leave that waiter and its capacity reservation intact; a valid negative
acknowledgement still completes the wait with `RemoteFailure`.

`RemoteResponder<T>` explicitly registers typed request/reply pairs against a
local runtime. It validates the request's reply alias against its configured
allowlist and emits a correlated positive/negative acknowledgement. Settle an
incoming request only after the response transport confirms. Authentication,
authorization, listener ownership and settlement remain explicit ingress duties.

The native reply frame and headers are a raddle protocol, not Wolverine wire
compatibility. The reply frame uses bounded JSON and can have a smaller effective
payload limit than a bare envelope. This native foundation does not establish
.NET integration, conventional discovery, all endpoint modes, batching, cluster
or complete Wolverine behavioral parity.
