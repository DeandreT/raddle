# Architecture

## Boundaries

raddle is embedded in an application, not a broker, ORM, service locator, or
separate workflow service. Business code consists of ordinary typed async
handlers. Applications retain ownership of state and explicitly assemble
adapters. Runtime reflection, runtime compilation, and automatic discovery are
not part of the design.

The `raddle` crate owns local dispatch and public execution contracts. Persistence,
transport, and HTTP integrations belong in separate crates that depend on the
core, never the reverse. Backend interfaces are introduced alongside a real
implementation rather than guessed in advance.

## Local execution contract

Each request type has one handler, reply type, and domain error type. Event
subscriptions have stable names, independent outcomes, and shared read-only
access to a single event allocation. Closures can capture application state;
there is no dependency-injection container.

The runtime bounds the number of active handlers across requests and event
subscriptions. Waiting futures remain caller-owned; there is no unbounded
internal task queue or detached worker. Dropping an execution future cancels its
unfinished handlers and releases admission permits. It cannot undo effects a
handler already performed. Closing admission rejects new and waiting handlers;
running handlers finish unless their callers cancel them. The application joins
its own work for graceful shutdown.

Event results are returned in registration order, even though subscribers can
run concurrently and finish out of order. A domain failure does not cancel
siblings. A panic still unwinds normally; raddle does not catch panics. Recursive
invocation through the same saturated runtime can deadlock; execute a shared
function directly or use separate capacity instead of re-entering dispatch.

## Durable execution design

PostgreSQL and SQLx are the first persistence target. Handler execution uses the
application's actual database transaction, including business writes, inbox
completion, saga state, outgoing messages, local subscriber jobs, and timers.
There is no hidden second transaction. Caller-owned transactions require an
explicit finalization boundary before commit and rollback after failure.

Durable ingress accepts custody by committing an inbox record before settling
the broker delivery. Each local subscriber owns a separate persisted execution
record. Outbox publishers claim bounded work, await broker acceptance, and then
mark delivery complete. A crash after acceptance can cause republishing. Delivery
is at least once, not exactly once; external effects need idempotency keys or
explicit compensation.

Stable contract names, schema versions, subscription identities, and routing
snapshots are persisted independently of Rust type paths. Retry, lease, expiry,
ordering, quarantine, and replay policies are explicit. Fenced claims and
database ownership checks prevent stale workers from committing database work.
No fence can make arbitrary external effects transactional.

## Integrations

- `raddle-postgres`: SQLx scopes, versioned migrations, inbox/outbox and recovery.
- `raddle-servicebus`: AMQP 1.0 Service Bus queues and subscriptions, settled
  sends, lock renewal, sessions, authentication, and bounded flow control.
- `raddle-axum`: the same execution contracts behind HTTP, with generated or
  implementation-checked OpenAPI for every exposed route and error.
- `raddle-testkit`: deterministic clocks, failure injection, and adapter checks.

Azure and Switchyard are independently qualified. An emulator result does not
establish Azure compatibility. RabbitMQ's AMQP 0-9-1 is a different adapter.
SQLite targets embedded/single-node deployments. Backend-specific capabilities
must be preserved rather than silently reduced to a common subset.

Sagas are versioned persisted state machines with explicit correlations and
timeout generations, not replayed Rust call stacks. Distributed request/reply
separates ephemeral waiters from durable conversations. A timeout means no result
was observed, not that the remote operation failed. Reply destinations are
configured and authorized, never arbitrary inbound URLs.
