# Application hosting and topology

`raddle-host` assembles ordinary async services into an explicitly polled Tokio
host. It is not a process manager, dependency-injection container, or deployment
service. Applications own configuration, migrations, transactions, broker
connections, signal handling, and dependency ordering.

## Supervision and readiness

Create `Host::new(grace)`, register uniquely named factories with `service`, take
a `HostHandle`, and await `run`. Registration starts nothing. Names follow
`SubscriptionName` (1–128 lowercase ASCII letters/digits, then `.`, `_`, `-`).
A host accepts at most 256 services; an empty host returns immediately and is
never ready. Handles observe services registered after the handle was created.

Factories receive a fresh `ServiceContext` for each attempt. Only
`context.set_ready(true)` marks that service ready: establish connections,
validate dependencies, and complete startup first. `set_ready(false)` withdraws
readiness. Old-attempt reports are ignored. A snapshot is ready only while the
host runs, shutdown has not been requested, and every service explicitly reports
ready. Per-service samples are not a transactional cross-service health probe;
applications must withdraw readiness when a dependency fails.

`ServiceFailure::Permanent` immediately stops the host. Transient failures may
consume an explicitly selected `RestartPolicy`: at most 32 restarts, exponential
delays bounded between 1ms and 60s, with no budget reset after a successful start.
Successful service completion is not restarted. Panic recovery requires
`PanicPolicy::Restart`; otherwise panics fail fast. Recovery uses Tokio's joined
task boundary and still consumes the same finite budget. It does not recover
process aborts or undo external effects. No hidden infinite restart loop exists.

## Shutdown and cancellation

`handle.shutdown()` requests shutdown through watch receivers. Service code can
await `context.cancelled()` or pass `context.shutdown()` to an existing worker.
On a terminal failure, the host also requests sibling shutdown. Each in-flight
batch should finish before returning. The grace period is validated at
1ms–300s; at its deadline remaining tasks are aborted and then joined. Every
normal `run` return, including error returns, has joined all host-owned tasks.
A terminal service error takes precedence over a sibling shutdown timeout.

Dropping/cancelling `run` requests shutdown and aborts its owned tasks, but Rust
drop cannot asynchronously join them. Await `run` rather than timing it out and
discarding it when joined shutdown matters. Service code must yield and must
not detach additional tasks: blocking code cannot be forcibly stopped by Tokio,
and children spawned outside the host are outside its ownership. Dropping a
control handle does not stop the host; shutdown is an explicit operation.

## PostgreSQL workers

`postgres_worker` hosts the actual `raddle_postgres::Worker` and its existing
transactional batch/run APIs. `postgres_outbox` polls the actual `OutboxWorker`
through bounded batches and confirmed transport publication. `PollSettings`
accepts 1–256 claims and a 1ms–60s idle interval. Both helpers report readiness
only after their first successful database batch. An idle outbox batch does not
probe broker health; applications needing transport-readiness checks should own
that startup check and readiness reporting explicitly.

The [executable PostgreSQL host example](../crates/raddle-host/examples/postgres_host.rs)
actually uses both helpers, with an explicitly confirmed disposable database.
Its intentionally unconfigured transport returns transient failure instead of
falsely acknowledging delivery. Production applications provide their own
configured confirmed transport and register transactional business handlers.
The SQLx integration test executes a real handler transaction and an outbox
publication, then verifies persisted completion before shutdown returns.

Forced cancellation can leave recoverable claims, and a broker acceptance
followed by lost completion can cause redelivery. Hosting does not strengthen
the existing at-least-once guarantees or make external side effects atomic.

## Explicit RabbitMQ topology

`raddle_rabbitmq::Topology::new(exchanges, queues, bindings)` validates a complete
plan locally before any broker operation. It accepts at most 256 combined
exchange/queue declarations and bindings. Names are 1–255 ASCII letters/digits,
`.`, `_`, or `-`, excluding RabbitMQ's reserved `amq.` prefix. Duplicate names or
bindings, unknown references, invalid keys, and oversized plans are rejected.
All references must point to declarations in the same plan.

Exchange kinds are typed `Direct`, `Fanout`, and `Topic`. Durability is explicit
on each queue and exchange. Queues are named, shared, non-exclusive, and not
auto-deleted; exchanges are not auto-deleted. Fanout bindings require empty
keys. Direct keys are exact; topic keys use dot-separated segments with whole
`*`/`#` wildcards. Arbitrary broker arguments and server-generated names are not
part of this bounded API.

```rust
use raddle_rabbitmq::{Binding, Exchange, ExchangeKind, Queue, Topology};

let topology = Topology::new(
    [Exchange { name: "orders".into(), kind: ExchangeKind::Topic, durable: true }],
    [Queue { name: "billing".into(), durable: true }],
    [Binding { exchange: "orders".into(), queue: "billing".into(), routing_key: "orders.*".into() }],
)?;
// Explicit startup step, before creating publishers and receivers:
connection.declare_topology(&topology).await?;
```

Compatible declarations are idempotent. Broker conflicts or network failures can
leave a partially declared plan: AMQP declarations are not transactional, and
raddle does not silently delete or roll back entities. Use application-owned
names and reconcile deliberately. Publisher and receiver creation never declares
topology implicitly. Mandatory publication still rejects unroutable messages.
This API manages RabbitMQ broker entities only; it never provisions Azure,
Service Bus, or other cloud resources.
