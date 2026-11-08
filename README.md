# raddle

Rust-native messaging, durable execution, and application orchestration.

raddle is an embedded framework: applications own their handlers, state, runtime,
and deployment. Its APIs make delivery guarantees and transaction ownership
explicit. It is MIT licensed and uses stable Rust.

## Local execution

The `raddle` crate provides typed local requests and independent event
subscriptions. It owns no background tasks: callers poll execution futures and
retain responsibility for joining their tasks. The local runtime is volatile;
it does **not** persist messages, retry handlers, or promise delivery after a
process exits.

```rust
use std::convert::Infallible;

use raddle::{Request, RuntimeBuilder};

struct Greeting(String);

impl Request for Greeting {
    type Reply = String;
    type Error = Infallible;
}

async fn greet(request: Greeting) -> Result<String, Infallible> {
    Ok(format!("Hello, {}!", request.0))
}

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut builder = RuntimeBuilder::default();
builder.handle(greet)?;
let runtime = builder.build();
assert_eq!(runtime.invoke(Greeting("world".into())).await?, "Hello, world!");
# Ok(())
# }
```

Run the complete request/event example with `cargo run -p raddle --example local`.
See the crate documentation for concurrency, fan-out, cancellation, and shutdown
contracts. [Architecture](docs/architecture.md) describes the adapter boundaries.

## Durable execution

`raddle-postgres` provides explicit SQLx transaction enrollment, durable local
queues and outbox delivery, named fan-out, delayed work, bounded retries, fenced
claims, tenant budgets, audited replay, and explicit tombstone retention. See
[PostgreSQL integration](docs/postgres.md) for ownership and failure guarantees.

Wire messages have stable names and schema versions, validated envelopes,
bounded metadata, and explicit upcasters. These are separate from local Rust
type identities. A confirmed transport still provides at-least-once delivery;
external side effects require idempotency.

Persisted sagas and durable request/reply conversations share those transaction
boundaries. A bounded ephemeral reply registry is also available for callers
that do not need restart recovery. See [orchestration](docs/orchestration.md).

## Application framework

See [capabilities and ownership](docs/capabilities.md) and the
[composed application walkthrough](docs/application.md) for the full execution path.

Typed [pipelines](docs/pipelines.md) add ordered async middleware, validation,
explicit context, bounded cascading commands/events, and owned retry decisions.
Ordinary dispatch retains its original typed reply and never silently retries
side effects or executes returned work.

[`raddle-host`](crates/host/README.md) supervises explicitly registered
services with bounded restart policies, generation-safe readiness, cooperative
shutdown and an enforced drain deadline. PostgreSQL worker/outbox helpers and
explicit RabbitMQ topology declarations compose with application-owned state;
see [hosting](docs/hosting.md).

PostgreSQL adds [recurring interval schedules and typed saga dispatch](docs/recurring.md),
[cluster membership and fenced singleton agents](docs/cluster.md), and
[event streams, aggregate rehydration and durable projections](docs/events.md).
Business state and these durable effects use the same SQLx transaction. A failed
independent message, schedule or projection stream does not require stopping
unrelated work. Database fencing does not make external side effects transactional.

## Network interfaces and operations

`raddle-axum` adds authenticated typed HTTP requests, durable idempotent
acceptance, and opt-in operational endpoints. Its OpenAPI 3.1 document is generated
from the same registrations and checked against actual responses. See
[HTTP integration](docs/http.md).

HTTP also supports explicit typed path/query/header/JSON binding, bounded SSE,
per-identity ingress limits, private conditional GET revalidation,
authenticated readiness and payload-free tenant queue views. The
[`raddle-grpc`](crates/grpc/README.md) adapter exposes typed registrations
through a documented Protobuf JSON bridge over standard gRPC, with application-owned
authentication and listener configuration. The
[`raddle-admin`](crates/admin/README.md) CLI provides read-only diagnostics
and explicitly confirmed, audited tenant-scoped replay.

## Storage and transports

`raddle-sqlite` supports single-node durable queues with explicit writer
serialization. `raddle-rabbitmq` supports AMQP 0-9-1 confirms, mandatory routing,
and bounded explicit-ack consumption. See [adapter guarantees](docs/adapters.md).

Run the [SQLite durable walkthrough](crates/sqlite/examples/sqlite_durable.rs)
against a dedicated example file, or the
[PostgreSQL purchase/receipt walkthrough](crates/postgres/examples/postgres_durable.rs)
against an explicitly confirmed isolated database. Both retain the same effects
when rerun. See [running the walkthroughs](docs/walkthroughs.md) for commands and
safety boundaries.

`raddle-servicebus` supplies AMQP 1.0 connections, confirmed publication,
peek-lock settlement, and session receivers. See [Service Bus integration](docs/servicebus.md)
and [vendored dependency provenance](THIRD_PARTY.md). This adapter is not
publishable to a registry while it requires the vendored client fixes; Git
consumers retain the patched source.

Local Service Bus conformance can run against both Switchyard and
[Microsoft's official emulator](conformance/microsoft/README.md), without Azure
access. These checks do not claim live Azure identity or network qualification.

## Development

`raddle-testkit` provides deterministic clocks, bounded transport fault scripts,
and shared custody checks. See [qualification](docs/qualification.md) for fuzzing,
database, feature-isolation, and native platform gates.

The repository pins Rust in `rust-toolchain.toml`. Run:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --all-targets --locked
cargo doc --workspace --no-deps --locked
```

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Work is tracked in GitHub issues and
delivered through pull requests, qualified locally against their exact head.
