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
contracts. [Architecture](docs/architecture.md) describes the boundaries for
durable execution and additional adapters.

## Development

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
