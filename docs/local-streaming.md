# Local bus streaming

`MessageBus::stream` executes a locally registered `Request` whose reply implements
`Stream<Item = Result<T, M::Error>>`. It returns a lazy `LocalStream`: constructing
it does not execute the handler or poll the source. Each requested item polls the
source directly, with no buffering queue, background producer or eager collection.
The handler can return an unfinished source, so the consumer need not wait for the
whole sequence before receiving its first item.

`InputStream<T, R, E>` is a local request with one reply `R` and typed error `E`.
Register its handler with `RuntimeBuilder::handle`, then call
`MessageBus::invoke_stream`. The handler consumes the request directly as a
fallible stream. It must propagate or explicitly handle source errors; the bus
does not silently discard errors or retry partially consumed input.

Neither operation requires `WireMessage`. Both use the existing runtime's typed
registration and admission. A missing local handler is `Unregistered`, even when
the bus has matching remote routes and a remote invoker. HTTP SSE and transport
streams are separate contracts; these operations never serialize a stream or
fall back to a remote endpoint.

## Ownership and failure

- Output setup uses `StreamOptions::timeout`, starting when the caller first
  polls. Once the handler returns its source, item production and consumer pauses
  have no implicit timeout. Use the explicit cancellation signal to end a source.
- Input aggregation uses one timeout for admission, handler execution and all
  source consumption. Budgets must be nonzero and at most one day.
- Cancellation takes priority over the next item. One terminal error is returned,
  then the output stream ends. Handler failures retain `ExecutionError`; source
  failures use `StreamError::Source`.
- Dropping output drops pending setup or its returned source. Dropping input
  execution drops the handler and its source. Cancellation and errors release
  their owned state immediately; they do not cancel healthy neighboring work.
- Runtime admission covers output handler setup, not a returned source's lifetime.
  The consumer owns active streams and any resource/concurrency limits across
  them. Input aggregation retains its runtime permit until it finishes or drops.
- Dropping Rust state cannot undo side effects or join tasks an application
  detached itself. Sources must be cancellation safe. There is no durable custody,
  implicit transaction, retry, authenticated identity or authorization added here.
  `StreamOptions::context` supplies diagnostic runtime metadata only.

Run the application-owned example with:

```sh
cargo run -p raddle-messaging --example local_streaming
```

These are Rust-native local streaming contracts, not CLR API or foreign-wire
compatibility claims.
