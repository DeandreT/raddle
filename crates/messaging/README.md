# raddle-messaging

Explicit application messaging above the unchanged caller-polled `raddle`
runtime: bounded host-owned queues, local-first invocation with configured native
request/reply fallback, trusted message context, typed scoped factories and
transactional returned effects.

See [the message bus contract](../../docs/message-bus.md) for routing, deadlines,
queue ownership, transaction phases, cancellation, post-commit outcomes and the
remaining behavior boundaries. This crate is not a claim of full Wolverine or
.NET wire compatibility.

## Local streaming

The application bus also supports caller-polled typed output streams and
input-stream aggregation. Both require local registration and never fall back to
remote routing. See [local streaming](../../docs/local-streaming.md) for source
ownership, backpressure, cancellation and timeout boundaries.
