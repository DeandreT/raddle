# raddle-grpc

Typed raddle requests over standard protobuf gRPC/HTTP/2, with application-owned
bearer verification, exact contract/version and permission allowlists, bounded
messages and metadata, admission budgets, deadlines and cancellation.

The stable [`raddle.v1.Dispatch`](proto/raddle/v1/dispatch.proto) unary service
carries versioned JSON application schemas inside protobuf bytes. It is not a
generated per-command protobuf schema. Register `Authenticated<M>` handlers in
the ordinary bounded runtime and expose selected `M: Request + WireMessage`
contracts through `GrpcBuilder`.

Listeners, TLS, credential verification and task lifetimes remain application
owned. There is no automatic server, durable acceptance, retry or discovery.
See [the gRPC guide](https://github.com/DeandreT/raddle/blob/main/docs/grpc.md) for the protocol and security contract.

MIT licensed.
