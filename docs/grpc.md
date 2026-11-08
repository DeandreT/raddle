# Typed requests over protobuf gRPC

`raddle-grpc` exposes selected `Request + WireMessage` types through real
protobuf gRPC over HTTP/2. It uses Tonic's standard transport, generated services
and Prost codec. It is not a JSON-over-HTTP endpoint that imitates gRPC.
Applications own listeners, TLS, connection policy, credential validation and
server task shutdown. The adapter does not start a listener automatically.

## Stable transport contract

The included [`raddle/v1/dispatch.proto`](../crates/grpc/proto/raddle/v1/dispatch.proto)
defines one unary RPC, `/raddle.v1.Dispatch/Invoke`:

```proto
service Dispatch {
  rpc Invoke (InvokeRequest) returns (InvokeReply);
}
message InvokeRequest {
  string contract = 1;
  uint32 version = 2;
  bytes json = 3;
}
message InvokeReply {
  bytes json = 1;
}
```

The package is `raddle.v1`. These protobuf field numbers and route are the stable
generic transport contract. `contract` and positive `version` identify the
application's `WireMessage::NAME` and `VERSION`; JSON bytes follow that request's
Serde schema. Reply JSON follows `Request::Reply`. This bridge deliberately does
not generate a per-command protobuf schema. Non-Rust clients generate their
normal gRPC stub from the included `.proto` and implement the registered JSON
application schemas. The outer protobuf package version is separate from each
application schema version. There is no implicit upcasting, reflection, schema
discovery, streaming, durable acceptance or automatic retry.

## Typed application assembly

Register ordinary handlers for this adapter's `Authenticated<M>` wrapper, then
explicitly expose each wire contract and its required permission alias:

```rust,no_run
use std::convert::Infallible;
use raddle::{Request, RuntimeBuilder};
use raddle::wire::WireMessage;
use raddle_grpc::{Authenticated, Authenticator, GrpcBuilder, GrpcService};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Greeting { name: String }
impl Request for Greeting {
    type Reply = String;
    type Error = Infallible;
}
impl WireMessage for Greeting {
    const NAME: &'static str = "greeting.say";
    const VERSION: u32 = 1;
}

fn assemble(auth: impl Authenticator)
    -> Result<GrpcService, Box<dyn std::error::Error>>
{
    let mut handlers = RuntimeBuilder::default();
    handlers.handle(|request: Authenticated<Greeting>| async move {
        // Repository access must be scoped to request.principal().tenant().
        // request.context() carries the monotonic deadline/cancellation signal.
        Ok(format!("Hello, {}", request.message().name))
    })?;
    let mut grpc = GrpcBuilder::new(handlers.build(), auth).with_capacity(64)?;
    grpc.request::<Greeting>("greeting.invoke")?;
    Ok(grpc.build())
}
```

Add the returned service to an application-owned `tonic::transport::Server` with
`add_service`. The normal bounded `Runtime::invoke` path executes each registered
`Authenticated<M>` handler; no parallel executor bypasses runtime admission.
Missing runtime handlers are reported as unavailable, not dynamically installed.

The application chooses TLS settings and enables its Tonic TLS provider/features
when using `Server::tls_config` or a TLS client channel. Bearer authentication
does not encrypt credentials; use TLS across trust boundaries. Plaintext loopback
in tests is not production TLS qualification.

## Authentication and ownership

`GrpcBuilder::new` requires an `Authenticator`; there is no anonymous default.
Async closures accepting an owned token and returning
`Result<Principal, AuthenticationError>` implement it automatically. Verify the
credential's signature, issuer, audience, validity and application authorization
using the application's real identity system before constructing a principal.
Token presence or a caller-provided tenant is not verification. Tokens are not
logged, echoed, or included in domain/serialization error messages by the adapter.

`Principal::new` validates bounded tenant and subject identifiers and an explicit
list of permission aliases. Each is 1–128 printable ASCII bytes without spaces;
permission aliases use the core's lowercase wire-name syntax and at most 128
distinct entries. An empty permission list is valid but grants no invocation
rights. Aliases have exact equality semantics, not wildcards or patterns.

`request::<M>("permission.alias")` exposes only the exact registered contract and
version, requiring that exact permission from the verified principal. There are
at most 256 registrations. Registration rejects malformed names, zero versions,
duplicates and invalid policy before altering the builder.

`authorization: Bearer <token>` in the initial metadata is the only credential
input. Duplicate initial bearer fields and all trailing bearer fields are
rejected before credential verification, including trailer-only credentials.
Caller metadata such as `x-tenant`, `x-subject`, roles or
permissions is never mapped to trusted ownership. The wrapper's fields and
constructor are private; handlers obtain identity only from the verifier-created
principal. This adapter does not automatically filter SQL: application handlers
must use that trusted tenant when accessing tenant-owned data.

## Bounds and status contract

Each encoded protobuf request/reply is capped at **1 MiB**, excluding gRPC's
five-byte frame prefix. JSON has a slightly smaller limit, **1 MiB minus 256
bytes**, leaving bounded room for the protobuf envelope. Request decoding is
bounded before handler execution; response serialization writes through a
capped buffer rather than allocating an unbounded serialized reply. Serde's
normal JSON recursion limit remains enabled. Custom application serializers,
deserializers and handlers must remain cooperative and resource-conscious.

Initial metadata and request trailers share a limit of 32 fields and 8192 bytes,
counting names, values and 32 bytes of per-field overhead, including standard
transport fields. The service checks initial headers before decoding a body and
inspects trailer frames before Tonic merges their metadata. Repeated names across
initial headers and trailers each consume the budget; overwriting a name cannot
hide its original cost. Trailing credentials and timeout fields are rejected
before the authenticator runs. Other bounded request trailers remain supported.
The application should also set
`Server::http2_max_header_list_size` and connection/stream limits: HTTP/2 header
decompression occurs before the adapter sees a request. A transport-level header
rejection may reset the stream rather than return an adapter gRPC status.

Adapter statuses are fixed and do not expose domain errors, JSON input, runtime
type names or serializer error text:

- Missing/rejected credentials: `Unauthenticated`.
- Missing verified permission: `PermissionDenied`.
- Unknown contract or version: `NotFound` (after authentication).
- Malformed JSON or timeout metadata: `InvalidArgument`.
- Message, metadata or in-flight admission limit: `ResourceExhausted`.
- Domain rejection: `FailedPrecondition` with `request rejected`.
- Closed/missing runtime handler or unavailable verifier: `Unavailable`.
- Reply serialization/internal execution failure: sanitized `Internal`.
- Adapter whole-call deadline: `DeadlineExceeded`.
- Wrong canonical RPC route: `Unimplemented`.

Tonic's length-prefix decoder normally reports oversized messages as
`OutOfRange`; this unary service normalizes that decoder response to
`ResourceExhausted`, before returning it over the actual gRPC transport.

## Deadlines, cancellation and backpressure

The service defaults to 128 in-flight calls, shared across all its clones.
`with_capacity` accepts 1–65535; excess calls fail immediately, without creating
an internal queue. The permit covers body decode, authentication, runtime
admission and execution, and response production. The runtime's independent
shared concurrency limit still controls active business handlers.

`with_max_duration` accepts 1 ms–120 s (default 30 s). The whole-call budget starts
at service ingress, before protobuf decode and authentication. A valid standard
`grpc-timeout` can shorten it, never extend it. Tonic 0.14 has no public request
deadline getter, so the service parses the standard bounded 1–8-digit timeout and
unit (`H`, `M`, `S`, `m`, `u`, `n`) directly from ingress metadata. Zero means an
already-expired call. Malformed or duplicate timeout fields are rejected.

`Authenticated::context` provides a monotonic `deadline`, `is_cancelled` and an
awaitable `cancelled` signal. Deadline expiry or a cancelled HTTP/2 stream drops
the caller-polled runtime future, releases admission and signals that call's
context. Other calls and the runtime remain live. There are no detached handler
tasks. Tonic itself may win a transport-level timeout race and report `Cancelled`;
this still cancels the handler, not the entire service.

Timeouts are cooperative, not CPU preemption. Synchronous application code that
does not yield cannot be interrupted while it runs. Connection establishment,
TLS handshake and pre-service transport limits are owned by the application.
Cancellation does not undo effects already performed. A lost reply or timeout
does not establish that an operation failed; design idempotency at the business
boundary when retries are appropriate.

## Typed client and checks

`GrpcClient::new(channel)` uses an application-created Tonic channel, retaining
its TLS/connection policy. `invoke::<M>(&message, bearer, timeout)` serializes the
typed request, sends the standard protobuf RPC and deserializes `M::Reply`.
It applies the message bound, marks bearer metadata sensitive, sets standard
timeout metadata and also bounds its local wait. `protocol::dispatch_client`
exposes the generated standard client for other integrations and diagnostics.
Remote gRPC statuses pass through unchanged: another server's application-level
`OutOfRange` is not reinterpreted as a message-size failure. The server adapter's
decoder normalization is limited to errors with a known local decoder origin.

`cargo test -p raddle-grpc` uses real loopback sockets and HTTP/2, including the
generated client and an ordinary Tonic codec client. Tests cover verified versus
spoofed identity, exact contract/version routing, authorization, actual decoder
limit statuses, bounded metadata, malformed JSON, sanitized failures, deadlines,
HTTP/2 cancellation and shared admission/runtime concurrency.
Request-trailer tests also cover metadata overwrites, combined budgets and
credential ambiguity before authentication; an independent standard service
checks preservation of remote `OutOfRange` errors. No cloud resource,
external server or global protoc installation is needed. The build uses the
locked `protoc-bin-vendored` executable through Prost's safe configuration API;
it does not change process environment variables.
