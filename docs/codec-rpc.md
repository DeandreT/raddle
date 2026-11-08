# Selected native request/reply

Enable `raddle-messaging/serialization-rpc` and `raddle-tcp/serialization` for the
explicit plaintext native RPC v2 profile. `MessageBus::invoke` still prefers a
typed local handler; only a missing local registration uses the remote invoker.
Existing constructors without `with_selected_rpc` retain native v1 JSON behavior.

## Configure both peers

1. Register the canonical request `M` and its `M::Reply` in `Registry`, with real
   typed wire models and converters for the chosen media. Select the endpoint's
   default media. JSON, MessagePack and protobuf all require explicit pairs.
2. Build an `Arc<EndpointCodecs>` and a `SelectedRpc` for each peer. Call
   `register::<M>()` for every accepted request/reply pair. This checks the exact
   selected media/canonical contract/version registrations, not invented default
   instances. Empty configurations, duplicate pairs and missing M/R codecs fail.
3. Configure the caller's production request `Publisher` using
   `with_codecs("requests", Arc::clone(&codecs))`, and the server's request
   `Receiver` using `with_codecs(Arc::clone(&codecs))`. Pass the publisher into
   `ReplyHubInvoker::new`, then opt in with `with_selected_rpc(profile)`.
4. Keep the server's **reply alias publisher and caller's reply receiver on the
   ordinary JSON envelope path**, without TCP body-codec selection on that alias.
   Build `RemoteResponder::new` with this publisher, opt in using a separate
   `SelectedRpc` with the same pairs/media, then call `handle::<M>()` against its
   typed local runtime. No private framework wrapper registration is necessary.
5. Register the route and invoker on `BusBuilder`; own both listeners and their
   cancellation/join/settlement. Authenticate and authorize ingress independently,
   and feed replies to `accept_reply` with the independently trusted origin.

For example, the typed profile configuration (shared by the caller and server) is:

```rust,ignore
let mut profile = raddle_messaging::SelectedRpc::new(Arc::clone(&codecs));
profile.register::<MyRequest>()?; // MyRequest::Reply must also be registered
let invoker = ReplyHubInvoker::new(request_publisher, 64, "replies", responders)?
    .with_selected_rpc(profile)?;
```

`SelectedRpc` configures the framework protocol, **not** a hidden transport
builder. It cannot inspect arbitrary `Transport` wrappers. The application must
enforce the alias settings above, and must refuse selected/encrypted transport
combinations at startup. An inbound mode/media declaration establishes neither
physical wire encoding nor authentication. A configured TCP path supplies the
actual RDTC2 request encoding; a canonical legacy TCP request with a forged mode
header is not proof of RDTC2. If physical protocol enforcement is required,
provide that additional ingress policy; generic `MessageContext` does not carry
an authenticated transport-profile identity.

## Protocol and failure boundaries

Requests carry the existing configured reply alias and fresh request token, plus
`raddle-reply-mode: selected-v2:<media>`. Before running a business handler, a
selected responder requires an exact configured mode and typed pair, validates
the canonical request's selected projection/bounds, and checks the reply alias
allowlist and token. A legacy responder refuses an explicit mode declaration;
a selected responder refuses legacy, unknown and mismatched modes. There is no
automatic version negotiation, downgrade, resend or media fallback.

Replies use `raddle.native-reply` version 2 with JSON outer payload:

```json
{"mode":"selected-v2:binary/messagepack","response":{"status":"success","body":[82,68,83,66,1]}}
```

The body shown is only the framing prefix, not a complete reply. A real success
contains the complete RDSB1 header/metadata and **actual selected typed R bytes**.
The JSON byte array is a protocol carrier, not the application's representation.
MessagePack/protobuf are produced from their registered typed models, not by
wrapping an unchanged JSON R body. Failures use the explicit
`{"status":"failure"}` response, never serialized remote exception objects.

`accept_reply` checks the expected trusted source, tenant, fresh correlation
nonce, original request causation, frame version, exact mode/media and expected
canonical R contract/version and final typed R model. It decodes and applies
codec canonicalization once, then validates the canonical reparse/typed handoff
before handing content to `ReplyHub`. Missing/malformed
success bodies, unknown media/schema, wrong identity, trailing/truncated bytes,
protected root frames and output bounds fail without consuming the waiter.
Later valid replies can still finish that same invocation. Headers or inner
metadata cannot upgrade the supplied `TrustedOrigin`.

Both the selected encoded body and post-upcast canonical envelope obey their
`EndpointCodecs` limits. The outer protocol and internal canonical handoff each
obey the ordinary native envelope limit (1 MiB/32 headers). JSON byte-array
expansion makes the effective application limit smaller than a bare envelope;
there is no padding or truncation to fit it. Callback/serde allocations remain
trusted application work, not a hostile-code allocator sandbox. Codec errors
expose fixed, payload-free messages.

Drop, explicit caller cancellation, timeout and failed request publication
release both registrations and retain the caller-owned transport/listener.
Saturation fails admission without sending another request. A timeout does not
undo remote work. Encoding or publishing a response after business execution can
fail or have an uncertain result: `respond` returns that error, does not report
success and never automatically reruns the handler. The ingress owner must not
blindly replay an already-handled business operation on reply failure; use external
idempotency/reconciliation for at-least-once delivery. No durable RPC recovery or
exactly-once business guarantee is added.

## Supported boundary and evidence

`crates/messaging/tests/codec_rpc_tcp.rs` uses the actual MessageBus local-first
remote fallback, configured RDTC2 request publisher/receiver, configured
RemoteResponder and JSON outer reply aliases across two independent OS processes.
Its transparent loopback proxies capture actual socket frames and assert raw
JSON, MessagePack and protobuf request and R-body representations. Malformed
neighbor replies and business failures leave healthy current/subsequent requests
working. The original `tcp_request_reply.rs` remains the independent v1 regression.
Lifecycle/adversarial tests are in `codec_rpc.rs`, including actual CryptoCodec v2
reserved-root refusals when `crypto-ingress` is also enabled.

These fixtures use synthetic application-configured loopback trust, not real
credentials or authentication implemented by raw TCP. They establish this native
profile, not every opaque generic transport, Wolverine/.NET wire compatibility,
foreign generators, all serialization-family parity, or encryption composition.
Existing `EncryptionCodec` v2 retains its JSON inner-body profile; do not stack it
with selected RPC/TCP codecs or treat a plaintext codec as a decrypting ingress.
