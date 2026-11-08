# Typed body serialization

`raddle-serialization` separates a typed wire model from the canonical application
envelope. The envelope's body remains JSON for existing typed handlers and storage
identity. A registered codec encodes its **typed model**, not a `serde_json::Value`
mistaken for a MessagePack or protobuf schema. No Rust/CLR runtime type name is
activated from input, and erased registrations return validated `Envelope`s,
never `Any` payloads.

## Registration and selection

```rust
use std::sync::Arc;
use raddle::wire::WireMessage;
use raddle_serialization::{EndpointCodecs, Limits, MessagePack, MessagePackLayout, Registry};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Order { number: u32, note: Option<String> }
impl WireMessage for Order {
    const NAME: &'static str = "orders.received";
    const VERSION: u32 = 1;
}

let mut app = Registry::default(); // JSON default, no implicit unknown types
app.json::<Order>()?;
app.register::<Order, Order, _, _, _>(
    MessagePack(MessagePackLayout::Positional), Ok, Ok,
)?;
app.set_default("binary/messagepack")?;
let app = Arc::new(app);
let ordinary = EndpointCodecs::new(Arc::clone(&app), Limits::default())?;
let mut json_endpoint = EndpointCodecs::new(app, Limits::default())?;
json_endpoint.set_default("application/json")?;
# Ok::<(), raddle_serialization::Error>(())
```

Registration does not change the default. JSON registration before or after a
binary selection retains that binary default. Each registration is keyed by
media type, native contract and positive version; duplicate keys fail. A local
registration overrides that exact key without mutating shared application state.
Incoming lookup is local first, application second. Unknown/missing media or
type/schema fails at this wire boundary; there is no permissive fallback to a
different decoder. Media types are explicit lowercase tokens without parameters;
charset/parameter negotiation is not implemented.

`register<WireModel, CanonicalModel, ...>` requires explicit trusted converters in
both directions. Wire and canonical models can be different (including differently
named JSON properties and custom text). A `BodyCodec<M>` implementation must consume
the entire body, respect bounds and return typed errors. Canonical-to-wire
conversion also refuses canonical members silently ignored by the canonical
serde target. Applications must reject projections that lose information: examples
include non-JSON native values, unknown fields that must survive,
or graph identity not represented by the chosen contract. A callback returning a
different model intentionally is not proof of a lossless universal conversion.
The native JSON/canonical serializers explicitly refuse non-finite floats even
inside nullable values and collections, rather than silently emitting JSON null.

`EnvelopeMetadata`, `Envelope::from_parts` and `Envelope::rebody` preserve the UUID,
creation time, independently optional correlation and causation, headers and schema.
Construction is validated; deserializing metadata alone does not admit a message.
An optional endpoint canonicalizer can apply explicit `wire::Codecs` migrations and
`rebody` before custody. It cannot change identity/time/correlation/causation/headers.
The existing upcaster's 32-step guard remains distinct from codec registration.

Canonical `content_bytes()` is independent of body encoding and is compared only
after decoding/projection. Crypto key/nonce rotation likewise must return canonical
plaintext before custody; codec registration does not silently compose encryption
with serialization. Applications choose that order through `IngressDecoder`.

### Framework request/reply boundary

Application-body registrations do not automatically register the private framework
reply wrapper used by `RemoteResponder` and `ReplyHubInvoker`. Keep their outer
reply publisher on JSON. With messaging's explicit `serialization-rpc` feature,
`SelectedRpc` selects genuine registered application request/reply bodies and a
versioned native v2 reply frame containing an RDSB1 application envelope. See the
[native codec RPC guide](codec-rpc.md) for configuration, trust and limits.
Untouched APIs remain native v1 JSON, including existing decrypting ingress paths.
The new selected RPC profile is plaintext only, not a binary/crypto composition.

`EndpointCodecs::supports(media, contract, version)` checks an exact **canonical**
contract/version key, just like `encode_body` and `decode_body`: endpoint-local
registration wins, otherwise application registration is inherited. Invalid or
missing keys return false. It does not execute converters, fabricate a default
model, mutate registrations or fall back to JSON. Availability does not establish
that a specific value converts successfully, that any transport uses this codec,
or that a peer is authenticated/authorized. `SelectedRpc::register::<M>` uses this
lookup to check actual selected request and reply registrations before operation.

### Encryption composition is explicitly unsupported

The selected-body profile is **plaintext only**. Existing `EncryptionCodec` v2
authenticates its inner content type but encodes its actual inner body as JSON.
It does not protect arbitrary MessagePack/protobuf bytes. Do not stack
`EncryptedTransport<Publisher>` around a publisher configured with body codecs,
or substitute a body decoder for a required decrypting ingress policy.
Applications must reject that unsupported combined configuration at startup.
The current generic `Transport` API cannot inspect an outer wrapper at construction;
there is no advertised combined host/startup builder or working binary/crypto chain.

`UnsupportedProtection` is a payload-free boundary refusal for the reserved
root `raddle-aes...` frame namespace, before selected publication and before returning
canonical ingress (including permissive JSON-value models). A configured TCP
selected profile also applies its plaintext guard to legacy requests/replies, so
RDTC1 compatibility cannot pass ciphertext into that profile's consumer. This is a
composition guard, not authentication, decryption or a universal detector of
application-encrypted blobs. Trusted converters remain trusted application code.

JSON and MessagePack inspect the incoming root frame before target-model
deserialization. Optional/defaulted serde models cannot erase the protected-frame
marker to bypass this guard. MessagePack inspects raw root-map key/value scalars,
independently of unknown-field preservation; nested application maps and positional
arrays are not interpreted as protected-envelope declarations.

Default TCP without body-codec configuration retains its existing JSON-envelope
path. An explicitly decrypting/receive-required `EncryptionCodec` ingress can keep
that path and must finish authentication/decryption before bus custody. Plaintext
must still fail its receive-required policy. Selected body codecs with crypto
remain an open native composition clause, not global serializer/crypto closure.

## Body profiles

- **JSON** (`application/json`): typed serde UTF-8; accepts a UTF-8 BOM. Duplicate
  object members, malformed UTF-8, trailing bytes and target errors fail. JSON null
  is supported for explicitly nullable models. There is no untyped object-activation
  API. Explicit primitive/array types can be registered; that is not the released
  Newtonsoft untyped `JObject` behavior.
- **MessagePack** (`binary/messagepack`): genuine `rmp-serde` typed bytes. Structs
  can use positional arrays or property-name maps; tuple/record and explicit
  integer-key map models retain their declared serde shape. Nested nullable values,
  Unicode and collections are supported. A nil **whole message** fails. Extensions,
  compression, dynamic formatters and schema negotiation are unsupported. The
  preflight scanner checks declared lengths, full consumption, depth and nodes
  before typed deserialization. This safer bounded profile is not MessagePack-CSharp
  Standard/TrustedData/resolver or attribute identity.
- **Protobuf** (`binary/protobuf`): genuine `prost::Message` bytes with an explicit
  language-neutral schema/field numbers, not protobuf JSON or protobuf-net RPC.
  Empty proto3 messages can encode to zero bytes, but this boundary deliberately
  refuses empty reads, matching the released wrapper's asymmetry. A field-framing
  preflight rejects invalid keys/wire types, truncated/oversized declared lengths
  and malformed trailing data. Unknown *valid fields* follow prost decoding and
  may be discarded; schemas requiring unknown-field preservation need an explicit
  projection/profile. A different registered target is refused by the registry;
  protobuf itself cannot distinguish two semantically different schemas that use
  identical field numbers/wire types. Groups are unsupported. Nested schema decoding
  uses prost's recursion guard, not the configurable canonical depth setting.

Independently authored language-neutral test schemas live in
`crates/serialization/tests/fixtures/messages.proto`. Native tests use matching
prost derives and specification-derived byte vectors; this is not execution of a
foreign generator or counterpart.

Dependencies remain open-source: rmp-serde 1.3.1 and resolved rmp 0.8.15 are MIT;
prost 0.14.4 is Apache-2.0; base64 0.22.1 is MIT OR Apache-2.0. No proprietary codec
or foreign implementation is copied. The crate itself remains MIT.

## Bounds and allocation trust

`Limits` separately bounds encoded bytes, whole canonical envelope bytes/headers,
canonical/JSON/MessagePack depth and nodes, and declared lengths. Encoded inputs are
bounded before parsing. MessagePack collections/strings and protobuf top-level
length-delimited fields are scanned without length-sized allocations. JSON parsing
has serde_json's recursion guard and an encoded-byte bound, then configurable
depth/node checks before typed decode. Canonical values are checked after callbacks.

These are admission bounds, **not an allocator sandbox**. Custom codecs, converters,
serde implementations and protobuf schema decoders can allocate application memory
before returning; protobuf embedded byte fields are schema-dependent, not recursively
guessed by the generic preflight. Canonical serialization/validation can also allocate
before its output-size check. Do not register untrusted code or claim hostile-code
bounded-memory behavior from these settings.
Custom synchronous callbacks are not preempted by an asynchronous socket timeout.

## Opt-in messaging and TCP

Messaging's optional `serialization-ingress` feature implements `IngressDecoder`
for `EndpointCodecs`. `MessageBus::receive` finishes decoding/canonicalization before
route execution or durable identity comparison. The caller retains acknowledgement
responsibility and supplies an independently authenticated `TrustedOrigin`.

TCP's optional `serialization` feature adds `Publisher::with_codecs(alias, codecs)`
and `Receiver::with_codecs(codecs)`. Outgoing selection affects **real socket bytes**.
An untouched publisher alias remains RDTC1 JSON. Configured receivers retain RDTC1
clients and accept selected RDTC2 frames; unconfigured receivers refuse RDTC2.
The selected body codec/canonicalizer applies to RDTC2; RDTC1 keeps its existing
JSON-envelope boundary, without applying the selected-body registry/canonicalizer.
When a selected profile is configured, its plaintext-only validation still applies
to legacy compatibility traffic. Unconfigured legacy/crypto behavior is unchanged.
There is no protocol fallback/resend after an uncertain result. Both requests and
replies use the selected path; replies still require correlation and causation.

RDTC2 changes only the explicit connection version to `RDTC\x02`. It retains the
existing message-count, length-prefixed framing and per-item status acknowledgements.
Each selected frame is `RDSB\x01`, big-endian u32 JSON-header length, JSON header
`{ "metadata": EnvelopeMetadata, "content_type": "..." }`, then the **raw body**.
The outer frame remains nonempty even for a zero-byte protobuf body. Entire frames,
headers, declared lengths and canonical output remain bounded. Encoded outgoing
batches are retained once, within the batch-byte budget, to prevent a custom codec
changing bytes after size accounting. Raw TCP still supplies no authentication/TLS.

Malformed bounded bodies receive per-item refusal and allow later valid frames;
invalid lengths/framing close the connection rather than draining untrusted input.
Retain observed successes, retry only failed items with original identities, and
declare durable custody only after inbox commit. No codec adds an order-level retry
or whole-batch transaction policy. PostgreSQL deduplication remains tenant/source
scoped and compares canonical content, not binary bodies or receipt times.

## CloudEvents 1.0

`cloudevents::Mapper` supports structured `application/cloudevents+json` and generic
binary HTTP/Kafka attribute mapping. It does **not** configure broker clients or
acknowledgements. Kafka attributes use `ce_`; HTTP uses case-insensitive `ce-`.
HTTP 1.0.2 attribute values use UTF-8 percent encoding and single-pass percent
decoding after legacy quoted-string unescaping. Invalid UTF-8/escapes and decoded
control characters fail. Kafka attributes stay ordinary UTF-8, without HTTP
percent encoding. Attribute strings are at most 2,048 bytes; HTTP wire values
allow their percent-encoding expansion plus legacy outer quotes.
Binary bodies are actual codec bytes; structured JSON uses `data` and binary data
uses strict padded `data_base64`. Supplying both is refused, including JSON null.
Conflicting/duplicate attributes, malformed UTF-8/base64, missing type/specversion,
unknown aliases and reserved native/trust metadata fail before admission.

`Aliases` maps arbitrary external event-type strings to validated native contracts
and versions. Non-UUID event IDs and optional occurrence times are preserved in
`EventMetadata`. The application **must** supply a deterministic, collision-safe
source/id-to-native-identity projection, with a stable creation time even when event
time is absent. There is deliberately no random UUID or receipt-time default.
Choose a durable mapping if a deterministic hash is inappropriate. Reusing a native
identity for different source/id pairs is an application configuration defect.

`MappedEvent` returns the canonical envelope **and separate typed external metadata**.
The mapper enforces its own canonical-envelope limits on the final decoded output,
after endpoint conversion and upcasting, even when the endpoint uses larger limits.
Endpoint limits still apply independently; both boundaries must accept the message.
Persist/forward that metadata separately when needed after custody; the existing
native inbox stores only its canonical envelope. External source/subject/extensions
never populate `TrustedOrigin`, native headers or privileged correlation fields.
Outgoing standard CE mapping refuses nonempty native headers/correlation/causation
rather than silently losing them or inventing private attributes. Structured scalar
extension types are retained; binary extension values are textual because their type
cannot be inferred safely without an application extension schema. Header/attribute
counts and encoded-byte/value bounds apply independently of canonical headers.

## Compatibility boundaries

These native profiles do not close whole serialization or broker-family parity rows.
`SERIALIZER-SELECTION` and `COMP-F01-C10` describe native endpoint/body/conversion
behavior; adapter-specific delivery modes still need their own qualification.

`INV-SERIALIZER-MESSAGEPACK` retains C# resolvers/attributes/custom formatters and
effective-option callback behavior as separate requirements. Rust's explicit layout
enum is not evidence a .NET immutable `With...` callback changed its active options.
`INV-SERIALIZER-PROTOBUF` retains Google.Protobuf descriptor/generated-type identity,
foreign schema evolution, actual callback behavior and bidirectional counterpart
tests. Protobuf bytes alone do not establish those APIs.

`INV-SERIALIZER-MEMORYPACK` remains open: no MemoryPack semantic layout, generated
formatter, version evolution or byte compatibility is implemented here. No different
binary encoding is advertised as MemoryPack. `INV-SERIALIZER-NEWTONSOFT` also remains
open for shared references/cycles, polymorphic graph profiles, CLR `$type` handling,
named converters/settings and the separate HTTP/null-response integration. Ordinary
typed JSON and custom converters are only the explicitly documented native subset.

The `broker-wire-interop-mapper-family` portable slice is standard CE mapping and
aliases with independent vectors. Standard CloudEvents is not Wolverine private
headers, MassTransit/NServiceBus outer envelopes, raw broker mode or per-broker
producer/consumer interoperability. Kafka/other adapter wiring, retry/DLQ fidelity,
counterpart tests and every named .NET library/API identity remain separate gates.

Standards references: [CloudEvents HTTP binding 1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/http-protocol-binding.md)
and [Kafka binding 1.0.2](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/kafka-protocol-binding.md).
