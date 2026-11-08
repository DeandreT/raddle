# raddle-serialization

Typed JSON, MessagePack and protobuf body codecs, application/endpoint selection,
explicit canonical projections, and binding-independent CloudEvents 1.0 mapping.
The native envelope remains canonical JSON for handler and storage identity;
binary bytes come from registered Rust models, not a generic JSON value.

See [the serialization guide](../../docs/serialization.md) for configuration,
framing, bounds, allocation trust and precise compatibility limitations. TCP uses
these codecs only with its optional `serialization` feature and explicit endpoint
opt-in. Messaging's `serialization-ingress` feature enables pre-custody decoding.
Neither feature changes existing defaults or authenticates envelope metadata.

No Newtonsoft, MemoryPack, Wolverine, CLR type activation, broker framing or .NET
interop is implied. Licensed under the repository's MIT license.
