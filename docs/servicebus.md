# Service Bus transport

`raddle-servicebus` uses the AMQP 1.0 client in `azservicebus`. It supports
authenticated TLS or secure WebSockets, named outgoing endpoint aliases,
peek-lock queues and subscriptions, explicit message settlement and renewal,
and named session receivers with session renewal. It does not provision Azure
entities. Supply existing test or application queues/subscriptions.

Create a connection using a connection string supplied through an approved
secret mechanism, or an Azure token credential such as managed identity. Never
print credentials. The publisher maps only configured aliases to entity paths;
incoming reply headers cannot select arbitrary addresses.

Publisher success means broker acceptance, not consumer completion. Large SDK
futures are boxed at the integration boundary so callers do not inherit their
stack footprint. Independent outgoing entities have independent sender locks.
The conservative 240 KiB envelope limit leaves space for Standard-tier AMQP
framing. Sessions use the validated `session-id` envelope header.

Receiving does not imply acknowledgment. A `HeldMessage` remains bound to its
receiver until a successful complete, abandon or dead-letter operation. Decode
errors retain the lock so poison messages can be deliberately dead-lettered.
Commit database custody before calling `complete`; if commit or completion is
uncertain, retain the stable message identity and rely on inbox deduplication.

Close receivers and publishers before closing the shared connection. Reconnection
and bounded protocol retries use the client library; durable retry decisions
belong to the outbox rather than an unbounded send loop.

## Emulator and conformance

The opt-in `emulator` feature enables plaintext connections only to an explicit
numeric loopback address. It is not a production transport and cannot be enabled
silently through an inbound message or header.

The separate `conformance` workspace pins a Switchyard revision and its required
toolchain. It exercises actual AMQP sockets on both memory and durable backends.
Run from that directory with `cargo test -- --test-threads=1`. Its failures remain
visible; a Switchyard result never substitutes for Azure qualification.

An independent [Microsoft emulator fixture](../conformance/microsoft/README.md)
runs the same adapter contract against Microsoft's official Service Bus emulator
without an Azure subscription. It is opt-in and uses disposable loopback-only
containers. This supplies an additional broker implementation for local
compatibility work, while Entra ID, cloud networking, WebSockets, and actual Azure
service qualification remain separate from emulator results.

The narrowly patched vendored client is documented in `THIRD_PARTY.md`. Git
consumers retain the direct path dependency. Do not publish the adapter to a
registry with an unpatched dependency substituted for this source.
