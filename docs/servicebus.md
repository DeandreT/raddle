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

Settlement uses Service Bus management request/reply with the delivery's lock
token and associated receiver link (and session ID when applicable). Success
requires the broker's successful response, not merely enqueueing a link
disposition. Expired or rejected complete, abandon, and dead-letter requests
leave `is_settled()` false. Before each attempt, the client also rejects a lock
whose latest broker-supplied deadline has already passed locally: some brokers
acknowledge stale abandonment or dead-letter requests as no-ops. Renewal updates
that deadline; missing message deadlines still require broker confirmation.
Session settlement uses the receiver's session lease, including session renewals,
not the per-message annotation. Unbounded session-message sentinels and malformed
or out-of-range lock annotations supply no finite deadline and never panic.
If a response arrives after the known deadline, settlement remains uncertain
and is reported as an error even if the broker returned a successful status.
Keep the host's UTC clock synchronized. A timeout remains uncertain: the broker
may have acted before the response was lost, so deduplication is still required.

Management calls use unique request IDs and match the response correlation ID
before decoding its status or typed body. A late reply to a cancelled or timed-out
operation cannot confirm a later operation. Receiver recovery also invalidates
the local custody generation; an obsolete held delivery cannot be settled by
mistaking a successful stale-token no-op for confirmation.

Each bounded receive uses one total deadline across attempts, retry delays,
recovery, and credit cleanup. Recovery and its absolute retry deadline persist
across calls: a short poll interrupted during recovery returns an error, not a
successful idle result. It returns at most the requested count, including
when an earlier attempt acquired part of the batch. An idle deadline returns an
empty batch even for a wait longer than the ordinary SDK attempt timeout. A
partial batch is returned instead of hiding acquired locks behind a later
receive error; errors before acquiring any message remain errors. Prefetch can
hold additional locks on the receiver, but does not enlarge the returned batch.
Manual credit is revoked when a receive returns or is cancelled. If that flow
cannot be enqueued immediately, the link is discarded and the next receive must
recover it. This cleanup owns no background task; already acquired partial
batches remain visible even when their link's custody has become uncertain.

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
