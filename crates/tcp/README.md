# raddle-tcp

Bounded, raddle-native TCP messaging over application-configured host/port aliases.
It does not implement Wolverine's wire format. Raw TCP provides no encryption or
peer authentication; use an independently secured network and configure trusted
tenant/source state outside the envelope. A socket address is not authorization.

`Publisher` implements the confirmed `raddle::Transport` boundary. Success means
the remote `Consumer` explicitly declared custody, not that a broker persisted it
or a business handler succeeded. Durable consumers must commit their inbox before
returning `Delivery::Accepted`. A lost acknowledgement is uncertain: retain the
same message identity, retry only failed items, and deduplicate at the receiver.
There is no automatic resend on disconnect or built-in durable storage.

Run `cargo run -p raddle-tcp --example loopback` for an actual loopback request/reply
through the ordinary bounded raddle runtime. The opt-in PostgreSQL integration
test demonstrates returning custody only after inbox commit and retaining one
durable job per identity on corrected retries.

For PostgreSQL ingress, `Scope::stage_idempotent` can use the original message
UUID as its source-and-tenant-scoped key. It retains the first delivery plan even
when a retry arrives later. `Scope::stage` instead compares immutable routing and
schedule metadata as well as content; recomputing its due time on every retry is
an identity conflict, not equivalent repeated custody.

`publish_batch` retains independent item results, including successes before a
later failure. Per-message refusal and malformed bounded envelopes do not stop
subsequent messages. Invalid lengths or protocol framing close the connection.
One batch uses one new connection; bounded sequential request/ack exchanges avoid
large-reply socket deadlocks. `request` also checks reply correlation, causation
and the expected contract. These checks do not authenticate the network peer.

`Receiver::run` is caller-owned and takes a cancellation token. Connection count,
message count, aggregate batch bytes, envelope bytes and the total connection
deadline are bounded. Aggregate bytes include both requests and replies; a locally
oversized request batch is refused before opening its connection. A peer that
exhausts the receive budget loses the connection without further confirmations.
Shutdown aborts and joins in-flight custody futures; it cannot reverse already
committed or external side effects. Publishers fail fast at connection capacity.

## Native framing

Each connection starts with five bytes `RDTC\x01`, then a big-endian `u16` message
count. Each request is a big-endian `u32` nonzero byte length followed by a validated
JSON envelope. The receiver returns one byte: `0` custody accepted, `1` uncertain/
retry, `2` permanent refusal, or `3` a reply with the same length-prefixed framing.
The sender observes each acknowledgement before transmitting the next item.
Replies must correlate to and be caused by that request's identity. Unknown
statuses, truncation and deadlines never produce a positive acknowledgement.

Licensed under the repository's MIT license.

## Optional typed body codecs

The `serialization` feature adds explicit per-alias `Publisher::with_codecs` and
`Receiver::with_codecs` configuration using `raddle-serialization::EndpointCodecs`.
Selected connections use `RDTC\x02` with bounded `RDSB\x01` metadata/raw-body frames;
the existing RDTC1 JSON default is unchanged. Configured receivers also retain
legacy clients. Unconfigured receivers refuse the new connection version rather
than guessing body content. There is no automatic protocol fallback or resend.
Replies use the selected path and retain the existing correlation checks.

Outgoing encoded batches are retained once within the batch-byte budget so custom
codecs cannot change their output after size accounting. Selected decoding and
explicit canonicalization finish before the consumer can declare custody. See the
[serialization guide](../../docs/serialization.md) for framing, configuration,
allocation trust and compatibility limits. This remains raddle-native framing,
not Wolverine's TCP protocol or named .NET codec interoperability.
