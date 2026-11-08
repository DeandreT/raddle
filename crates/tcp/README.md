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
