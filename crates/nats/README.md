# raddle-nats

Explicit Core NATS and JetStream messaging. Core socket flushing is unconfirmed;
JetStream publishing waits for the broker's correlated PubAck. Neither attests
business completion or application database custody.

Configure trusted client/tenant namespaces separately from envelope headers.
Broker deduplication is scoped and opt-in, with the broker's finite window.
Consumers retain native receipts across decoding failures and require explicit
settlement after handling or inbox commit. Applications own lease renewal and
host lifecycle; this crate does not start a hidden receiving worker.

See the repository's [streaming transport guide](../../docs/native-streams.md)
for provisioning, scheduling, lease limits, security and interoperability bounds.
