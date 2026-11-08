# raddle-mqtt

Caller-polled MQTT 5 messaging with explicit protocol acknowledgements. QoS0 is
unconfirmed; confirmed publishing requires positive QoS1 PubAck or QoS2 PubComp.
Protocol QoS does not establish exactly-once business handling or database custody.

Applications own event-loop progress and shutdown. Connection uncertainty fences
the session and its held receipts; replace it rather than letting reused packet
IDs settle new work. Invalid envelopes retain source custody. Quarantine snapshots
use explicit framing so empty native poison can be inspected without being
mistaken for a retained-message deletion.

See the repository's [streaming transport guide](../../docs/native-streams.md)
for session recovery, snapshot decoding, security and interoperability boundaries.
