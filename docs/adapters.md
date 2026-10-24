# Persistence and transport adapters

Each adapter retains its native transaction and protocol guarantees. They are
not interchangeable connections to one uniform broker.

Kafka and JetStream are separate expansion decisions: their offset, ordering,
and acknowledgment models require their own adapters and qualification, not a
configuration switch on RabbitMQ or Service Bus.

## SQLite

`raddle-sqlite` is for embedded, single-node installations. `Store::open` enables
WAL, full synchronization, foreign keys, and a bounded busy timeout; migration
remains explicit. A supplied application pool must enforce its own equivalent
settings. Keep the database on a local filesystem, not a network share.

`Scope::enroll` uses the actual application SQLx transaction. `Store::begin`
starts `BEGIN IMMEDIATE`, avoiding read-to-write upgrade races. Unlike PostgreSQL
row locks, an execution reserves SQLite's single writer, across every tenant.
Keep handlers short and free of network effects. Use `scope.tenant()` and
`scope.envelope()` for the authenticated custody scope and original metadata.

Local delivery, atomic fan-out, outbox, keyed scheduling, bounded attempts,
generation-fenced takeover, expiry, quota, audited replay, and explicit completed
tombstone retention are supported. Replay is also subject to admission capacity.
Workers are caller-owned futures. Dropping an execution rolls back its business
writes; the claimed attempt remains recoverable after its lease expires.
Claim leases range from one millisecond through two hours; shorter leases are
refused rather than silently rounded to zero by timestamp storage.

PostgreSQL's saga/conversation and HTTP idempotency tables are not implicitly
created in SQLite. Use PostgreSQL when those capabilities or parallel writers
are required. Expired/dead work is retained for inspection; deleting completed
tombstones ends their duplicate-detection protection.

## RabbitMQ

`raddle-rabbitmq` uses AMQP 0-9-1, not Service Bus's AMQP 1.0. Applications supply
existing exchanges, bindings, and queues. Production connections require TLS;
plaintext is permitted only for numeric IPv4/IPv6 loopback fixtures.

Publishers resolve configured aliases, mark messages persistent, require
publisher confirms, and set `mandatory`. A confirmed return from an unroutable
publication is a rejection, not successful delivery. Persistence also depends on
durable broker topology and the selected queue type. An interrupted confirmation
is uncertain: retry the same envelope identity and deduplicate at custody.

Receivers use explicit acknowledgment and a finite prefetch window. Commit
durable custody before `complete`. `reject(Requeue::No)` applies the queue's
configured dead-letter policy; without that policy the broker discards the
message. Closing a receiver returns outstanding deliveries to the broker.
RabbitMQ does not offer the Service Bus per-message lock renewal or session
contract. Use configured routing/keyed local execution rather than pretending
that those native features exist here.

For redelivery, preserve immutable routing metadata: for immediate inbox custody
use the original `envelope.created_at()` as the delivery due time, not a fresh
wall-clock value on every attempt. The source and tenant come from trusted
listener configuration/authentication, never from message headers.

## Qualification

Normal workspace tests are offline except real-file SQLite tests. PostgreSQL
and RabbitMQ integration tests are opt-in and must target disposable resources.
The RabbitMQ suite creates uniquely named queues and exercises confirms,
mandatory returns, prefetch, settlement, and a complete PostgreSQL durable
request/reply workflow. Set `RADDLE_RABBITMQ_TEST_URI` and `DATABASE_URL` through
the test environment and run:

```sh
cargo test -p raddle-rabbitmq -- --ignored
```

These results do not certify Service Bus or Azure; their independent conformance
gates remain separate.
