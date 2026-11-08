# raddle-kafka

Native Kafka publishing uses configured topic aliases and actual broker delivery
reports. The adapter disables automatic topic creation and does not implicitly
create or delete objects. Supply application-owned native TLS/SASL settings and
select a security protocol explicitly; raw record headers are not tenant or
authentication authority.

`Publisher::publish_record` preserves native binary keys, duplicate headers,
null versus empty header values and real keyed tombstones. `publish_keyed` sends
the existing raddle envelope without mutation. `raddle::Transport` success means
Kafka accepted the record with `acks=all`, not that a consumer handled it or that
replication exceeds the topic's configured in-sync-replica requirements.

Native delivery timeouts and cancellation are uncertain. Kafka idempotence is
opt-in and protects native producer retries, not separate application submissions
or new producer sessions. Retain the original message identity and use durable
inbox duplicate protection where required. Admission and native queue bounds are
separate so abandoned caller futures cannot build an unbounded native backlog.

## Caller-polled receiving

`Receiver` subscribes to configured topics with an explicit group. Its current
settlement profile is `CommitStrategy::PerMessage`; other offset strategies are
rejected by this receiver rather than approximated. `ConsumerSettings::native_config`
can independently construct provider options, but is not proof that every group
mode or broker profile has been qualified.

Poll `receive()` regularly while processing to service rebalance callbacks. It
returns one native record or `None` after a short poll, not an end-of-stream marker.
`Received::validate` checks application byte/header bounds. Invalid records retain
their offset barrier; durably quarantine them before calling `complete`. No
envelope header authenticates a source, tenant or group.

Call `complete` only after successful handling or durable inbox commit. Concurrent
completion order is independent of native offset order: a later record returns
`Deferred` until all earlier observed records have custody. Numeric offset gaps
do not require nonexistent records. A broker-confirmed safe prefix returns
`Committed`. Failed commits retain completed state; `flush` retries settlement
without repeating business work. Another receiver's or assignment's handles cannot
complete the current owner's records.

Canceled polling/commit waits retain their task and result in the receiver; a
successor call joins them before advancing. Dropping an uncompleted `Received`
does not acknowledge it: its partition remains blocked until recovery. Admission
has one additional bounded slot for incidental records around pause/rebalance.
At capacity, complete existing records before requesting more; this caller-polled
profile is not a continuously pumping, host-owned handler runner. Keep processing
within Kafka's configured maximum poll interval.

An opaque native polling failure returns `ReceiverFenced` and prevents subsequent
offset settlement. For example, a CRC failure can skip a native batch without
exposing that batch's partition/offset through the safe client API. Continuing to
commit later records would lose the failed batch. Stop and explicitly recover the
receiver instead. Ordinary application validation failures keep their known
per-record barriers and do not enter this shared native failure state. Partition-
scoped recovery for opaque SDK errors is not provided by this profile.

**Application limits are not a hard bound on native decompression memory.**
librdkafka decompresses batches before this adapter can validate or detach their
records; a small compressed batch can expand beyond the application record and
queue-byte settings. Use independently enforced decompressed-input bounds on a
trusted broker/producer profile. This adapter does not promise bounded native
memory for hostile compressed input; transport encryption does not solve that
allocation risk.

Call `Receiver::shutdown` to join pending native calls and close the consumer off
an async worker. Unfinished records remain uncommitted. Native shutdown/Drop itself
is not a hard-deadline guarantee. This native profile does not establish durable
key scheduling, replay/hot-tail, retry/DLQ topic workflows or schema interoperability.

The SDK owns native worker threads. Call `Publisher::shutdown().await` to flush
and destroy the producer off an asynchronous worker. Native destruction is not a
cancellable hard-deadline guarantee. A dropped send future does not cancel a
record already admitted by the SDK.

The package links bundled librdkafka through the safe `rdkafka` Rust API. Linux
builds use CMake, a C toolchain, zlib and OpenSSL development files. No .NET runtime
or Wolverine wire-compatibility claim is implied.
