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

`Receiver` subscribes to configured topics with an explicit group and custody
strategy. All four strategies disable native automatic storage of fetched
positions, including when the supplied native config requested it:

- `StoreThenAutoFlush`: store only a safe next-read watermark using plural
  `store_offsets`, enable native auto-commit at the supplied interval, and release
  local admission on successful safe storage. This returns `Stored`, **not**
  `Committed`; a later uncorrelated SDK callback never settles a new assignment.
- `PerMessage`: synchronously confirm the eligible completed prefix with Kafka.
- `BatchCount`: flush after the configured number of new custody completions since
  a successful flush, without recounting completed records left behind a barrier;
  also flushing safe progress at a full admission window so a threshold larger
  than capacity cannot stall indefinitely.
- `Interval`: flush after elapsed time, serviced by regular caller `receive`
  calls even on an idle topic or while admission is full. There is no independent
  application timer task; the caller must keep polling.

`flush` explicitly confirms all safe positions regardless of the count/interval,
including locally stored positions. Graceful `shutdown` performs this same flush.
An incomplete record always remains a barrier. Explicit commits use one partition
per native synchronous request so a partial failure cannot falsely acknowledge a
different partition. An uncertain result retains completed state for offset-only
retry; no handler is resubmitted by the receiver.

Failed offset settlement does not gate native polling. `receive` serves native
poll/rebalance events before retrying due safe positions, so a queued revoke can
complete and a denied partition cannot prevent fetching an independent partition
while admission remains available. Recoverable offset failures still return
`Uncertain` or `Stale`; any record fetched during that call stays in the bounded
incidental slot for the next `receive`. Retry receiving or flushing, not completed
business work. After assignment loss, broker redelivery still requires the
application's existing durable custody/deduplication policy.

Isolation defaults explicitly to `read_uncommitted`, matching the released
Wolverine contract rather than bundled librdkafka 2.12.1's `read_committed` default.
Set native `isolation.level=read_committed` to exclude aborted transactions and
wait behind open transactions. Kafka transactional producers are used only in
fixtures; no transactional consume-transform-produce/offset EOS engine is implied.

`ConsumerSettings::resolve` returns final provider options and typed warnings.
KIP-848 removes `partition.assignment.strategy`, `session.timeout.ms`,
`heartbeat.interval.ms` and `group.protocol.type`, one warning per explicit
removal, retaining static identity and unrelated options. Report these through
the application's diagnostics (`Receiver::configuration_warnings` retains them
after connection). Explicit classic/cooperative selection preserves compatible
classic settings; contradictory explicit protocols/assignors still fail. This
explicit membership API does not implement transport-to-endpoint inheritance or
automatic instance-ID discovery.

Poll `receive()` regularly while processing to service rebalance callbacks. It
returns one native record or `None` after a short poll, not an end-of-stream marker.
`Received::validate` checks application byte/header bounds. Invalid records retain
their offset barrier; durably quarantine them before calling `complete`. No
envelope header authenticates a source, tenant or group.

Call `complete` only after successful handling or durable inbox commit. Concurrent
completion order is independent of native offset order: a later record returns
`Deferred` until all earlier observed records have custody and the policy flushes.
Numeric offset gaps
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

Application ownership generations fence stale/foreign completion and preserve
unaffected partitions during incremental revocation. They are not broker member
epochs or leader epochs. The safe SDK does not expose consumed leader-epoch
propagation for computed watermark TPLs; no full KIP-320/log-truncation guarantee
is claimed.

An opaque native polling failure returns `ReceiverFenced` and prevents subsequent
offset settlement. For example, a CRC failure can skip a native batch without
exposing that batch's partition/offset through the safe client API. Continuing to
commit later records would lose the failed batch. Stop and explicitly recover the
receiver instead. Ordinary application validation failures keep their known
per-record barriers and do not enter this shared native failure state. Partition-
scoped recovery for opaque SDK errors is not provided by this profile.

The opt-in `tests/crc_wire.rs` requires the run-owned Kafka 4 wire proxy selected
by `python3 conformance/kafka/run.py test --mode crc` in the source checkout. It
checks a single CRC-field bit fault, an unfinished held-record settlement fence,
unchanged broker watermark and explicit unmodified redelivery/recovery. Its
test-only disabled-CRC control and raw native BadMessage observation distinguish
actual CRC rejection from application validation or a mocked SDK error. This
narrow PerMessage/classic Fetch-v4 profile does not qualify every Kafka mode or
remove the native allocation and teardown limitations below.

**Application limits are not a hard bound on native decompression memory.**
librdkafka decompresses batches before this adapter can validate or detach their
records; a small compressed batch can expand beyond the application record and
queue-byte settings. Use independently enforced decompressed-input bounds on a
trusted broker/producer profile. This adapter does not promise bounded native
memory for hostile compressed input; transport encryption does not solve that
allocation risk.

Call `Receiver::shutdown` to join pending native calls and close the consumer off
an async worker. Unfinished records remain uncommitted. Native shutdown/Drop itself
is not a hard-deadline guarantee. Await shutdown to completion: cancelling during
its preliminary flush or simply dropping a Receiver is not graceful close and can
run native destruction on the dropping thread. This native profile does not establish durable
key scheduling, replay/hot-tail, retry/DLQ topic workflows or schema interoperability.

The SDK owns native worker threads. Call `Publisher::shutdown().await` to flush
and destroy the producer off an asynchronous worker. Native destruction is not a
cancellable hard-deadline guarantee. A dropped send future does not cancel a
record already admitted by the SDK.

The package links bundled librdkafka through the safe `rdkafka` Rust API. Linux
builds use CMake, a C toolchain, zlib and OpenSSL development files. No .NET runtime
or Wolverine wire-compatibility claim is implied.
