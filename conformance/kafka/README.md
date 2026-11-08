# Native Kafka fixture

Run the adapter's opt-in broker suite against an isolated official Apache Kafka
4.1.2 JVM image:

```sh
python3 conformance/kafka/run.py test
```

Ownership and cleanup guards also have unit tests that do not contact Docker:

```sh
python3 -B -m unittest discover -s conformance/kafka -p 'test_*.py'
```

The Linux runner requires Docker Compose and the verified perok Btrfs mount. It
resolves the native-platform image digest, uses a run-specific Compose project,
binds only numeric loopback, and keeps broker data under perok. Kafka and native
SDK dependencies retain their own licenses and notices. No host Java installation,
proprietary registry image or cloud account is required.

One combined broker/controller uses replication factor one. This qualifies
neither replicated durability nor broker leader failover. The test process uses
one compilation lease, a checkout-specific target directory and one build job.
One-shot execution acquires that lease and compiles before starting Kafka, so
the fixture does not consume resources while other builds are ahead of it.
The one-shot runner removes its containers and network even after a test failure;
broker logs and the ownership receipt remain available for inspection, while
disposable broker data is removed after successful teardown.

For an explicitly retained development fixture, run `up`, retain its printed
state-file path, then use `test --state <path>` and finally `down --state <path>`.
Teardown verifies run ownership and does not select containers by shared image.
The fixture sends synthetic test data only; do not point this runner at production.

The socket suite covers all four custody strategies, count saturation, idle and
graceful flush, and aborted/open transactions under both isolation settings. The
runner also passes its verified container ID for read-only Kafka 4 CLI group-type
and assignor checks; cooperative rebalancing alone does not establish KIP-848.
Two native clients join each membership mode, with eager reacquisition or
incremental preservation of unaffected work and stale revoked handles checked.
These in-process joins do not establish static-ID fencing, coordinator loss,
multi-process recovery, replicated failover or the full membership contract.
`tests/sdk_mock.rs` separately
injects native commit denial/delay to exercise uncertain retries and cancellation;
mock results are not broker interoperability evidence.

## CRC-only wire fault

Select the separate, one-shot CRC profile explicitly:

```sh
python3 conformance/kafka/run.py test --mode crc
```

This selects the package-contained `tests/crc_wire.rs`, not the ordinary ten-test
broker suite. Its test is ignored by default and requires this live fixture's
loopback control address and per-run token. An ordinary broker, retained `--state`,
or `up --mode crc` cannot silently qualify an inert fault test.

The standard-library Python proxy runs in the runner's own process and is joined
on teardown. The broker advertises the proxy's EXTERNAL address, while Docker maps
a different backend port; metadata and coordinator routing therefore cannot bypass
the proxy. A client-ID-scoped ApiVersions response cap leaves every API except
Fetch unchanged, verifies that the broker offers v4, and requires the named test
clients' actual Fetch requests to use v4. Other clients are byte-transparent.
This is a narrow Kafka 4 / librdkafka 2.12.1 / magic-2 / Fetch-v4 profile, not a
general flexible-version fault injector or membership compatibility proof.

An authenticated arm barrier selects one client, topic, partition and known batch
base offset **before** the producer publishes that separate single-record batch.
The proxy verifies its original Castagnoli CRC, flips exactly one CRC-field bit,
and disarms. Response structure, record bytes, keys and sizes stay unchanged.
`wire-receipt.json` retains only protocol coordinates, CRC values, body digest,
mutation counts and bounded diagnostics, never full record payloads or the token.
Frame size, active connections, inflight correlations, lifetime frame count,
control line size, receipts and read/write times are bounded. This is a synthetic
local fault fixture, not a hostile-client security boundary.

The shipping Receiver first commits a healthy baseline, holds an unfinished
record, and then refuses the corrupted batch with `ReceiverFenced`. Completion
of the already-held handle, flush, repeat receive and shutdown must stay fenced;
broker offset queries must remain at the baseline. With the fault disarmed, a new
CRC-checking Receiver must redeliver both uncommitted records in order and commit
only their completed prefix. A separate test-only `check.crcs=false` control must
read the unchanged binary key/body under the same one-bit fault, and a raw pinned
native SDK client must report `MessageConsumption(BadMessage)`. Disabling CRC
checking is solely a causal control, **never** a recommended recovery setting.

This closes only the actual CRC-wire-fault custody example under issue #40. It
does not establish hostile decompression bounds, hard native teardown, TLS/SASL,
replication/failover, all custody/membership/isolation profiles under corruption,
independent-process recovery or foreign-runtime parity. The ordinary broker suite
continues to cover its separate custody/membership/isolation profiles unchanged.
