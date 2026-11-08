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
