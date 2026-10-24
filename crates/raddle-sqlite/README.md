# raddle-sqlite

Single-node durable messaging for raddle using SQLx and SQLite. Local work,
transactional fan-out, outbox publication, retries, scheduling, and audited replay
share application-owned transactions. The built-in store uses WAL and full
synchronization on a local filesystem.

SQLite has one active writer. Keep handlers short and free of network effects;
use PostgreSQL when parallel writers or persisted sagas/conversations are needed.

The [durable example](https://github.com/DeandreT/raddle/blob/main/crates/raddle-sqlite/examples/sqlite_durable.rs)
commits a credit, closes and reopens the database, then applies the credit once.
Run it against a dedicated example file from the repository checkout:

```sh
cargo run -p raddle-sqlite --example sqlite_durable -- /path/to/example.sqlite
```

Repeat the command with the same file: the balance remains 25 and the second run
reports `processed=false`. The example's fixed message identity and routing
snapshot deliberately model redelivery; do not reuse them for different input.

- [Backend guarantees and qualification](https://github.com/DeandreT/raddle/blob/main/docs/adapters.md)
- [Source repository and Git dependency usage](https://github.com/DeandreT/raddle)

MIT licensed. Source versions are not evidence of a registry release.
