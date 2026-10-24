# raddle-postgres

SQLx PostgreSQL persistence for raddle: transactional inbox/outbox, independently
retried subscribers, scheduling, fenced claims, audited replay, versioned sagas,
and durable request/reply. Application writes and messaging effects share the
same actual SQLx transaction.

Workers and migrations are explicit. Confirmed remote publication remains
at-least-once: a crash after broker acceptance can cause republishing, so consumers
must be idempotent. PostgreSQL 14 or later is the supported database baseline.

The [durable example](https://github.com/DeandreT/raddle/blob/main/crates/raddle-postgres/examples/postgres_durable.rs)
stages a purchase, commits its business row and completed saga together, and
processes a durable receipt. Repeating it retains the original effects. It
requires an **isolated example database**, since workers claim from that database
and the example creates its own application table.

Set `DATABASE_URL` securely and `RADDLE_EXAMPLE_CONFIRM=isolated`, then run from
the repository checkout:

```sh
cargo run -p raddle-postgres --example postgres_durable
```

- [Transactions, workers, retention, and qualification](https://github.com/DeandreT/raddle/blob/main/docs/postgres.md)
- [Saga and conversation contracts](https://github.com/DeandreT/raddle/blob/main/docs/orchestration.md)
- [Source repository and Git dependency usage](https://github.com/DeandreT/raddle)

MIT licensed. Source versions are not evidence of a registry release.
