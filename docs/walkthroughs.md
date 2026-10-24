# Runnable durable walkthroughs

Run these examples from the repository root with the pinned Rust toolchain.
They use real storage and the same public APIs available to applications. They
do not connect to an external broker or certify Azure/native-platform support.

Use a dedicated example database, never production or an existing application's
database. The examples run migrations and create their own application tables;
the PostgreSQL worker can claim other ready work in the selected database.

## SQLite: commit, reopen, consume

Choose a new file on a local filesystem and substitute its native absolute path:

```sh
cargo run -p raddle-sqlite --example sqlite_durable -- /path/to/example.sqlite
```

The example stages a credit in a transaction, closes the connection pool, opens
the file again, and consumes the persisted job. The credit and inbox completion
commit together. It reports a balance of 25 and `processed=true`.

Repeat the same command against the same file. It reports the same job identity
and balance, with `processed=false`: duplicate custody returned the existing
completed job instead of repeating the business write.

The fixed message ID, creation time, source, destination, and payload are
intentional redelivery fixtures. Real applications generate an identity for new
work and retain it on retries. Changing content under this identity is a
conflict, not another credit. Removing completed tombstones ends their duplicate
protection; do not prune the example state and then expect that protection.

## PostgreSQL: purchase, saga, receipt

Create or select a disposable database on your existing local PostgreSQL server.
Supply its `DATABASE_URL` through the environment or your approved secret
mechanism without logging the value. Set `RADDLE_EXAMPLE_CONFIRM=isolated` in the
same environment, then run:

```sh
cargo run -p raddle-postgres --example postgres_durable
```

The example first persists a purchase request. A worker transaction inserts its
business row, finishes a versioned purchase saga, and stages a correlated local
receipt. Another worker transaction records that receipt. All handlers are
ordinary typed functions registered explicitly by the application.

The first run reports `total=25 receipt=true processed=2`. Repeating it against
the same database reports the same job identity and total, with `processed=0`.
No network effect is hidden inside a database transaction; the receipt is a local
durable follow-up, not a claim of broker delivery.

The executable refuses to connect unless isolation is explicitly confirmed.
That confirmation is a safety guard, not a check that the selected URL really is
disposable. The operator must choose the correct database.

## Test and package behavior

Both examples are Cargo test targets. The SQLite repeat-run check is part of the
ordinary workspace tests. The PostgreSQL repeat-run check is opt-in and uses
SQLx's isolated test-database allocation:

```sh
cargo test -p raddle-postgres --example postgres_durable -- --ignored
```

Each package ships its own README. Links to broader repository documentation are
absolute, so they do not rely on files outside the package archive. Service Bus
remains non-publishable while it depends on the patched vendored client; package
creation alone is not publication or a substitute for its qualification gates.
