# Qualification

Use the repository's pinned Rust toolchain for production crates. Run each
package independently as well as the workspace: development dependency feature
unification can hide missing production features. The optional Service Bus
`emulator` feature and its separate conformance workspace are additional gates,
not substitutes for default-feature builds or live Azure qualification.

## Application framework checks

Core tests cover middleware entry/exit order, typed validation, cancellation and
permit release, finite cascading graphs, independent effect outcomes and owned
retry decisions. Host tests cover service restart exhaustion, readiness generation
fencing, permanent failure, cancellation and deadline-enforced task joining.

With a disposable `DATABASE_URL`, run the ignored PostgreSQL cluster, recurring,
saga-dispatch, event, operation-cancellation and operations tests as well as the
existing durability suites. Their cases exercise concurrent ownership, expired
leases and stale snapshots; interrupted transactional work; recurring progress;
event commit visibility; isolated stream checkpoints; and retained successful work.
Run `raddle-host`'s database tests and integrated application against a disposable
local RabbitMQ broker. Broker tests must use run-owned entity names and clean
those entities up; topology creation is explicit, never an implicit cloud step.

HTTP checks include real sockets for finite SSE and database-backed authenticated
readiness, while RPC checks use standard Tonic clients over actual HTTP/2 sockets.
The gRPC Protobuf contract is part of its package. CLI integration tests execute
the compiled binary against an isolated database, checking server-enforced
read-only diagnostics and explicitly confirmed tenant-scoped replay/audit.

Qualify each new package independently with default features as well as in the
workspace. Dependency feature unification is not proof that an isolated package
builds. Check package contents for its license, README, migrations and protocol
sources, and retain exact command exits rather than accepting filtered log output.

## Frozen-source qualification

Qualify the clean committed source that will be delivered, including all integrated
changes. Record its revision, the command plan and each command's actual exit
status in a fresh run directory. Earlier branch or package results do not qualify
a later integrated head. Keep execution receipts outside the source repository.

After an interruption, retain a passed stage only when the source revision,
working-tree cleanliness, command and verification configuration still match.
Confirm that the previous driver and its children have stopped before resuming.
Keep failed and interrupted attempts separate from their reruns; a later passing
attempt does not change an earlier outcome. Broker fixtures that acquire their
own compilation lease must not be wrapped in a second copy of that lease.

## Offline and database checks

The normal workspace suite covers typed runtime behavior, independent fan-out,
cancellation, wire framing/upcasting, HTTP schema contracts, and real-file SQLite
transactions. PostgreSQL tests require an explicitly selected disposable server
with database-create permission. They allocate isolated databases and cover
transaction rollback, concurrent claims, process exits, upgrades from the
initial schema, saga state, and request/reply races.

Do not run the same SQLx PostgreSQL test concurrently from different checkouts
against one server. SQLx 0.8.6 derives its test database name from the test path,
not the checkout, so those runs can recreate or remove each other's database.
Use separate explicitly selected servers or serialize the entire test execution.
A compilation lease alone is insufficient when prebuilt test binaries continue
running after that lease is released.

`raddle-testkit` supplies a manual clock and bounded scripted transport. Scripts
distinguish failure before acceptance from lost confirmation after acceptance;
receipts retain identities rather than message bodies. Script exhaustion refuses
further sends instead of silently making them successful. Both persistence
implementations run the shared custody suite for duplicate identities, tenant
separation, and changed-content conflicts.

`Envelope::with_identity` provides deterministic message IDs and creation times.
Avoid using `sleep` to simulate durable due times: pass the manual clock's time
to storage claims. Wall-clock waits are appropriate only for actual transport
deadlines and cancellation boundaries.

## Fuzzing

The `fuzz` workspace is isolated from production dependency resolution. It uses
`cargo-fuzz` and a nightly toolchain with AddressSanitizer. Keep generated corpora,
artifacts, and build output outside the source tree or in ignored directories.
Seed the corpus with `fuzz/seeds/envelope.json`; the target checks that every
accepted envelope survives canonical serialization and preserves its identity,
contract, version, and headers. Bound local runs explicitly:

```sh
cargo +nightly fuzz run envelope -- -runs=50000 -max_len=1048577
```

Preserve minimized failures as regression tests before fixing them. A bounded
fuzz run is evidence for that run, not a proof of memory safety or protocol
correctness. Production code forbids unsafe Rust; dependencies need their own
security and license review.

## Platform qualification

Native Windows, Linux, and macOS qualification must run the same pinned-toolchain
format, Clippy, tests, build, and rustdoc gates. Test SQLite on a local native
filesystem and the transport adapters against isolated broker instances. A
Linux cross-check or successful target compilation is not a substitute for
native runtime tests on Windows or macOS. Do not mark an unavailable platform
qualified. Keep environment-specific receipts in the associated issue, not in
these reusable instructions.

## Package qualification

Verify the publishable package archives as well as the workspace checkout:

```sh
cargo +1.95.0 package --workspace --exclude raddle-servicebus --locked \
  --target-dir "$FRESH_PACKAGE_TARGET"
```

Use the pinned toolchain explicitly when running outside the checkout, where
`rust-toolchain.toml` does not select it. Start from a clean committed head and a
new head-specific package target, including its temporary registry. Cargo treats
registry package versions as immutable; reusing staging for a different source
head with the same version can select stale internal dependencies. Do not clear
shared Cargo caches to achieve this isolation.

This creates and builds local archives; it does not upload them. Inspect each
`.crate` archive, not an existing extracted directory. Verify that
`.cargo_vcs_info.json` names the exact committed head without a dirty flag,
compare the license bytes with the source license, and check the README, declared
targets and required protocol or migration files. Compare archived source content
with that revision and record archive digests and actual command exits outside
the repository. Packaging's library build does not compile every test target.

### Isolated HTTP archive tests

Extract verified archives into a fresh fixture outside any source checkout. Use
their normalized `Cargo.toml` files, not `Cargo.toml.orig`. If internal packages
are not published, a temporary root workspace with `[patch.crates-io]` entries
may resolve them only to other extracted archives verified against the same
head. Do not point dependencies at a source checkout or recreate sibling source
directories. Check dependency metadata to confirm that every internal package
comes from the verified fixture.

Prepare the fixture's lock resolution before using `--locked`; when starting
with the packaged lockfile, `cargo +1.95.0 update --offline --workspace` can adjust
internal package sources without updating external dependencies. Record any
resolution or toolchain failure separately from test compilation failures. Use
a separate fresh target for each fixture, then run from its workspace root:

```sh
cargo +1.95.0 test -p raddle-axum --all-targets --no-run --locked \
  --target-dir "$FRESH_FIXTURE_TARGET"
# Supply DATABASE_URL securely for an explicitly selected disposable PostgreSQL
# server with database-create permission before executing the tests.
cargo +1.95.0 test -p raddle-axum --all-targets --locked \
  --target-dir "$FRESH_FIXTURE_TARGET" -- --include-ignored --test-threads=1
```

Storage owns the checksummed migrations: they belong in the `raddle-postgres`
archive and are embedded by its public `Store::migrate` method. HTTP SQLx replay
fixtures use `#[sqlx::test(migrations = false)]` and explicitly call that migrator
before creating application tables or accessing storage. Do not copy migrations
into the HTTP archive or depend on `../postgres/migrations`. Keep the tests'
activation unchanged; unignored database tests also require `DATABASE_URL`.
Serialize the entire runtime against other checkouts using the same server, as
described above. Exercise replay, compensation, cancellation, process-crash
boundaries and successful-neighbor/failed-only retry assertions from the archives,
not just from the checkout.

Examples marked as test targets are also exercised by the workspace test suite;
the PostgreSQL walkthrough additionally needs the isolated database gate.

`raddle-servicebus` remains excluded from this registry-oriented verification
while it requires the patched vendored dependency. Its package metadata forbids
publication. Continue to qualify it from the source workspace and independent
conformance harness rather than replacing the patched client for packaging.
