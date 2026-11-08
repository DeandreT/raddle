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
cargo package --workspace --exclude raddle-servicebus --locked
```

This creates and builds local archives; it does not upload them. Check that each
archive includes its package README, license, and required migration files.
Examples marked as test targets are also exercised by the workspace test suite;
the PostgreSQL walkthrough additionally needs the isolated database gate.

`raddle-servicebus` remains excluded from this registry-oriented verification
while it requires the patched vendored dependency. Its package metadata forbids
publication. Continue to qualify it from the source workspace and independent
conformance harness rather than replacing the patched client for packaging.
