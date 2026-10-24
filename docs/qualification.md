# Qualification

Use the repository's pinned Rust toolchain for production crates. Run each
package independently as well as the workspace: development dependency feature
unification can hide missing production features. The optional Service Bus
`emulator` feature and its separate conformance workspace are additional gates,
not substitutes for default-feature builds or live Azure qualification.

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
