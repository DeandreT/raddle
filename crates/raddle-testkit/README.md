# raddle-testkit

Deterministic test support for raddle: an explicit manual clock, bounded scripted
transport outcomes, and shared durable-custody checks. It starts no background
tasks and retains acceptance identities rather than message bodies.

Transport scripts distinguish failure before broker acceptance from a lost
confirmation after acceptance. Exhausting a script rejects subsequent sends;
it never silently makes them successful. The shared custody contract checks
deduplication, tenant separation, and immutable-content conflicts.

- [Qualification and fuzzing procedures](https://github.com/DeandreT/raddle/blob/main/docs/qualification.md)
- [Clock and fault-script examples](https://github.com/DeandreT/raddle/blob/main/crates/raddle-testkit/tests/faults.rs)
- [Source repository and Git dependency usage](https://github.com/DeandreT/raddle)

MIT licensed. Source versions are not evidence of a registry release.
