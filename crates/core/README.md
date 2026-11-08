# raddle

Typed, bounded local execution for Rust messaging applications. Applications own
their handlers, state, tasks, and shutdown; raddle performs no runtime reflection
or automatic discovery. Requests have one typed result, while named event
subscribers have independent outcomes.

Typed async pipelines add ordered middleware and short-circuit validation.
Explicit cascading replies execute bounded command/event effects only after
parent admission is released. Owned retry decisions require an application
rollback or idempotency assertion; ordinary handlers are never silently replayed.
Typed route tables resolve stable wire contracts only to configured aliases.

Declared request and event families share one typed behavior across explicit
concrete variants, with atomic registration and ordinary bounded execution.
They use native enums/adapters, not reflection or inherited wire contracts.

The local runtime is volatile. Durable execution belongs to the separate
PostgreSQL and SQLite adapters; it is not implied by invoking a local handler.
All crates are independently implemented and MIT licensed.

From the [repository](https://github.com/DeandreT/raddle), run:

```sh
cargo run -p raddle --example local
```

- [Runnable request/event example](https://github.com/DeandreT/raddle/blob/main/crates/core/examples/local.rs)
- [Runnable declared-family example](https://github.com/DeandreT/raddle/blob/main/crates/core/examples/families.rs)
- [Declared message-family contracts](https://github.com/DeandreT/raddle/blob/main/docs/message-families.md)
- [Architecture and execution guarantees](https://github.com/DeandreT/raddle/blob/main/docs/architecture.md)
- [Pipelines, cascades, retry policy, and typed routing](https://github.com/DeandreT/raddle/blob/main/docs/pipelines.md)
- [Durable orchestration](https://github.com/DeandreT/raddle/blob/main/docs/orchestration.md)
- [Workspace overview and contribution guidance](https://github.com/DeandreT/raddle)

Source consumers can select this package through a Git dependency. Pin a commit
when reproducibility matters; do not assume a source version is already released
to a registry.
