# raddle

Typed, bounded local execution for Rust messaging applications. Applications own
their handlers, state, tasks, and shutdown; raddle performs no runtime reflection
or automatic discovery. Requests have one typed result, while named event
subscribers have independent outcomes.

The local runtime is volatile. Durable execution belongs to the separate
PostgreSQL and SQLite adapters; it is not implied by invoking a local handler.
All crates are independently implemented and MIT licensed.

From the [repository](https://github.com/DeandreT/raddle), run:

```sh
cargo run -p raddle --example local
```

- [Runnable request/event example](https://github.com/DeandreT/raddle/blob/main/crates/raddle/examples/local.rs)
- [Architecture and execution guarantees](https://github.com/DeandreT/raddle/blob/main/docs/architecture.md)
- [Durable orchestration](https://github.com/DeandreT/raddle/blob/main/docs/orchestration.md)
- [Workspace overview and contribution guidance](https://github.com/DeandreT/raddle)

Source consumers can select this package through a Git dependency. Pin a commit
when reproducibility matters; do not assume a source version is already released
to a registry.
