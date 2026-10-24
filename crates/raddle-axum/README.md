# raddle-axum

Authenticated Axum ingress over raddle's execution contracts. Typed synchronous
requests use the local runtime; durable acceptance uses the PostgreSQL store.
The router and its OpenAPI 3.1 document are generated from the same registrations.

Applications supply bearer authentication, tenant/subject identity, and explicit
permissions. Operational inspection, metrics, and audited replay are opt-in.
No listener is started automatically, and payloads are not logged by default.

Durable acceptance requires an idempotency key scoped to the authenticated tenant
and route. A 202 response means custody committed, not handler completion.

- [HTTP setup, identity, idempotency, and operations](https://github.com/DeandreT/raddle/blob/main/docs/http.md)
- [Runnable-router contract tests](https://github.com/DeandreT/raddle/blob/main/crates/raddle-axum/tests/contracts.rs)
- [Source repository and Git dependency usage](https://github.com/DeandreT/raddle)

MIT licensed. Source versions are not evidence of a registry release.
