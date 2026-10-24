# HTTP integration

`raddle-axum` produces an Axum router and its OpenAPI 3.1 document from the same
registrations. It starts no listener or administration service automatically.
The application owns binding, TLS termination, and graceful shutdown.

Supply an `Authenticator` that verifies bearer tokens and returns a bounded
`Principal`. Authentication runs before body extraction. The library checks
explicit Invoke, Inspect, and Replay permissions; resource-level authorization
still belongs in application handlers. No raw token, body, or handler error is
logged or returned in the HTTP error contract.

## Requests and acceptance

`ApiBuilder::request<M>` registers a typed POST operation whose body is `M` and
whose response is `M::Reply`. Register its handler as `Authenticated<M>` in the
ordinary raddle runtime. That wrapper carries the verified principal into the
same bounded executor used by local callers. Domain failures return a fixed 422
response, not the potentially sensitive error string.

`ApiBuilder::durable<M>` registers a POST operation returning 202 only after its
SQLx transaction commits. It requires an `Idempotency-Key` header scoped by
tenant and route. Concurrent equivalent submissions return the original message
identity; changed content under that key returns 409. A failed transaction
leaves neither a key record nor queued work. Acceptance does not mean execution
has finished.

`Store::prune_idempotency` applies an explicit bounded retention cutoff and keeps
records with live jobs. Removing a record ends duplicate protection for that
tenant, route, and key; choose the cutoff according to the client's retry horizon.

## Opt-in operations

`operations(prefix, store)` registers tenant-scoped job inspection, audited
dead-letter replay, and aggregate HTTP counters. These endpoints require their
own permissions. Metrics contain no per-request IDs, tenant labels, tokens or
payloads. Keep the operational router private even when the application's
business endpoints are publicly reachable.

All supplied routes, the authenticated `/openapi.json` endpoint, intentional
headers, request/response schemas, errors, and security scheme come from the
builder. Generated JSON Schema references are relocated into OpenAPI components
without dropping nested definitions. Tests validate actual responses against
those schemas and exercise both router calls and a real HTTP socket. Additional
application-owned routes require their own implementation-checked contracts.

The body limit is 1 MiB. JSON, path, authentication, method, and missing-route
errors use one stable JSON shape. The 401 response includes a Bearer challenge.
HTTP tracing records only completion status and duration.
