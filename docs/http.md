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
response, not the potentially sensitive error string. Replies are serialized
inside the adapter before a success response is returned. Serialization failures
return the stable JSON error contract with status 500; neither partial reply
content nor the serializer's error text is returned.

A transactional synchronous handler must await its SQLx scope's commit before
returning a reply, and propagate commit failures through its handler error. The
adapter awaits that same handler; it does not open or commit a hidden transaction.
Consequently, a failed application commit cannot become an HTTP success merely
because business statements or message staging finished earlier.

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
without dropping nested definitions. Request schemas describe deserialization;
response schemas describe serialization, including direction-specific Serde names
and skipped fields. Tests validate actual responses against those schemas and
exercise both router calls and a real HTTP socket. Authenticated per-operation
checks distinguish actual registered handlers from authentication on a fallback
route. Database-backed socket checks cover committed replies, failed commits,
durable acceptance rollback, and tenant-scoped audited replay. Additional
application-owned routes require their own implementation-checked contracts.

The body limit is 1 MiB. JSON, path, authentication, method, and missing-route
errors use one stable JSON shape. The 401 response includes a Bearer challenge.
HTTP tracing records only completion status and duration.
