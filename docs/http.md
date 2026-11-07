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

## Typed binding and validation

Implement `HttpRequest` with explicit Serde/JSON Schema `Path`, `Query` and `Body`
types and a fallible `bind` function. `ApiBuilder::query` binds body-free GETs;
`bound_request` binds POST JSON alongside path/query/header inputs. Named scalar
path fields must match the template exactly. Query defaults follow Serde; nested
JSON query objects and catch-all paths are not silently advertised as supported.
Declare additional scalar headers in `HEADERS`; the adapter checks required
headers before invoking the binder. Authorization remains the authenticator's
responsibility, and the trusted principal remains separate in `Authenticated<M>`.
The binder can return `ApiError::invalid()` without exposing supplied values.
Runtime pipelines can add reusable asynchronous validation to the same handler.

## Bounded server-sent events

`ApiBuilder::events` registers authenticated GET query ingress whose typed reply
is `EventStream<T>`. `EventStream::channel` requires a finite capacity of 1–256;
`StreamOptions` bounds event count and lifetime. SSE carries JSON `data` values;
OpenAPI advertises `text/event-stream` and the `x-raddle-event-schema` describing
each JSON value. A serialization failure closes the stream rather than returning
private error text after headers have already been sent.

The application owns producer tasks. Dropping or completing the response closes
the channel, unblocking pending sends with a closed error. The runtime's handler
permit covers stream creation, not its lifetime. Clients must reconnect/reconcile
using application-defined cursors if durable streaming is needed: this bounded
stream is not a persisted subscription or delivery acknowledgment protocol.

## Readiness and diagnostics

`health(prefix, store, workers_ready)` adds private `/live`, `/ready` and `/queues`
routes. All require Inspect permission. Liveness reports that the router is
responding; readiness combines a bounded database round trip with the configured
worker readiness callback, returning 503 when either is false. Connect that
callback to actual host readiness rather than assuming a spawned task is ready.
A database probe or idle outbox cannot establish broker connectivity.

The queue view is scoped to the authenticated tenant and contains aggregate
ready/active/dead/completed/due counts, never bodies, error text or cross-tenant
enumeration. See [raddle-admin](../crates/raddle-admin/README.md) for command-line
diagnostics and explicit audited replay without an HTTP listener.

## Ingress limits and conditional responses

`limits(RequestLimits)` opts the API into a finite fixed-window request budget
per authenticated tenant and subject. The identity table is bounded; when it is
full, new identities receive the same safe 429 response until old windows expire.
Caller-supplied tenant/subject headers cannot select a new budget. Authentication
still runs on denied requests. This is an in-process limiter, not a distributed
quota service; apply an upstream limiter when a shared cluster budget is required.

`conditional_get(path)` opts an already registered JSON GET into SHA-256 ETag
revalidation. A matching `If-None-Match` returns a body-free 304, but only **after**
current authentication, binding, validation and handler execution. Resource-level
authorization cannot be bypassed by a previous response. Responses use
`Cache-Control: private, no-cache` and vary on Authorization. The adapter retains
no shared response cache and buffers at most 1 MiB for two seconds; it does not
apply conditional caching to SSE or error responses. This saves unchanged response
transfer, not application/database execution.
