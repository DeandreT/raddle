# Native HTTP contracts

`raddle_axum::native` is an explicit Rust endpoint pipeline over the application's
existing `raddle::Runtime`. It is a semantic implementation, not ASP.NET, CLR
discovery, Asp.Versioning, System.Text.Json or a .NET library bridge. The comparison
target is Wolverine **V6.62.0**, resolved to
`3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e`. Later upstream documentation is not the
contract. Issue 37 remains open until its remaining behaviors and counterpart
qualification are complete.

## Registration and execution ownership

See `crates/raddle-axum/examples/native_contract.rs` for a complete application
registration example with separate typed request/resource schemas and a 201 response.

1. Implement `Endpoint`, declaring `Input`, `Resource`, `Error`, binding and handling.
2. Create an `EndpointContract` with its method, route, stable name, body reader,
   typed parameters and allowed `ResponseKind` alternatives. `body_schema()` is the
   exact body DTO schema, separate from route/query/header inputs. Arbitrary started
   responses require explicit `response_metadata`.
3. Call `EndpointRegistration::prepare(&mut runtime_builder, contract, endpoint)`
   **before** `RuntimeBuilder::build()`. One endpoint type owns one core registration;
   distinct newtypes can reuse business functions without duplicate type ownership.
4. Give that same runtime to `NativeApiBuilder::new(runtime, authenticator)` for
   bearer verification, or `NativeApiBuilder::cookie_sessions(runtime, sessions)`
   for cookie verification and matching security metadata. Mount
   the prepared registration with `endpoint()`, and call `build()`.
5. Serve the returned `Api::router()` using the application's Axum host. Its
   `specification()` is available offline, without starting a listener or opening
   database/broker connections.

Authentication, rate admission, CSRF, `before_bind`, binding, resource authorization,
validation, cache lookup/fill, handling, `after`, effects, replay transaction work
and `finally` run within the **same core admission permit** as ordinary requests.
There is no second executor. Do not recursively invoke a runtime while holding its
last permit. Routing's structural method/media/version selection is not an
application callback. A wrong/unregistered runtime returns 503, not direct execution.
Authentication/rate/CSRF denials stop before endpoint lifecycle callbacks. An
admitted concurrency permit stays owned through `finally` and header processing.

Native authentication accepts the supplied verifier's supported credential types,
including real cookie sessions. Authentication and Invoke permission are required
unless `anonymous` is explicitly selected. Raw tenant hints never grant identity.
`Endpoint::authorize` runs on every request, **before cache lookup and response
replay**, so a cached response cannot bypass revoked resource access. Anonymous
handlers still receive no fabricated principal.

The legacy `ApiBuilder` is separate and unchanged: its bearer-only authentication,
scalar binding, malformed-route 400, JSON strings, conditional GET and durable 202
acceptance are not silently switched to native defaults.

## Binding and routing

- GET, POST, PUT, DELETE, PATCH, OPTIONS, HEAD and QUERY are explicit methods. HEAD
  suppresses the body. Unregistered methods return 405 with Allow. An explicit
  OPTIONS endpoint uses the same pipeline. QUERY may bind JSON, but is represented
  by `x-http-query-operation`, not an invalid OpenAPI 3.1 `query` operation.
- Routes use full named segments, optional terminal `{id?}` and terminal `{*rest}`.
  Invalid syntax, duplicate capture names and equivalent conflicting templates are
  rejected before mounting. This is not all ASP.NET inline-constraint syntax.
  Variants sharing one OpenAPI operation must agree on ordinary binding and
  security metadata; ambiguous mixtures are startup errors rather than shadowed
  handlers or misleading required parameters.
  `RoutePrefixes` selects endpoint > most-specific namespace > global without
  stacking. The stable route name is the default operation identity;
  `EndpointContract::link` percent-encodes values and omits missing optional segments.
  OpenAPI uses a standard `{rest}` placeholder for a catch-all, retaining the actual
  `{*rest}` template and catch-all semantics in native extensions.
  Concrete optional/version path expansions have distinct stable operation IDs.
  Variants merged into one path/method select the lexicographically least
  candidate operation ID, independent of their registration order.
  Schema bundles reserve collision-free component names before rewriting their
  references, so another route's root cannot replace a nested definition.
  Definition references retain their full subschema-pointer suffix; only the
  definition-name token is decoded and its component name re-escaped.
- `Parameters::path<T>` / `required_path<T>` use `FromStr`, including handwritten
  strong IDs. Exact argument-name casing matters. Malformed native route values
  default to 404; `RouteParseFailure::BadRequest` explicitly selects 400.
- `query`, `query_optional`, `query_many`, `header` and `header_optional` take external
  aliases and explicit defaults. Missing values preserve defaults. Lenient mode
  preserves scalar defaults and drops malformed repeated collection elements;
  strict mode returns named 400 ProblemDetails for the entire collection. Repeated
  scalar query/header values are comma-joined. String headers remain strings.
  `Parameter::typed<T>` publishes actual scalar/array/newtype schemas and defaults.
- JSON uses Serde DTO policies. A transport-declared missing body is distinct from
  a can-have-body stream containing no JSON bytes. Content-Length: 0 is missing;
  unknown-length HTTP/1 chunked and HTTP/2 bodies are read incrementally up to the
  configured total bound before deserialization. Empty declared chunked JSON is
  malformed 400. Wrong JSON media types return 415.
  JSON `null` binds as absent for an optional body; it is not an empty declared
  stream. Missing responses use the explicit response enum and emit no JSON null
  body. Named Newtonsoft HTTP settings remain a separate later integration (its
  plain HTTP defaults are not Newtonsoft messaging's reference/type defaults).
- Exactly one `BodyMode` is selected, making competing JSON/form/stream readers an
  invalid configuration rather than a hidden second body read. GET/HEAD body-reader
  registration is rejected. Mixed-source constructors are ordinary Rust code in
  `bind`; services are explicit endpoint-owned dependencies, not inferred bodies.
- `Form` buffers bounded URL-encoded/multipart input once, preserving repeated
  aliases, metadata and named files. `before_bind` and `bind` share that buffer.
  Filenames and media types are untrusted metadata, never filesystem paths.
- `Multipart` exposes one shared `MultipartReader` cursor. Middleware can consume
  the first field and the binder consumes subsequent fields without rewinding or
  whole-body buffering. Limits apply to total wire bytes, section bytes and section
  count; malformed boundaries return 400, non-multipart 415, exceeded bounds 413.
  Fields must be dropped/consumed before requesting another field. Cancellation
  drops the body/reader; no framework temp files or producer tasks are created.

## Responses, errors and versions

`EndpointResponse` makes JSON, text, explicit status, empty 204, missing 404, created
201, accepted 202 and started/custom responses distinct. Created/accepted results
carry validated Location headers and optional JSON resources. A started response
preserves the application's status/body. `MissingResponse::NoContent` is legal only
for GET/QUERY. Required entity loading remains separate application authorization/
binding behavior, not an automatic persistence integration.

`EndpointOutput` separates the primary response from secondary `EndpointEffect`s;
`(response, effects).into()` never serializes a tuple as the resource. Effects run
on the same context/scope before commit. External network/filesystem effects cannot
be rolled back and have no exactly-once promise.

Configured `ResourceWriter`s run in registration order. Matching deliberately uses
the one list frozen during preparation for both metadata and request execution.
Selection uses
case-sensitive `Accept.contains(media_type)`, including a match with `q=0`; it is
not RFC quality/wildcard negotiation. Loose mode falls back to JSON; strict mode
with configured writers returns empty 406 when none matches. Same-route request
dispatch matches exact configured Content-Type ignoring parameters. Vendor types
stay distinct; absent/unsupported media and wildcard declarations do not match.

`Problem` emits safe RFC 9457 `application/problem+json`, named errors and explicit
extensions. Reserved extension keys cannot replace its status/title/type fields.
`Validation::NoProblems` is the only continuation sentinel; Reject stops before
handler/effects. `Endpoint::map_error` receives the concrete native error for typed
matching/filtered conflict factories; errors are otherwise redacted 500s. This does
not implement CLR exception hierarchy/catch merging or .NET validators.

`Versioning` explicitly declares supported versions, mapped subsets, neutral mode,
default/required selection and URL/header/query/media readers. Combined supplied
selectors must agree. URL `{version}` expands into literal routes. Sunset,
Deprecation, Link and supported-version headers are applied to native pipeline
responses, including binding/validation failures after successful version selection.
Failures in structural matching and application host exception middleware are not
promised those headers. Metadata uses native names and `x-api-versions`; it does not
claim an Asp.Versioning package integration or a complete per-version document CLI.

## Caching, limits and sessions

`ResponseCache` writes Cache-Control/Vary headers only. It does not store a result
and is independent of legacy conditional GET/ETag revalidation. Public caching is
explicit; use private/no-store for sensitive responses.

`OutputCachePolicy` selects TTL, query/header variation, tags and eligibility.
Default eligibility is anonymous requests without Authorization; authenticated
caching requires `AuthenticatedPartition` and always includes verified tenant and
subject. Resolved API version is part of route cache identity regardless of vary
settings. Repeated vary values use nested, unambiguous key components. Resource
authorization and validation still run on cache hits. Slots and
body size are finite (at most 4096 slots / 64 MiB configured body capacity), with
bounded keys and headers. Saturation and unknown-length/oversized streaming bodies
bypass storage. Per-key locks coalesce concurrent misses. `evict_tag` invalidates
both matching stored entries and pending fills. Apply invalidation only after the
application's successful commit; there is no automatic store-change subscription.
Responses carrying `no-store` bypass storage regardless of directive casing or
which Cache-Control field contains it; directive names are matched as tokens.

`RatePolicy` attaches a named finite partition table to a route. Omitting it is an
explicit exemption. Fixed-window, exact rolling-window, continuously replenished
token-bucket and concurrency algorithms reject immediately with 429; there is no
implicit queue. Identity keys use verified tenant/subject, or one anonymous key.
Live quotas are not evicted to admit extra identities. Concurrency guards release
on completion/cancellation of pipeline execution, not the lifetime of arbitrary
application-owned response streams. Queue fairness/distributed quotas remain open.

Antiforgery is **off by default**, matching the pinned source rather than stale
upstream prose. `CookieSessions` issues bounded expiring opaque sessions only after
the application authenticates a principal. Its cookie is HttpOnly/SameSite=Lax;
deployment should require Secure. `CsrfPolicy::Session` validates an independent
session-bound `X-CSRF-Token` against the authenticated principal before any body
read, including streaming forms. Session revocation is checked before cached hits.
Session storage is in-process, not distributed login/identity infrastructure.

## Persisted response replay and crash boundaries

`ReplayPolicy` is separate from durable 202 message acceptance. At least one
tenant/user/endpoint dimension must be selected. Tenant/user values come only from
the verified principal; endpoint identity is the stable registration name plus its
resolved version, never a raw caller-supplied version/tenant hint. Keys
are bounded. Fingerprints SHA-256 exact length-framed method/path/raw-query/body
bytes, not a normalized DTO. Missing required key is 400; in-flight is 409; changed
fingerprint is 422. Completed responses persist status/body/Location only, with a
1 MiB body limit and 8192-byte Location limit. All persisted-response bounds are
checked before business commit. JSON Content-Type is reconstructed, not an
arbitrary saved header.

The migration `202611080007_http_responses.sql` adds independent response storage;
existing custody/idempotency tables and acceptance behavior are untouched.

- `SeparateClaim`: claim commits before work. Business writes/effects use the
  explicit scope from `EndpointContext::scope`. Serialization is checked before
  business commit. The business commit precedes separate response recording. Normal
  precommit failure rolls back work and compensates the fenced claim, including
  failure to open the business transaction after a confirmed claim commit.
  Cancellation/process crash can leave unanswered 409; postcommit failure must never release a
  claim and repeat committed work. `after_commit` runs in this recording window.
- `SameTransaction`: claim, business writes/effects and response recording use the
  **same Scope and PostgreSQL transaction**, without a hidden second transaction.
  Before-commit process death rolls back everything; after-commit death preserves
  the replay. Concurrent active transactions return immediate 409 using a
  transaction-owned advisory guard. This is deliberately not Wolverine's optimistic
  two-handlers-execute/loser-rolls-back Marten algorithm.
- A 400+ returned response is a precommit refusal: business changes roll back and
  the key is released. Optional-key requests without a key use that same refusal
  boundary; redirects commit their enrolled work too. Unsupported dynamic
  text/started arms commit an unanswered claim and warn rather than rerunning work. Always-unsupported declarations,
  custom writers and form/multipart replay combinations are rejected at startup.
  Combining output caching with replay on one endpoint is also rejected: a cache
  hit must not bypass required keys or raw-fingerprint mismatch checks.
  Rejecting all multipart replay is a conservative native limitation, not a claim
  that the pinned source blanket-bans file uploads.
- Expired rows remain honored until `Store::prune_http_responses` explicitly
  deletes a bounded page. Application owners choose retention/cleanup timing.
  Cleanup clamps future cutoffs to now and preserves unexpired claims. Owners
  must choose a window covering live work; expiry cleanup deliberately ends that
  key's protection window. Ownership tokens fence successor claims, completed
  answers cannot be overwritten/released, and stale completion poisons its scope
  with `IdentityConflict` rather than the .NET store's boolean update API.
  The response table is migration-provisioned; this is not upstream option-gated
  schema provisioning.

## Contract ledger and executable proof locations

“Closed subset” below means the listed native behavior has an executable contract,
not that the entire upstream row, .NET identity or issue 37 is closed. There is no
parity percentage. Exact qualification results belong in the issue/handoff, not
this durable contract reference.

| Pinned rows | Native implementation / proof source | Remaining clauses / row status |
| --- | --- | --- |
| HTTP-01–02 | Explicit prepared registration and shared runtime; `native_policies::native_middleware_holds_the_existing_core_runtime_permit` | Partial: CLR/ASP.NET ecosystem, discovery filters/hybrid ownership not implemented |
| HTTP-03–05 | All methods, optional/wildcard routes, strong parse mode, prefix/link/name APIs; `native_contracts` | Partial: complete ASP.NET constraints and discovery-derived identity collision policy remain |
| HTTP-06–08 | Aliases/defaults/nullable/scalar/collection query/header bindings and schemas; `route_ids_aliases_collections_defaults_typed_headers_and_named_problems` | Closed explicit native binding subset; automatic record/property/middleware inference remains partial |
| HTTP-09–12 | One-reader typed mixed-source binding, buffered form/files and shared incremental cursor; `native_uploads` | Partial: automatic model/service construction, temp-file buffering policies and every disconnect edge remain |
| HTTP-13–15 | Optional/required Serde JSON over real HTTP/1 and HTTP/2; `optional_json_transport_presence_over_chunked_http1_and_real_http2` | Partial: exhaustive HTTP/2 empty-stream distinctions; .NET serializer identities/overrides remain open |
| HTTP-16–17 | Typed primary response/effects, Location/status/text/empty/missing/started behavior; `return_categories_validation_and_configured_writer_negotiation_match_wire` | Partial: scope-wide missing-policy precedence and required-store-loader integration remain |
| HTTP-18–19 | Actual writer branches and exact request-media dispatch; native socket negotiation/media tests | Closed configured native subset; arbitrary user writer metadata remains explicit |
| HTTP-20 | No invented ReadsAttribute consumer | Declaration-only in pinned standard HTTP, not an automatic native reader requirement; later runtime negative counterpart remains distinct from explicit reading/media dispatch |
| HTTP-21–22 | Verified identities/Invoke/resource authorization, explicit anonymous flag, real sessions/header CSRF; `native_policies` | Partial: global/named claims policies, async auth and .NET middleware identity remain |
| HTTP-23 | Verified principal available to handler/effects | Partial: automatic user/correlation/event/outbox/trace relay remains |
| HTTP-24–28 | Safe ProblemDetails, async validation sentinel, typed map_error and ordered native lifecycle hooks | Partial: merged filtered catch hierarchy, all backend concurrency races, reusable validator adapters, request replacement policies remain |
| HTTP-29–30 | Explicit Rust dependencies/context/same transaction and ordered resource writers | Partial: keyed/request/isolated scope composition, clock/binding policy extension systems and .NET APIs remain |
| HTTP-31–34 | Offline schema generation, QUERY extension, typed metadata, native version selectors/expansion/headers; `native_contracts` | Partial: custom inference/provider conventions, filtered command/per-version documents and actual Asp.Versioning bridge remain |
| HTTP-35 | Explicit started Axum response/stream pass-through | Partial: full SSE/event/flush/slow-client/cancellation qualification remains |
| HTTP-36–38 | Separate response headers, bounded output cache/tag eviction/auth-before-hit, four finite rate algorithms; `native_policies` | Closed named native policy subset; host middleware identity, queues/fairness/distributed storage remain partial |
| HTTP-39–40 | Secure principal scoping, no path-derived membership grant | Partial/missing: tenant resolution strategies and refusal-only global logical dedup are separate future contracts |
| HTTP-41–44 | Real relational replay/scope/fingerprint/transactions; `native_replay` includes concurrent duplicates, successful neighbors/corrected retries, owned-process pre/postcommit crashes, immutable/fenced answers, Unicode/null fields and live-claim cleanup | Partial: unsupported typed ASP.NET arms, optimistic losing transaction algorithm, option-gated schema and every serializer/outbox failure window remain; no literal ComplianceTests harness identity claimed |
| HTTP-45–46 | Inline secondary effects and explicit same PostgreSQL HTTP scope | Partial: Send/Publish forwarding, automatic dependency-selected transactions, full outbox dispatch/flush ordering remain |
| HTTP-47–50 | No invented backend-aware HTTP or saga integration | Missing/native future wave; actual Marten/Polecat/Fisher/EF identities remain open |
| HTTP-51–56 | Legacy acceptance/operations left unchanged | Missing: peer HTTP messaging, push/PubSub/serverless and expanded stable Http DLQ contracts. AdminApi is source-only in the standard release pack list, not a proven stable package; alternative distribution is unestablished |
| HTTP-57–59 | Native offline route/OpenAPI contracts only | Partial/missing: correlated tracking, endpoint/event-model manifests and CLR codegen/static registries/assembly reuse remain |
| RPC-01–20 | Not owned by this HTTP implementation | Separate gRPC wave and subsequent .NET counterpart qualification; no closure inferred |
