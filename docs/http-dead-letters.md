# Secured HTTP dead-letter management

`ApiBuilder::dead_letters(store, authorizer)` opts into native PostgreSQL-backed
management at `/dead-letters`. `dead_letters_at(prefix, store, authorizer)` selects
an absolute nonempty ASCII prefix without a trailing slash, parameters, query or
fragment or dot-path segments. Neither registration starts a listener. The existing
business API,
payload-free `operations` routes and explicitly confirmed CLI remain unchanged.
Apply the versioned PostgreSQL migrations in a controlled migration step first.

The registered contracts and OpenAPI operations come from the same types:

- `POST /dead-letters/`: query canonical failed messages.
- `POST /dead-letters/replay`: independently replay the supplied delivery IDs.
- `DELETE /dead-letters/`: independently discard the supplied delivery IDs.

The trailing slash on the query/discard resource is intentional; there is no
implicit redirect or alternative method. Requests and responses are JSON.
`/openapi.json` uses the API's existing bearer authentication. These native contracts
are not ASP.NET/Wolverine JSON or binary wire compatibility promises.

## Authentication, authorization and confirmation

Keep this router private and supply an `Authenticator` that verifies bearer
signature, issuer and expiration. Cookie-only requests are not authenticated.
Use an explicit Authorization header, not an ambient browser cookie. Do not adapt
cookie identity into this authenticator without designing separate CSRF defenses.
TLS termination, browser origin policy and credential storage belong to the host.

Registration also **requires** a `DeadLetterAuthorizer` implementation. There is
no default policy. Its `authorize` method must check an explicit separate role for
`Search`, `Replay` and `Discard`. Search includes private payloads and headers;
ordinary status inspection permission is not automatically sufficient. All
database queries use only the verified principal's tenant and audit subject.
Caller-selected tenant/database fields are rejected, not used as an all-tenant
fallback. Envelope metadata and extra HTTP headers cannot upgrade authority.

For mutations, `confirm` must additionally verify application-owned approval for
the **specific action, tenant, ID list and reason**. A signed short-lived challenge
or application approval record can implement that policy. The library requires
a bounded opaque confirmation string but does not interpret its presence as
approval. Do not implement confirmation as `confirm=true`, a nonempty string test,
or an anonymous fallback. Callback failures use the same fixed safe error types.

## Query bounds and private data

Example search body:

```json
{"limit":2,"after":0,"destination":"orders","contract":"orders.submit","error_code":"validation"}
```

`limit` defaults to ten and must be 1–10. `after` is an exclusive nonnegative
durable sequence cursor. Destination and contract filters are exact bounded
strings up to 128 bytes; classified error filters are exact up to 64 bytes.
Unknown fields and empty/control-character filter strings are rejected. There
are no SQL expressions, wildcard scans or unrestricted exception-message searches.

The response contains `items` and `next_after`. Each item carries delivery `id`,
original `message_id`, durable `sequence`, destination and the validated canonical
raddle `envelope` (including actual failed payload, correlation and bounded
headers). Envelopes are limited to 1 MiB and 32 validated headers, so no query can
return more than ten bounded canonical envelopes. Storage-protected or corrupt
framing is not decrypted or returned as raw data by this slice; it yields a fixed
payload-free failure instead. Do not log successful private response bodies.

Destination/error filtering runs in PostgreSQL. Contract matching validates only
the bounded candidate page, rather than reading every failed payload. Follow
`next_after` even when `items` is empty; null means the scan reached the end.
Concurrent mutations can change later pages: pagination is not a cross-request
snapshot, and no unbounded total-count scan is performed. Query transactions are
explicitly read-only with a two-second statement and HTTP execution budget.

## Independent mutations and retained custody

Example application-approved replay/discard body:

```json
{"ids":["00000000-0000-4000-8000-000000000001"],"reason":"approved recovery","confirmation":"<application-issued approval>"}
```

Bodies are limited to 16 KiB. Lists contain 1–32 delivery IDs; reason is a
nonblank, control-free string up to 512 bytes and confirmation up to 128 bytes.
Each canonical UUID must be non-nil. Malformed, duplicate and oversized individual
IDs produce `invalid` at that position without blocking independent valid IDs.
Input strings are not echoed back. IDs identify deliveries, not message identities.

After authorization and confirmation, each ID has its own transaction and a
two-second budget. Response `results` contains an `index` and fixed outcome for
each position: `replayed`, `discarded`, `invalid`, `not_eligible` or `unknown`.
A 200 response means results were produced, **not** that every ID succeeded.
Missing, cross-tenant, concurrently locked or ineligible deliveries all report
`not_eligible` without disclosing which condition occurred. Fixed errors and traces
contain no database details, message payloads or exception strings.

Replay uses the existing Store replay contract, including tenant admission,
expiry checks and atomic `raddle_replay_audit`. It retains the original delivery
and message identities; it does not create successors or replay successful jobs.
It does not edit payloads. Correct an external failure and retry only the failed
eligible IDs, not the neighbors that already succeeded.

The HTTP path opts into `Store::replay_dead_letter_bounded`, sharing the original
replay implementation with transaction-local two-second PostgreSQL statement and
lock timeouts installed before quota admission. Discard uses the same server
timeout limits. These keep an abandoned blocked statement from indefinitely
holding admission/row locks; they are not hard real-time cancellation guarantees.
Ordinary `Store::replay` and CLI callers retain their existing budget ownership.

Discard locks only an eligible dead delivery and atomically writes a dedicated
`raddle_discard_audit` with actor, reason and `discarded_at`. Its terminal status is
`Discarded`, **not** successful `Completed`. DELETE removes it from the eligible
DLQ; it does not erase the canonical message. Original identity, fingerprint,
generation, body and any recovery metadata remain unchanged. Discarded work cannot
be claimed or replayed and does not block a later live partition member. Repeating
discard reports `not_eligible` and does not duplicate its audit.

Discarded records are retained deduplication tombstones. Re-staging identical
custody cannot resurrect them; changed content conflicts. They are excluded from
successful-completion counts and normal completed-tombstone pruning. The dedicated
audit foreign key restricts deletion, and pruning also excludes discard audits.
This API has **no automatic discarded-payload/audit purge**. Applications must
account for retained private payloads in their access, retention and privacy policy;
an independently designed explicit purge would end duplicate protection and is
outside this management slice.

## Cancellation and recovery

Dropping an operation or its deadline drops unfinished SQLx transactions for
rollback. Completed earlier IDs remain committed, and later independent IDs can
still proceed after a per-ID failure. A deadline or backend failure returns
`unknown`, never fabricated success: a transport failure during COMMIT may have
committed. Reconcile tenant status and audit before retrying an unknown ID.
Server deadlines do not cancel or undo SQL that already committed.
Client disconnect does not guarantee server cancellation; the host may keep
processing until the bounded operation finishes. A lost response likewise does
not establish rollback. No request cancellation or failed authorization changes
the policy or cancels subsequent healthy requests.

## Public behavior reference and scope

The public behavior reference is Wolverine **V6.62.0**, pinned to
`3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e`:

- [HTTP routes and ID request](https://github.com/JasperFx/wolverine/blob/3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e/src/Http/Wolverine.Http/DeadLettersEndpointExtensions.cs)
- [Actual GET-request/result DTOs](https://github.com/JasperFx/wolverine/blob/3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e/src/Wolverine/Persistence/Durability/DeadLetterManagement/DeadLetterEnvelopeResults.cs)
- [Relational query/replay/discard handlers](https://github.com/JasperFx/wolverine/blob/3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e/src/Persistence/Wolverine.RDBMS/MessageDatabase.DeadLetterAdminService.cs)

This native slice supplies the HTTP-56 baseline of secured bounded search,
replay and discard. Its stricter tenant, approval, independent-result and retained
tombstone contracts are explicit native behavior. The upstream DTO's exception
matching, archive/failure-group/history options, total counts and database routing
are not advertised here. Separate source-only AdminApi JSON/HTML diagnostics,
ASP.NET `RouteGroupBuilder` identity, alternative distribution and full HTTP/AdminApi
family closure remain separate scope; no released AdminApi package or .NET
interoperability qualification is claimed.

## Local checks

Focused `raddle-axum` `dead_letters` integration tests use real HTTP sockets and
isolated SQLx-created PostgreSQL databases, not production data or tower-only
network substitutes. Supply a disposable local `DATABASE_URL` with database-create
permission, then run `cargo test -p raddle-axum --test dead_letters -- --include-ignored`.
The package-local migration upgrade test runs with
`cargo test -p raddle-postgres --test dead_letters -- --include-ignored`.
They exercise default/custom routes and generated contracts, roles and tenant
isolation, payload search/filter/cursors, bounds, audit rollback, invalid/stale/audit
failures at the beginning/middle/end, corrected failed-only retries, deadline
cancellation, retained discard identity/deduplication and partition progress.
