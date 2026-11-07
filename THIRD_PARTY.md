# Vendored dependencies

## azservicebus

`vendor/azservicebus` contains the published source of `azservicebus` 0.25.1,
authored by Minghua Wu, from <https://github.com/minghuaw/azservicebus>.
The registry archive SHA-256 is
`ee87ee5702a4a33f760859859b15a80dbdd666871e6f6209b945910bb81bb8db`.
The original package declares the MIT license in its preserved Cargo metadata.
The published package does not include a separate license file; existing source
and metadata notices are retained unchanged.

An integration fix preserves a custom endpoint's port and bracketed IPv6
authority when constructing AMQP/TLS/WebSocket connection URLs. Previously only
`host_str()` was retained, silently selecting the protocol's default port.
Regression tests cover TCP, WebSocket, and plaintext endpoint construction.

The explicit plaintext development path also keeps logical service identity
canonical (`amqps`) while the physical connection uses `amqp`. Upstream signs
connection-string credentials for that canonical identity; changing the requested
identity solely because the socket is plaintext makes those credentials disagree.
TLS validation is unchanged. One elided lifetime is
spelled explicitly to keep current Rust diagnostics clean.

Peek-lock settlement uses the existing management disposition protocol for both
link and management deliveries, validating the broker response before reporting
success. Link dispositions subsequently advance local credit/bookkeeping; they
are not treated as confirmation. Local tracking misses no longer count as
settlement success; requests still require the broker response.
Every attempt first rejects a locally expired broker-supplied lock deadline,
including its latest renewal, because a stale management abandon/dead-letter can
otherwise receive a successful no-op response. Missing deadlines still require
the management response; the host's UTC clock must be synchronized.
Successful responses arriving after the known deadline remain uncertain and do
not count as confirmed settlement.
Lock annotation decoding uses checked timestamp conversion: malformed values and
out-of-range/unbounded sentinels produce no finite deadline instead of panicking.
Session settlement selects the receiver's session lease rather than the message
annotation, preserving the authority of session renewal.

The management wrapper assigns unique request IDs, discards replies with another
correlation ID, and decodes status and typed bodies only after matching. This
also isolates late replies after cancellation without patching another dependency.

Bounded receives apply a single total deadline around protocol retries and cleanup,
fill only the remaining batch capacity, and return acquired partial batches on a
later error or deadline instead of dropping their custody. Idle waits may exceed
the ordinary operation timeout without becoming transport errors. The patched
source requires Rust 1.85 or later for lending async callbacks; raddle's pinned
toolchain is newer.
The receiver retains actual owned recovery and credit-cleanup futures across
short calls and cancellation, including recovery-failure backoff. A blocked
zero-credit flow retains its link and queue position instead of relying on a
best-effort destructor detach; later calls must finish cleanup before granting
new credit. Enqueueing revocation does not establish immediate broker observation.
Acquired partial deliveries survive caller cancellation and remain bounded by the
next requested batch size. Receiver custody epochs invalidate held deliveries
from recovered links rather than accepting stale-token no-op dispositions. No
extra background tasks or dependencies are introduced.

## fe2o3 AMQP client

The following published packages by Minghua Wu are vendored from
<https://github.com/minghuaw/fe2o3-amqp>. Each directory retains the published
`Cargo.toml.orig` and `.cargo_vcs_info.json`. Registry archive SHA-256 values are:

- `vendor/fe2o3-amqp`, version 0.14.1:
  `95c762db93ff5d7a5721c78ff41f522b24c68dbd0bc71ce0b9a8f49ab74bbb82`.
- `vendor/fe2o3-amqp-management`, version 0.14.0:
  `0582084762bdf022540c37868a0808e9f54dbcc51fe56f6212da59c167569cda`.
- `vendor/fe2o3-amqp-cbs`, version 0.14.0:
  `6cae904b214ffa3c9bae26e4129d300fe79189d2ef70503071fb25ff9127531e`.

Their published manifests declare `MIT/Apache-2.0`. The archives omit separate
license files. Each vendored directory includes the upstream MIT `LICENSE.txt`
from its recorded source revision: [core license](https://github.com/minghuaw/fe2o3-amqp/blob/e2d2850ddce54398e671f6e73fecca7d5ef1c081/LICENSE.txt)
and [management/CBS license](https://github.com/minghuaw/fe2o3-amqp/blob/05ee2797c15c8c3bf427ed9d31a27c4377363299/LICENSE.txt).

The core sender registers its unsettled payload and acknowledgment waiter before
enqueueing the first transfer. An immediate broker disposition therefore cannot
arrive before local registration and leave the publisher waiting for an already
consumed acknowledgment. A drop guard removes registration if nothing was
enqueued. Once any frame is enqueued, cancellation or a later send error retains
the original payload for acknowledgment or normal link resumption. Pre-settled
sends do not allocate a waiter. Deterministic unit tests cover the enqueue and
disposition boundaries without adding a production delay or a background task.

Management and CBS implementation sources are unchanged. Their manifests use
direct paths to the vendored core and management package. The azservicebus
manifest uses direct paths to all three. `fe2o3-amqp-ws` has no production
dependency on the core client and remains a registry package; the shared AMQP
types package also remains unchanged in the registry.

Both Cargo workspaces patch all four vendored packages. The complete direct-path
dependency graph also retains these sources for Git consumers, whose Cargo does
not inherit a dependency's `[patch.crates-io]` table. No downstream patch table is
required, and sender/session types must resolve to one core package. The vendored
packages are excluded from the application workspace and remain standalone Cargo
workspaces with their upstream test dependencies intact.

Registry publication must wait for upstream fixes or separately published patched
dependencies; it must not silently substitute the broken registry packages.
Remove these patches only after conformance passes against their replacements.
