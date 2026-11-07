# Vendored dependency

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

Bounded receives apply a single total deadline around protocol retries and drain,
fill only the remaining batch capacity, and return acquired partial batches on a
later error or deadline instead of dropping their custody. Idle waits may exceed
the ordinary operation timeout without becoming transport errors. The patched
source requires Rust 1.85 or later for lending async callbacks; raddle's pinned
toolchain is newer.
Receive recovery/backoff state survives short calls and cancellation. Manual
credit cleanup runs on drop; a blocked cleanup invalidates the link for recovery
instead of leaving it reusable with outstanding credit. Receiver custody epochs
invalidate held deliveries from recovered links rather than accepting stale-token
no-op dispositions. No extra background tasks are introduced.

Both Cargo workspaces select the same vendored source. The adapter's direct path
dependency also preserves that selection for Git consumers, not just workspace
builds. Registry publication must wait for an upstream fix or a separately
published patched dependency; it must not silently substitute the broken registry
package. Remove these patches only after conformance passes against their replacement.
