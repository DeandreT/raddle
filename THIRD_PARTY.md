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
TLS validation and settlement semantics are unchanged. One elided lifetime is
spelled explicitly to keep current Rust diagnostics clean.

Both Cargo workspaces select the same vendored source. The adapter's direct path
dependency also preserves that selection for Git consumers, not just workspace
builds. Registry publication must wait for an upstream fix or a separately
published patched dependency; it must not silently substitute the broken registry
package. Remove these patches only after conformance passes against their replacement.
