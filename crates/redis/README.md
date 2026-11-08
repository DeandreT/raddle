# raddle-redis

Redis Streams publishing, fenced receipt settlement and explicit scheduling.
XADD acceptance does not imply replica acknowledgement, disk fsync or completed
business handling. Startup durability checks enforce the selected broker policy.

Native settlement checks consumer identity and delivery generation atomically.
Invalid messages retain their receipts; applications must handle or quarantine
them before acknowledging custody. Delayed scheduling and release are caller-owned
passes, not hidden background workers. Redis scripts do not roll back earlier
writes when a later command fails, so partial-state and corrected retries matter.

See the repository's [streaming transport guide](../../docs/native-streams.md)
for durability, scheduling format, ownership, lease and interoperability limits.
