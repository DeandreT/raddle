# Sagas and request/reply

## Persisted sagas

Implement `SagaState` with a stable lowercase name and positive schema version.
Use a SQLx `Scope` to load/create the state under a tenant and business
correlation key. Loading locks the row, so competing transitions see committed
state rather than losing updates. `save_saga` additionally verifies its revision.
Keep transitions short; do not call external services while holding their locks.

State, emitted messages, timeout jobs, and inbox completion share the same
transaction. Dropping the scope rolls all of them back. `finish` saves a terminal
tombstone; completed states cannot be modified or restarted by later messages.

`schedule_saga_timeout` persists a new generation and due time. A timeout handler
loads the state and checks `accepts_timeout` with the current time before acting.
Superseded, cancelled, early, or completed-state timeouts are no-ops. Route this
reserved timeout contract only from trusted internal custody. Generation checks
are not a replacement for ingress authentication.

State schema mismatches fail closed. `migrate_saga` accepts the exact old version
and an explicit transformation into the new state type. It locks and upgrades
the record in a transaction; it does not replay application code or run business
transitions. Migration code is trusted application code and must preserve its
business invariants.

## Durable conversations

`Scope::request` takes `RequestOptions` with configured destination/reply aliases,
an expected authenticated responder, reply contract, and absolute deadline. It
stores the pending conversation and outgoing request atomically. Register the
remote handler normally; its database effects and `Scope::reply` can commit
together. `reply` accepts only the explicitly supplied reply endpoint allowlist.
Never construct that allowlist from inbound message headers.

The reply listener passes its authenticated source to `accept_reply`. The first
matching reply before the deadline wins. Unknown, late, duplicate, or already
terminal replies do not overwrite state. A reply from the wrong responder or
contract is rejected. Querying a conversation reconciles deadline expiry and
returns its durable result, so a caller can reconnect after a process restart.

`cancel_request` commits local cancellation and a best-effort cancellation
command atomically. Applications that support remote cancellation must handle
`raddle.cancel` cooperatively. Cancellation does not undo a committed remote
operation, and timeout means no reply was observed, not proof of remote failure.

## Ephemeral callers

`raddle::conversation::ReplyHub` provides bounded in-memory waiters when caller
recovery is not needed. Register before publishing, then deliver replies with an
independently authenticated source. Waiters have explicit timeouts; dropping a
waiter releases its slot. The hub does not spawn tasks or retry messages.
Request IDs must not be reused while late replies may still exist. For
restart-safe correlation and results, use the persisted conversation API.
