# raddle-host

Explicit Tokio application supervision for raddle. Register named service
factories, choose finite restart and shutdown policies, and await `Host::run`.
Services report readiness only after their own dependency checks. PostgreSQL
worker and confirmed outbox helpers use the existing durable execution APIs.

The host starts no task until its run future is polled. A normally returning run
has joined every owned task, including deadline-aborted tasks. Dropping run
aborts tasks but cannot asynchronously join them; always await it for graceful
shutdown. Service code must yield and must not detach its own work.

See [application hosting](https://github.com/DeandreT/raddle/blob/main/docs/hosting.md) and the
[PostgreSQL example](examples/postgres_host.rs). MIT licensed.
