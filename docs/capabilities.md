# Capabilities and ownership

raddle is an independent Rust messaging and application framework. Its public
contracts describe what each component does, rather than assuming all transports
or persistence backends have uniform guarantees.

## Application composition

| Capability | Component and ownership |
| --- | --- |
| Typed requests and independent event subscribers | `raddle`; explicit registration and shared bounded admission |
| Middleware and validation | Typed async pipelines; single-use continuation and ordinary application state |
| Cascading commands/events | Explicit bounded breadth-first runner; per-effect outcomes, no implicit replay |
| Routing | Versioned wire contracts resolved to configured symbolic aliases |
| Application lifecycle | `raddle-host`; named services, readiness generations, finite restarts, joined shutdown |
| Broker topology | Explicit RabbitMQ declarations; connection/configuration remain application owned |
| Durable execution | PostgreSQL/SQLite inbox/outbox, claims, retries, quarantine, retention and audited replay |
| Recurring messages | PostgreSQL intervals with explicit missed-tick policy and schedule-local transactions |
| Sagas | Typed correlation/transition registration over persisted state, generations and tombstones |
| Cluster coordination | PostgreSQL node membership and fenced named singleton-agent leases |
| Event-sourced state | Versioned PostgreSQL event streams, optimistic append and deterministic aggregate folds |
| Projections | Transactional per-projection/tenant/stream checkpoints and application read-model callbacks |
| HTTP | Typed authenticated binding, OpenAPI, bounded SSE, private revalidation and ingress budgets |
| gRPC | Standard HTTP/2 and Protobuf JSON bridge with typed registrations and bounded authenticated calls |
| Operations | Authorized readiness/queue views, read-only CLI diagnostics and explicit audited replay |
| Testing | Deterministic clocks/fault scripts, real SQL/broker integration and shared custody checks |

## Transaction and failure boundaries

Business SQL, inbox completion, saga/event changes, projection checkpoints and
outgoing intent share the application's actual SQLx scope. No hidden second
transaction supplies an apparent atomicity guarantee. Failed or interrupted
transactional operations must not be committed. A lost commit acknowledgment is
uncertain and must be reconciled through retained identities and revisions.

Local cascades are volatile; they are not a substitute for durable outbox intent.
Confirmed publication means broker acceptance, not consumer completion. Delivery
remains at least once. External systems require idempotency or explicit
compensation, even when database work is fenced and committed atomically.

Independent messages, schedules and projection streams have separate failure
boundaries. Successful work is retained when a failed item is corrected. Events
within one ordered aggregate stream cannot silently bypass a failed predecessor.

## Hosting and integrations

Applications own transport credentials, trusted tenants, endpoints, service
registration, clock policy, handler state and deployment. Host tasks exist only
while their explicitly polled lifetime is running; a process-local readiness bit
does not certify every broker or external service. Cluster leases coordinate
configured agents and fence database effects, not arbitrary external operations.

Rust compilation, functions, generics and explicit registration provide the
application model. There is no runtime handler discovery, reflection-based DI
container or runtime code-generation dependency. Backend packages and network
protocols have their own tests and distribution constraints. Consult
[qualification](qualification.md) and each adapter guide before making a platform,
cloud compatibility or publication claim.
