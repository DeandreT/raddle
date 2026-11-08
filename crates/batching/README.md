# raddle-batching

Typed, caller-driven receive-side aggregation with bounded size/time assembly,
trusted tenant/group boundaries, original contexts, last-value-wins input views
and explicit original-member settlement decisions. Default bounds are 100
originals and 250ms. Custom shapes are ordinary typed transformations of borrowed
input; no erased message collection or hidden execution task is required.

The optional `postgres` feature executes fenced batches over the existing
`raddle-postgres` inbox jobs, retaining all originals through coalescing, retry,
quarantine and recovery. Business writes use the same SQLx transaction as member
settlement. It supplies a caller-driven runner with deadlines and a durable,
one-time size-one isolation sweep.

Dedicated batch destinations explicitly opt out of sequencing against unbatched
destinations. Routing plans do not install themselves into the messaging runtime.
Volatile tracking is not durable custody and never acknowledges a broker delivery.
See [batching contracts and integration](../../docs/batching.md) for setup,
ownership, crash behavior and precise unsupported integration clauses.
