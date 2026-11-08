# Declared native message families

`RequestFamily` and `EventFamily` bind one reusable async behavior to explicitly
declared concrete types. The application uses an enum or typed adapter to express
the shared input. There is no class hierarchy, CLR assignability, trait-object
discovery, assembly scanning or reflection. Ordinary registrations remain exact
Rust `TypeId` matches; declaring a family installs real handlers for its members.

## Requests

Run `cargo run -p raddle --example families` for the complete request example.
The shared behavior accepts `(Family, ExecutionContext)` and returns an owned
future. Declare each concrete variant with `member::<Concrete>(Enum::Variant)`
or another synchronous typed converter, then install the group with
`RuntimeBuilder::handle_family`.

Every member must implement `Request` with exactly the shared reply and domain
error types. Rust checks these associated types at compilation. Requests, replies
and the family input need not implement `Clone` or `Sync`; the reusable behavior
and converters must be `Send + Sync + 'static`. The behavior is bound once and
intentionally shared through `Arc` by the installed adapters. Captured application
state is explicit ownership, not a dependency-injection container or proof of
dependency-injection lifecycle support.

`Runtime::invoke`, `invoke_with_context` and `has_request` recognize the installed
concretes. `MessageBus::invoke` therefore selects the same local handlers before
remote routes. Bus invocation still requires `WireMessage` on each concrete
request and its reply; the family input does not become a wire contract. Typed
local domain failures remain typed, and undeclared types are not family members.

## Events

An event converter receives the publication's existing `Arc<Concrete>`, not a
cloned event body. For example:

```rust
use std::convert::Infallible;
use std::sync::Arc;
use raddle::{Event, EventFamily, RuntimeBuilder};

struct Created(u64);
struct Updated(u64);
impl Event for Created { type Error = Infallible; }
impl Event for Updated { type Error = Infallible; }
enum Change { Created(Arc<Created>), Updated(Arc<Updated>) }

fn configure() -> Result<raddle::Runtime, Box<dyn std::error::Error>> {
    let mut audit = EventFamily::new(|change: Change, _context| async move {
        let id = match change {
            Change::Created(event) => event.0,
            Change::Updated(event) => event.0,
        };
        let _audited_id = id;
        Ok::<(), Infallible>(())
    });
    audit.member::<Created>("audit-created".parse()?, Change::Created)?;
    audit.member::<Updated>("audit-updated".parse()?, Change::Updated)?;
    let mut builder = RuntimeBuilder::default();
    builder.subscribe_family(audit)?;
    Ok(builder.build())
}
```

Each concrete event has an explicit runtime-wide unique subscriber name. For
another independent business behavior, declare another family with different
names. Ordinary subscribers and family subscribers can coexist, share the same
source allocation, and retain separate outcomes. Domain failures do not cancel
siblings or later publications. Report order remains registration order, not
completion order. Every member's `Event::Error` must equal the family's error.

## Registration and execution contracts

- Declarations are staged and do not mutate a runtime builder. A duplicate
  concrete in one family is an ambiguous converter and returns `DuplicateMember`,
  even if a second event declaration supplies another name. To subscribe another
  behavior, use a separate event family or ordinary subscription.
- A failed member declaration leaves the earlier declaration intact. The caller
  can correct the error and continue; the failed member or name is not reserved.
- Installation rejects empty declarations, request-type collisions, and
  subscriber-name collisions before installing any member. On failure the
  consumed group is dropped; existing registrations and all free slots/names
  remain unchanged. There is no partially replaced handler or fallback ordering
  between overlapping request families.
- Each concrete handler or event subscriber acquires one ordinary shared runtime
  permit. Conversion and behavior run inside that admission boundary, never in a
  hidden producer task. Context is passed unchanged and remains diagnostic only.
  The bus's existing metadata-to-local-context behavior is unchanged.
- Dropping invocation or publication futures cancels unfinished work and releases
  admission. Closing the runtime prevents new/waiting adapters from entering.
  Panics unwind normally, and cancellation does not undo completed side effects.
  No automatic retry, persistence, recursive runtime invocation, or task ownership
  is introduced.

## Scope

This is the native declared-family clause corresponding to Wolverine V6.62.0
(`3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e`) public handler-interface and
abstract-message behavior, not an implementation of CLR inheritance semantics.
It does not claim whole runtime-family or RT07 completion.
See the pinned [handler rules](https://github.com/JasperFx/wolverine/blob/3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e/docs/guide/handlers/index.md#rules-for-message-handlers)
and [discovery documentation](https://github.com/JasperFx/wolverine/blob/3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e/docs/guide/handlers/discovery.md#handler-type-discovery)
for the source behavior and its discovery boundary.

Existing concrete `handle`, `subscribe`, and typed pipeline contracts are
unchanged. Family-wide middleware configuration and inherited pipeline selection
are not provided. A family input may explicitly call an application-owned
pipeline, but that does not automatically apply concrete middleware registrations.
Wire inheritance, polymorphic broker fan-out, abstract wire contracts, automatic
trait-object interpretation and discovered handlers remain outside this API.
Declare and route each concrete wire contract explicitly; bus `send`/`publish`
retain their existing endpoint registrations rather than discovering runtime
event families.
