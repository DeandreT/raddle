# raddle-servicebus

AMQP 1.0 Service Bus transport for raddle: authenticated TLS or secure WebSockets,
configured publishers, bounded peek-lock receivers, explicit settlement and lock
renewal, and named sessions. Applications supply existing queues and subscriptions.

Commit durable custody before completing an incoming delivery. Publisher success
means broker acceptance, not business completion. Retries must retain stable
message identities.

**Registry publication is disabled.** This package needs the narrowly patched
vendored `azservicebus` source. Git dependencies retain that source; packaging an
archive is not permission to replace it with the unpatched registry dependency.

- [Connection, settlement, sessions, and conformance](https://github.com/DeandreT/raddle/blob/main/docs/servicebus.md)
- [Vendored-source provenance and publication restriction](https://github.com/DeandreT/raddle/blob/main/THIRD_PARTY.md)
- [Independent Switchyard harness](https://github.com/DeandreT/raddle/tree/main/conformance)
- [Source repository](https://github.com/DeandreT/raddle)

Switchyard results do not establish live Azure compatibility. MIT licensed.
