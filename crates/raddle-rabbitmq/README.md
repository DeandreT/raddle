# raddle-rabbitmq

RabbitMQ AMQP 0-9-1 transport for raddle. Configured route aliases use publisher
confirms and mandatory routing. Receivers use bounded prefetch and explicit
acknowledgment or rejection.

Commit durable custody before acknowledging an incoming message. An interrupted
send confirmation is uncertain, so retries retain the original envelope identity.
Durability also depends on the application's existing exchanges, bindings, and
queue configuration; this adapter does not provision that topology.

Production connections require TLS. Plaintext is limited to numeric loopback
fixtures. RabbitMQ does not provide the Service Bus session/lock-renewal contract.

- [Setup and backend-specific guarantees](https://github.com/DeandreT/raddle/blob/main/docs/adapters.md)
- [Real-broker durable request/reply example test](https://github.com/DeandreT/raddle/blob/main/crates/raddle-rabbitmq/tests/conversation.rs)
- [Source repository and Git dependency usage](https://github.com/DeandreT/raddle)

MIT licensed. Source versions are not evidence of a registry release.
