# Microsoft Service Bus emulator conformance

This is an independent local broker target alongside Switchyard. Both run the
same queue, topic/subscription, dead-letter, settlement, message/session renewal,
and wire-identity assertions in `tests/common`. An additional case waits for a
real five-second message lease to expire and verifies recovery after reconnecting.
No Azure subscription or cloud credentials are needed. The fixture is for
sequential disposable tests only.

The optional `durable_microsoft` target combines this broker with an isolated
PostgreSQL database. It publishes a persisted request through the outbox,
deliberately redelivers both request and reply after committed inbox custody,
and verifies one business execution and a recoverable durable conversation.

The images are pinned by digest: official Service Bus emulator 2.0.1 for
Linux/amd64 and SQL Server 2022. They need Docker Compose, several GB of RAM/disk,
and an available loopback port. SQL is not exposed to the host; AMQP and the
emulator health endpoint are bound to numeric loopback on dynamically assigned
ports. A project-scoped volume contains disposable SQL state.

## License acceptance

Read and accept the [Service Bus emulator license](https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt)
and [SQL Server Linux terms](https://go.microsoft.com/fwlink/?LinkId=746388)
before setting `RADDLE_MICROSOFT_ACCEPT_EULA=Y`. The Compose file defaults to no
acceptance. A generated local SQL password is a fixture credential, not an Azure
key; do not commit it or print the expanded Compose configuration.

## Run from the repository root

```sh
export RADDLE_MICROSOFT_ACCEPT_EULA=Y
export RADDLE_MICROSOFT_SQL_PASSWORD="Raddle9!$(openssl rand -base64 24)"
project="raddle-ms-$(date +%s)"
compose=conformance/microsoft/compose.yaml
docker compose -p "$project" -f "$compose" up -d --wait --wait-timeout 240
http=$(docker compose -p "$project" -f "$compose" port emulator 5300)
curl --fail --retry 30 --retry-all-errors --retry-delay 2 --max-time 10 "http://$http/health"
export RADDLE_MICROSOFT_TEST_ENDPOINT=$(docker compose -p "$project" -f "$compose" port emulator 5672)
export RADDLE_MICROSOFT_TEST_CONFIRM=isolated
cargo +1.97.1 test --manifest-path conformance/Cargo.toml --locked --test microsoft -- --ignored --test-threads=1
```

The explicit toolchain matches `conformance/rust-toolchain.toml`; invoking Cargo
from the repository root otherwise selects the older production toolchain.

For the full durable workflow, also supply `DATABASE_URL` for a disposable
PostgreSQL server with test-database creation permission, then run:

```sh
cargo +1.97.1 test --manifest-path conformance/Cargo.toml --locked --test durable_microsoft -- --ignored --test-threads=1
```

SQLx allocates an isolated database for that case. The PostgreSQL URL is not the
emulator's internal SQL Server database, and neither should contain application
or customer data. The test's dedicated `durable-requests` and `durable-replies`
queues are separate from the transport-only checks.

Always clean up the same project, including on test failure:

```sh
docker compose -p "$project" -f "$compose" down --volumes --remove-orphans
unset RADDLE_MICROSOFT_SQL_PASSWORD RADDLE_MICROSOFT_ACCEPT_EULA
unset RADDLE_MICROSOFT_TEST_ENDPOINT RADDLE_MICROSOFT_TEST_CONFIRM
```

Never point the test at an existing shared broker. Failed runs can leave messages
behind; recreate the disposable fixture rather than weakening identity checks.
The test constructs its public emulator connection string from the explicitly
selected loopback IP. Microsoft requires the logical link-address hostname to
match the connection OPEN hostname; mixing `localhost` with `127.0.0.1` fails
that check even though both resolve locally. Raddle still rejects plaintext
endpoints outside numeric loopback.

Emulator connections retain the normal bounded 30-second per-attempt deadline,
with automatic retries disabled. The shared receive checks use a matching
bounded wait rather than relying on in-memory broker response times.

## What this does not establish

The [official emulator limitations](https://learn.microsoft.com/en-us/azure/service-bus-messaging/overview-emulator)
exclude AMQP WebSockets, Entra ID, cloud network integration, partitioned entities,
and production durability/restart behavior. Passing this target verifies the
tested local AMQP contract, not those Azure-only capabilities or an SLA. Keep the
live Azure test separately opt-in for later verification of real service access.
