# Native streaming broker fixtures

Run `python conformance/streams/run.py nats redis mqtt` from this checkout.
The runner starts **one family at a time**, publishes only random numeric-loopback
ports, binds disposable state on the verified perok build disk, records actual
images, container IDs, broker logs and test exit codes, and removes only its exact
captured container IDs. It never provisions a managed service or installs a tool.
Each phase compiles under the shared native flock **before** starting its broker,
then copies its test executables into the run directory and runs those snapshots.
Broker runtime tests do not occupy the build lock or wait with an idle broker,
and later builds cannot replace the executable under an active test. Brokers run
as the invoking Linux UID so retained fixture state remains inspectable.
Build targets are checkout-specific by default. `--target-dir <path>` selects an
explicit directory on the verified perok volume; do not reuse another checkout's
target directory.
Readiness requires a broker protocol response, not just Docker's open port proxy:
NATS INFO, Redis PONG, or a successful MQTT5 CONNECT/CONNACK exchange.
Mount/target and readiness guards can be checked without Docker:
`python3 -B -m unittest discover -s conformance/streams -p 'test_*.py'`.
Standard pulled images remain Docker's shared cache; there are no run-created
networks, volumes or private image tags. Retained state/receipts are under
`/mnt/perok/build/raddle/parity-native/streams-runs/` for inspection.

Targets are NATS 2.14.7, Redis 8.2.9 and Mosquitto 2. If a target image is absent,
the runner explicitly records/prints the fallback NATS 2.12.5 or Redis 8.2.4;
that is not qualification of the missing target. Test output records the server's
actual version and native capabilities. XACKDEL is tested on the actual server.

NATS is Apache-2.0. For the unmodified Redis 8 local fixture, **AGPLv3 is the
selected licensing alternative**; this fixture does not accept RSAL or SSPL.
Mosquitto is offered under EPL-2.0 and the Eclipse Distribution License 1.0
(BSD-3-Clause). Fixture broker dependencies are separate programs, not copied or
relicensed into raddle. Rust clients keep their own Cargo license declarations.
No proprietary vendor EULA is accepted by this runner.
The Redis fixture invokes the image's unmodified core `redis-server` directly;
its default wrapper otherwise auto-loads unrelated search/vector modules. The
fixture keeps the original image and selects no proprietary module licensing.
Anonymous access is confined to random loopback-published fixture ports, and Redis
readiness requires an actual PONG rather than just a listening TCP socket.

These tests use anonymous loopback fixtures, not production credentials. TLS,
external OAuth/JWT rotation, clustered failover and foreign .NET counterparts
need their own focused qualification; local acceptance is not evidence for them.
