# raddle-admin

Payload-free PostgreSQL diagnostics for raddle. Supply `DATABASE_URL` through an
authorized child-process environment; never put credentials in command arguments.
The CLI prints neither its connection string nor underlying database errors.

```sh
raddle-admin check
raddle-admin stats <trusted-tenant>
raddle-admin inspect <trusted-tenant> <job-uuid>
raddle-admin replay <trusted-tenant> <job-uuid> <actor> '<reason>' --confirm-replay
```

`check`, `stats` and `inspect` open server-enforced read-only connections. Replay
is the only write operation; it requires the explicit confirmation flag, actor
and reason, uses the normal tenant-scoped replay rules, and records an audit row.
There is no automatic migration, provisioning, payload dump or bulk replay.
Database permissions and the process operator remain the authorization boundary.
Each invocation has a 20-second overall budget and two-second pool acquisition.
