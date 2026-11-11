# TN Vacuum Extension

Automated database maintenance through periodic vacuuming operations. Reclaims disk space and optimizes database performance by removing dead tuples using `pg_repack`.

## Configuration

Add to your node's configuration file (all values are strings due to the
extension config format):

```toml
[extensions.tn_vacuum]
enabled = "true"
block_interval = "50000"
# Optional tuning
# pg_repack_jobs = "1"      # limit pg_repack concurrency (default: auto)
# pg_repack_user = "tn_repack"  # role pg_repack connects as (default: see below)
# pg_repack_password = ""       # that role's password, if Postgres asks for one
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | string (`"true"`/`"false"`) | `"true"` | Enable/disable the vacuum extension |
| `block_interval` | string (numeric) | `"50000"` | Number of blocks between vacuum runs |
| `pg_repack_jobs` | string (numeric) | _unset_ | Passes `--jobs=N` to `pg_repack` to throttle concurrency (omit to use the binary default) |
| `pg_repack_user` | string | _unset_ | Postgres role `pg_repack` connects as. Unset, it is the node's own database user when that user is a superuser, and `tn_repack` otherwise |
| `pg_repack_password` | string | _unset_ | Password for `pg_repack_user`, if Postgres asks for one |

**Tuning `block_interval`:**
- High-write environments: `25000` - `50000` blocks
- Read-heavy environments: `75000` - `100000` blocks
- Minimum: `1` block (enforced)

## Prerequisites

Requires `pg_repack` binary installed and in system PATH.

> **Note:** If you're using the official TrufNetwork node image, `pg_repack` is already included. Skip installation.

**For custom installations:**

**Ubuntu/Debian:**
```bash
sudo apt-get install postgresql-client-16 postgresql-16-repack
```

**RHEL/CentOS:**
```bash
sudo yum install postgresql16 pg_repack_16
```

## Database Roles

`pg_repack` runs under its own Postgres role, `tn_repack`, which the node's
[`deployments/postgres-roles.sql`](../../deployments/postgres-roles.sql)
creates. The script also creates the `pg_repack` extension, which only a
superuser may do, and lets no role but `tn_repack` and superusers use it.
`tn_repack` is not a superuser: it acts on the node's tables as a member of the
`kwild` role, and owns what its runs create in the `repack` schema.

`pg_repack` copies each table with `CREATE TABLE ... AS` and, until it swaps
the copy in, replays on the copy the writes the table takes. The node's
publication covers every table, so Postgres refuses an update or delete on the
copy unless the copy has a replica identity. The script creates an event
trigger, `set_replica_identity_on_repack_copy`, that gives the tables
`CREATE TABLE ... AS` makes in the `repack` schema full replica identity. It
leaves `set_replica_identity_on_create`, which `kwild` checks at startup, as
`kwild` expects it.

When `pg_repack` connects as a role other than the node's own database user,
the extension passes `--no-superuser-check` to it, and clears what an
interrupted run left behind by dropping those objects one by one.

On a node whose database user is still a superuser, and with `pg_repack_user`
unset, `pg_repack` connects as that user, as before, and the extension creates
the `pg_repack` extension itself on first run.

## State Persistence

The extension keeps a lightweight bookkeeping table in your node database at
`ext_tn_vacuum.run_state`. On every successful run it records the block height
and timestamp, allowing restarts to pick up the schedule without re-running
immediately. The schema is prefixed with `ext_`, so it is ignored by consensus
hashing and remains entirely node-local.

## Metrics

When OpenTelemetry is enabled, the extension provides:

**Counters:**
- `tn_vacuum.vacuum_start_total` - Vacuum operations started
- `tn_vacuum.vacuum_complete_total` - Successful completions
- `tn_vacuum.vacuum_error_total` - Errors encountered
- `tn_vacuum.vacuum_skipped_total` - Operations skipped

**Histograms:**
- `tn_vacuum.vacuum_duration_seconds` - Duration of operations
- `tn_vacuum.tables_processed` - Tables processed per run

**Gauges:**
- `tn_vacuum.last_run_height` - Block height of last run

## Troubleshooting

### pg_repack Not Found

**Symptom:** Logs show "pg_repack binary not found"

**Solution:**
1. Install pg_repack (see [Prerequisites](#prerequisites))
2. Ensure it's in system PATH
3. Restart the node

### Permission Errors

**Symptom:** Vacuum fails with permission denied, or the extension logs that it
could not ensure the `pg_repack` extension

**Solution:** Run [`deployments/postgres-roles.sql`](../../deployments/postgres-roles.sql)
as a Postgres superuser, then restart the node. The script is safe to run again.

### Replica Identity Errors

**Symptom:** A run fails with exit status 254, and its output reads
`cannot update table "table_<oid>" because it does not have a replica identity
and publishes updates` (or `cannot delete from table ...`)

**Cause:** The table took an update or a delete while `pg_repack` worked on it,
and the database lacks the event trigger that gives `pg_repack`'s copies full
replica identity. `pg_repack` drops its copy when it fails, so nothing is left
behind.

**Solution:** Run [`deployments/postgres-roles.sql`](../../deployments/postgres-roles.sql)
again as a Postgres superuser, between runs: first check that no `pg_repack`
session is open (`SELECT count(*) FROM pg_stat_activity WHERE usename = 'tn_repack'`
returns 0), because the script clears an interrupted run's leftovers and would
wait on a live run's locks. On a node whose `kwild` already connects without
superuser rights, `kwild` need not restart; the next run uses the trigger. A node
whose `kwild` still connects as a superuser has to stop `kwild` first, as
[Moving an Existing Node Off the Superuser Role](../../docs/node-operator-guide.md#moving-an-existing-node-off-the-superuser-role)
describes.

**Before you do:** without the trigger, a run stops at the first table that
takes an update or a delete while it is copied. With it, a run copies every
table, and each copy passes through the node's replication slot like any other
write. On a large database the copy of the biggest table can take longer to pass
through than the node waits for a block to commit (30 minutes), and `kwild` then
restarts and the run starts over. Each table is also swapped in under an
`ACCESS EXCLUSIVE` lock, and `pg_repack` cancels the sessions that hold it up.
On a node where `tn_vacuum` is enabled, confirm that its biggest table copies
well within that limit, or set `enabled = false` under `[extensions.tn_vacuum]`,
before you run the script.

### High Memory Usage

**Symptom:** Node memory spikes during vacuum

**Solution:** Increase `block_interval` to run less frequently

## Performance Impact

Vacuum operations are non-blocking but consume resources:
- **CPU**: Moderate during operation
- **Memory**: Proportional to table sizes
- **Disk I/O**: Significant but spread over time

**Best Practices:**
- Start with default interval (50k blocks)
- Monitor for 24-48 hours before adjusting
- Consider database size and growth rate

## Security Note

Database credentials are passed via environment variables to `pg_repack`. It
runs as the `tn_repack` role described in [Database Roles](#database-roles),
which is not a superuser.

## Related Documentation

- [pg_repack Documentation](https://reorg.github.io/pg_repack/)
- [PostgreSQL VACUUM](https://www.postgresql.org/docs/current/sql-vacuum.html)
