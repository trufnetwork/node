# TN Vacuum Extension

Automated database maintenance through periodic vacuuming operations. Reclaims disk space and optimizes database performance by removing dead tuples using `pg_repack`.

## Configuration

Add to your node's configuration file:

```toml
[extensions.tn_vacuum]
enabled = true
block_interval = 50000
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable/disable the vacuum extension (`true` or `false`) |
| `block_interval` | integer | `50000` | Number of blocks between vacuum runs |

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

The extension automatically creates the `pg_repack` PostgreSQL extension on first run.

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

**Symptom:** Vacuum fails with permission denied

**Solution:** Ensure database user has superuser privileges OR grant explicit permissions:
```sql
GRANT EXECUTE ON FUNCTION pg_repack.* TO your_user;
```

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

Database credentials are passed via environment variables to `pg_repack`. Requires elevated database privileges (superuser or pg_repack role).

## Related Documentation

- [pg_repack Documentation](https://reorg.github.io/pg_repack/)
- [PostgreSQL VACUUM](https://www.postgresql.org/docs/current/sql-vacuum.html)
