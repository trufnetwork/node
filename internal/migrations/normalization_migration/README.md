NOTE: this directory is temporary and should be removed after the migration is complete.

We need some of these migration scripts that are customized to run in the correct order, because before our fork, part of the migrations were applied already.

This migration plan was executed against an isolated instance at AWS that is less performant than our nodes, but should be the base size of other nodes. It took 14 hours to complete (running non-stop). The unknown here is how it will behave in a network with heterogeneous nodes (not always same provider, etc).

Requirements
- Postgres image with fine-tuned configurations for performance.
- Kwil accepting partial indexes

## Migration 

To run these, replace the necessary variables and run `bash ./migrate_and_report.sh`. I.e. replace `PRIVATE_KEY` with your private key, and `csv_path` with the path to the csv file you want to save the results to.

### What to run?

01migrate-structure.sql: 12 seconds
02_insert_actions.sql: instant
02migrate-streams_dpid.sql: 12 seconds / 100K rows (total: ~200K)
03migrate-streams_id.sql: 6 seconds / 100K rows (total: ~200K)
04migrate-structure_2.sql: 2 seconds
05migrate-primitives.sql: 6~10 seconds / 10K rows (total: >60M)
06-after-primitives.sql: 12 seconds

The total time for the migration depends on how busy we want to make our network.

Considering the solution with partial index implemented:

Total raw migration time: 7 hours
Considering 5% busy: 140 hours (6 days)
Considering 10% busy: 70 hours (3 days)

If there wasn't risks, would be better to ignore partial indexes, with these calculations, but I think it's better to be safe on this.

### Postgres recommendations

#### Normal operations (postgresql.conf settings)

```
# Memory settings
shared_buffers (recommend 1/4 of RAM)
effective_cache_size (recommend 3/4 of RAM)
work_mem (recommend 32MB)
maintenance_work_mem (recommend 512MB up to 2GB for large migrations)

# Relax from migration settings but keep better than defaults
autovacuum_vacuum_scale_factor = 0.1        # Was 0.05 during migration, default is 0.2
autovacuum_analyze_scale_factor = 0.05      # Was 0.02 during migration, default is 0.1

# Keep improved thresholds for large tables
autovacuum_vacuum_threshold = 500           # vs default 50
autovacuum_analyze_threshold = 250          # vs default 50

# Revert cost settings to defaults - less aggressive than migration
autovacuum_vacuum_cost_delay = 20ms         # Back to default from 10ms
autovacuum_vacuum_cost_limit = 200          # Back to default from 400

# Keep improved worker settings
autovacuum_max_workers = 5                  # vs default 3
autovacuum_naptime = 1min                   # Back to default from 30s

# Disable migration-specific logging
log_autovacuum_min_duration = -1            # Back to default (disabled)
```


#### Migration period (postgresql.conf settings)

```
# Aggressive settings for bulk migration workload
autovacuum_max_workers = 5                  # vs default 3
autovacuum_naptime = 30s                    # vs default 1min
autovacuum_vacuum_cost_delay = 10ms         # vs default 20ms (faster cleanup)
autovacuum_vacuum_cost_limit = 400          # vs default 200 (more work per cycle)
autovacuum_vacuum_scale_factor = 0.05       # vs default 0.2 (trigger at 5% vs 20%)
autovacuum_analyze_scale_factor = 0.02      # vs default 0.1 (frequent stats updates)
autovacuum_vacuum_threshold = 500           # vs default 50
autovacuum_analyze_threshold = 250          # vs default 50
log_autovacuum_min_duration = 0             # Log all autovacuum activity
```

This configuration triggers autovacuum after ~5% of rows change instead of 20%, preventing excessive bloat during bulk updates.

Monitoring commands during migration:
```sql
-- Check autovacuum activity
SELECT schemaname, tablename, last_autovacuum, last_autoanalyze, 
       n_dead_tup, n_live_tup, 
       ROUND(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 2) as dead_ratio
FROM pg_stat_user_tables 
WHERE tablename IN ('primitive_events', 'streams')
ORDER BY dead_ratio DESC;

-- Monitor table bloat during migration
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
       n_dead_tup, n_live_tup
FROM pg_stat_user_tables 
WHERE tablename IN ('primitive_events', 'streams');
```