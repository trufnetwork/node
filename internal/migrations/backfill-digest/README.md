# Digest Backfill (One-time Migration)

Purpose: Pre-populate `pending_prune_days` with all primitive stream days that have raw data but are not yet queued, so `auto_digest`/`batch_digest` can process them.

This folder is temporary and should be deleted from the repo after the migration is completed across the network. This must be done after the new actions are already deployed, so backfill fills only the streams prior to the migration.

## Overview

- Source truth: `primitive_events` (raw), `streams` (filter primitive), `pending_prune_days` (queue)
- Day index: `day_index = floor(event_time / 86400)`
- Safety: exclude the current partial day (`<= floor((now_utc - 86400)/86400)`)
- Idempotent: `ON CONFLICT DO NOTHING`
- Handles case where `pending_prune_days` table doesn't exist yet (gracefully includes all candidates)

## Steps

1) Extract pending candidates from Postgres on a node with live data

- Run the read-only SQL to produce `pending_prune_candidates.csv`:

```bash
# Configure env: PG_URL
PG_URL=postgres://kwild@localhost:5432/kwild

# Use candidates.sql if pending_prune_days table exists
psql "$PG_URL" -v ON_ERROR_STOP=1 -A -F, -P footer=off -t -f candidates.sql > pending_prune_candidates.csv

# Or use candidates_no_table.sql if pending_prune_days table doesn't exist yet
psql "$PG_URL" -v ON_ERROR_STOP=1 -A -F, -P footer=off -t -f candidates_no_table.sql > pending_prune_candidates.csv
```

2) Insert candidates into the queue using kwil-cli in batches

- Configure env: `PROVIDER`, `PRIVATE_KEY` (authorized wallet)
- Execute the batch script; it will split CSV and send `exec-sql` inserts:

```bash
bash insert_batches.sh
```

3) Monitor and finish

- Run `auto_digest` on schedule; it will remove rows from `pending_prune_days` when days are fully processed.
- You can re-run steps safely; inserts are idempotent.

4) Cleanup

- After the migration is completed network-wide, remove this `backfill-digest` directory from the repo.

## Files

- `candidates.sql`: read-only query to compute `(stream_ref, day_index)` missing from `pending_prune_days` (requires table exists)
- `candidates_no_table.sql`: alternative query for when `pending_prune_days` table doesn't exist yet
- `insert_batches.sh`: batch insertion with `kwil-cli exec-sql`
