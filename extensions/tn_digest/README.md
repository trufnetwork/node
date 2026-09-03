# tn_digest extension

## What it does (brief)
- Periodically calls a Kuneiform action `main.auto_digest()` via real, signed transactions.
- Periodically calls `main.auto_prune_duplicates()` the same way, on its own schedule.
- Runs only when this node is the block leader (leader-gated scheduler with consensus checks).
- Reads enable/schedule from on-chain tables `digest_config` and `duplicate_prune_config`, and reconciles changes every N blocks.
- Uses singleton scheduler to prevent overlapping jobs and supports both 5-field and 6-field cron expressions.

The two jobs are independent. Separate tables, separate enable flags, separate
schedules, separate crons, so turning one off or rescheduling it leaves the other
alone. They do share one thing: only one of them drains at a time, because both
broadcast from the node's signer account and two drains in flight would take the
same nonce.

---

## Operators

### Prerequisites
- Node key file exists at `<root>/nodekey.json` (used to sign digest transactions).
- User JSON‑RPC is enabled via `[rpc].listen`. The extension automatically normalizes the address:
  - `0.0.0.0:<port>` → connects to `127.0.0.1:<port>`
  - `[::]:<port>` or `:::<port>` → connects to `127.0.0.1:<port>`
  - Empty hosts → connects to `127.0.0.1:<port>`
  - Specific addresses → used as-is
- Schema contains `digest_config` table (created by migration but not auto-seeded).

### Enable/Disable and Schedule
- The extension reads a single row in `digest_config` (id = 1):
  - `enabled` (boolean): turns cron digestion on/off.
  - `digest_schedule` (cron string): when jobs run.
- Default if row missing: disabled, schedule `0 */6 * * *` (every 6 hours).

Minimal SQL to adjust:
```sql
-- First-time setup: ensure the single row exists
INSERT INTO main.digest_config (id, enabled, digest_schedule)
VALUES (1, true, '0 9 * * *');

-- Subsequent changes
UPDATE main.digest_config SET enabled = true, digest_schedule = '*/10 * * * *' WHERE id = 1;
```

### Duplicate pruning

A stream that publishes the same value every day stores one row a day forever, and
reads carry the last observation forward, so every row after the first in such a run
answers exactly what the row before it already answered. `auto_prune_duplicates`
removes them. Migrations 056 and 057 hold the rule and the reasoning; the summary is
that no read changes its answer, but a range read returns fewer points and an
anchored read reports an older `event_time` beside the same value.

The extension reads a single row in `duplicate_prune_config` (id = 1):
- `enabled` (boolean): turns the sweep on and off. **Ships false**, and it is the
  only gate, and there is no build flag to set as well.
- `prune_schedule` (cron string): when the sweep runs. Defaults to `0 */6 * * *`.
- `retention_days` (int): how old a record must be before it is a candidate.
  Defaults to 30. The scheduler does not pass this, so changing it here changes
  what the sweep does without a release.
- `last_stream_ref` (int): where the sweep is. Duplicate-ness is a property of a
  whole stream, so there is no queue to drain: the sweep walks `streams.id` in
  order and wraps at the end.

Unlike `digest_config`, migration 056 seeds this row, so a network that has run its
migrations always has one.

```sql
-- Turn the sweep on
UPDATE main.duplicate_prune_config SET enabled = true WHERE id = 1;

-- Prune less aggressively, or reschedule
UPDATE main.duplicate_prune_config
SET retention_days = 90, prune_schedule = '0 3 * * *' WHERE id = 1;
```

Both tables are consensus state, so change them through a signed
`kwil-cli exec-sql` rather than psql: a direct write on one node diverges its
AppHash.

**Before turning it on**, read the two things a firing costs. Each run visits 100
streams and scans their whole history inside one consensus transaction, and a run
that deletes leaves dead tuples behind, so pruning a long backlog wants a
`pg_repack` after it, with the transient disk that implies. And the sweep is cyclic, so
`has_more_to_delete` means "the cursor has not finished a pass" rather than "there
is more to delete": a firing runs its whole loop rather than stopping early, which
on a large network is by design.

### Leader Gating & Lifecycle
- Each job starts only when this node becomes leader and its own `enabled = true`.
- Both stop immediately when leadership is lost, and each stops when its own `enabled` becomes false.
- The extension checks the config again every N blocks (default 1000, configurable below).

### Configuration (TOML)
- Reload interval and retry settings:
```toml
[extensions.tn_digest]
reload_interval_blocks = "1000"          # default 1000; how often to check digest_config for changes
reload_retry_backoff_seconds = "60"     # default 60; wait time between config reload retries
reload_max_retries = "15"               # default 15; max attempts to reload config before giving up
# optional explicit RPC URL (overrides [rpc].listen normalization)
# rpc_url = "https://127.0.0.1:8484"
```

**Config reload resilience**: If reading `digest_config` fails (e.g., database timeout), the extension retries up to `reload_max_retries` times with `reload_retry_backoff_seconds` between attempts. If all retries fail, the current config is preserved and the scheduler continues running (won't stop due to transient failures).
- RPC listen (must be enabled if `rpc_url` not set):
```toml
[rpc]
listen = "0.0.0.0:8484"           # extension connects to 127.0.0.1:8484 internally
```

### Observability
- Logs when scheduler starts/stops, leader transitions, and config reloads.
- Logs detailed warnings when prerequisites are missing (broadcaster, signer, engine, service).
- Logs broadcast failures and transaction hashes on success.
- The extension gracefully handles missing `digest_config` table without crashing.

### Security Notes
- Transactions are signed with the node's private key and broadcast via local JSON-RPC.
- Supports different broadcast modes: async (fire-and-forget) or sync (wait for acceptance/commit).
- The extension only runs when the node is the consensus leader (enforced via on-chain leader checks).
- Ensure JSON-RPC service exposure complies with your security posture (TLS, firewall, etc.).
- The `auto_digest` action should implement its own access controls if needed.
