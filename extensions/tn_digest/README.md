# tn_digest extension

## What it does (brief)
- Periodically calls a Kuneiform action `main.auto_digest()` via real, signed transactions.
- Runs only when this node is the block leader (leader-gated scheduler with consensus checks).
- Reads enable/schedule from on-chain table `digest_config` and reconciles changes every N blocks.
- Uses singleton scheduler to prevent overlapping jobs and supports both 5-field and 6-field cron expressions.

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
INSERT INTO digest_config (id, enabled, digest_schedule)
VALUES (1, true, '*/10 * * * *');

-- Subsequent changes
UPDATE digest_config SET enabled = true, digest_schedule = '*/10 * * * *' WHERE id = 1;
```

### Leader Gating & Lifecycle
- Scheduler starts only when this node becomes leader and `enabled = true`.
- Scheduler stops immediately when leadership is lost or when `enabled` becomes false.
- The extension checks the config again every N blocks (default 1000, configurable below).

### Configuration (TOML)
- Reload interval (best effort parse of integer blocks):
```toml
[extensions.tn_digest]
reload_interval_blocks = "1000"   # default 1000; set to small values for faster reconciling
# optional explicit RPC URL (overrides [rpc].listen normalization)
# rpc_url = "https://127.0.0.1:8484"
```
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
