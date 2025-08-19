# tn_digest extension

## What it does (brief)
- Periodically calls a Kuneiform action `main.auto_digest()` via a real, signed transaction.
- Runs only when this node is the block leader (leader‑gated scheduler).
- Reads enable/schedule from on‑chain table `digest_config` and reconciles changes automatically.

---

## Operators

### Prerequisites
- Node key file exists at `<root>/nodekey.json` (used to sign digest txs).
- User JSON‑RPC is enabled (`[rpc].listen` set). If set to `0.0.0.0:<port>`, the extension internally uses `127.0.0.1:<port>` for client requests.
- Schema contains `digest_config` table.

### Enable/Disable and Schedule
- The extension reads a single row in `digest_config` (id = 1):
  - `enabled` (boolean): turns cron digestion on/off.
  - `digest_schedule` (cron string): when jobs run.
- Default if row missing: disabled, schedule `0 */6 * * *` (every 6 hours).

Minimal SQL to adjust:
```sql
-- Enable and set to every 10 minutes (assuming row id=1 is managed externally)
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
listen = "0.0.0.0:8484"           # the extension will connect to 127.0.0.1:8484 internally
```

### Observability
- Logs when scheduler starts/stops and on broadcast failures.
- If prerequisites are missing (key file, RPC address), warnings are logged and the job is skipped.

### Security Notes
- Transactions are signed with the node’s key and broadcast via local JSON‑RPC.
- Ensure JSON‑RPC service exposure complies with your security posture (TLS, firewall, etc.).
