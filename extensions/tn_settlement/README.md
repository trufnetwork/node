# TN Settlement Extension

**Automatic market settlement for prediction markets on TRUF.NETWORK.**

This extension automatically polls for unsettled markets past their `settle_time` and broadcasts `settle_market()` transactions when attestations are available. It follows the same leader-only architecture as `tn_digest`.

## Architecture

- **Leader-Only Execution**: Uses `leaderwatch` to ensure only the current leader node runs the scheduler
- **Cron Scheduling**: Configurable polling frequency (default: every 5 minutes)
- **Retry Logic**: Exponential backoff with fresh nonce fetching on each attempt
- **Config Reload**: Dynamic configuration changes without restart

## Configuration

### Static Configuration (kwild.toml)

```toml
[extensions.tn_settlement]
enabled = true
schedule = "*/5 * * * *"  # Every 5 minutes (cron format)
max_markets_per_run = 10   # Limit markets processed per run
retry_attempts = 3         # Max retries per market
reload_interval_blocks = 100  # Check for config reload every N blocks
```

### Dynamic Configuration (Optional Database Table)

Create this table for runtime config changes:

```sql
CREATE TABLE main.settlement_config (
    id INT PRIMARY KEY DEFAULT 1,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    settlement_schedule TEXT NOT NULL DEFAULT '*/5 * * * *',
    max_markets_per_run INT NOT NULL DEFAULT 10,
    retry_attempts INT NOT NULL DEFAULT 3,
    CHECK (id = 1)  -- Enforce single-row table
);

-- Insert default config
INSERT INTO main.settlement_config (id, enabled, settlement_schedule, max_markets_per_run, retry_attempts)
VALUES (1, TRUE, '*/5 * * * *', 10, 3);
```

## How It Works

### Scheduler Flow

```
Every N minutes (configurable):
  ├─ Query: SELECT id, hash FROM ob_queries
  │         WHERE settled=FALSE AND settle_time <= NOW()
  │         LIMIT max_markets_per_run
  │
  ├─ For each market:
  │  ├─ Check if attestation exists AND is signed
  │  ├─ If yes → Broadcast settle_market(query_id) transaction
  │  ├─ Retry up to N times with exponential backoff
  │  └─ Log success/failure
  │
  └─ Summary: total markets, settled count, failed count, skipped count
```

### Settlement Transaction

Each settlement broadcasts:
```sql
-- Action call via signed transaction
settle_market($query_id)
```

The action:
1. Validates settlement time has arrived
2. Retrieves signed attestation for market hash
3. Parses result to determine winning outcome
4. Updates market: `settled=TRUE, winning_outcome=result, settled_at=timestamp`

## Log Messages

### Scheduler Lifecycle
```
INFO: tn_settlement scheduler started, schedule=*/5 * * * *
INFO: tn_settlement acquired leadership, starting scheduler
INFO: tn_settlement lost leadership, stopping scheduler
INFO: tn_settlement scheduler stopped
```

### Settlement Job Execution
```
INFO: starting settlement job, max_markets=10, retry_attempts=3
INFO: found unsettled markets, count=5
DEBUG: attestation not yet available, skipping market, query_id=1
INFO: market settled successfully, query_id=2, tx_hash=0x...
WARN: failed to settle market after retries, query_id=3, error=...
INFO: settlement job completed, total_markets=5, settled=4, failed=0, skipped=1
```

### Config Reload
```
INFO: settlement config changed, updating scheduler
INFO: tn_settlement (re)started with new schedule, schedule=*/10 * * * *
```

## Components

### Extension Core (`extension.go`)
- Singleton pattern
- State management (enabled, schedule, config)
- Background retry worker for config reloads

### Scheduler (`scheduler/scheduler.go`)
- Cron-based polling using `gocron` library
- Singleton mode prevents overlapping runs
- Context cancellation for graceful shutdown

### Engine Operations (`internal/engine_ops.go`)
- `FindUnsettledMarkets()` - Query markets ready for settlement
- `AttestationExists()` - Check if signed attestation available
- `BroadcastSettleMarketWithRetry()` - Build, sign, broadcast transaction

### Lifecycle Management (`scheduler_lifecycle.go`)
- Leader coordination via `leaderwatch` callbacks
- `OnAcquire()` - Start scheduler when node becomes leader
- `OnLose()` - Stop scheduler when node loses leadership
- `OnEndBlock()` - Check for config reload every N blocks

### Broadcaster (`broadcast.go`)
- JSON-RPC client wrapper
- URL normalization for client connections
- Transaction result polling

## Development

### Build
```bash
cd <repository-root>
task build
```

### Run Tests
```bash
# Unit tests
go test -v ./extensions/tn_settlement/internal/ -run TestEngineOps
go test -v ./extensions/tn_settlement/scheduler/ -run TestScheduler

# Integration tests (when implemented)
go test -v -tags kwiltest ./extensions/tn_settlement/tests/ -run TestSettlement -count=1
```

### Enable Extension

Add to `kwild.toml`:
```toml
[extensions.tn_settlement]
enabled = true
schedule = "*/5 * * * *"
```

Then start node:
```bash
kwild start
```

## Dependencies

- **Issue 7**: `settle_market()` action must be implemented in migrations
- **Kwil Extensions**: Hook system, precompile registration
- **Leader Watch**: Leadership coordination
- **gocron**: Cron scheduling library

## References

- **Reference Implementation**: `extensions/tn_digest/` (architecture template)
- **Settlement Action**: `internal/migrations/032-order-book-actions.sql:2171-2272`
- **Engine Operations**: `extensions/tn_digest/internal/engine_ops.go` (pattern reference)

## Status

✅ **Phase 1 Complete** - Core extension implemented and compiling
⏳ **Phase 2 Pending** - Unit tests and integration tests
⏳ **Phase 3 Pending** - Deployment and production validation

## Next Steps

1. **Create unit tests** for engine operations
2. **Create integration tests** for end-to-end settlement
3. **Deploy to devnet** and validate automatic settlement
4. **Monitor logs** for settlement activity
5. **Deploy to production** after successful devnet validation
