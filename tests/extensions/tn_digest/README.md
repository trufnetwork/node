# TN Digest Extension E2E Test

This directory contains end-to-end tests for the TrufNetwork digest extension (`tn_digest`).

## Overview

The E2E test verifies that the digest system works correctly with a real node, including:

- ✅ Extension registration
- ✅ Scheduler integration (automatic digest triggering every 30 seconds)
- ✅ Transaction nonce management and auto_digest broadcasting
- ✅ OHLC calculation and type flag verification (15 = OPEN+HIGH+LOW+CLOSE)
- ✅ Database configuration via main.digest_config table
- ✅ Context lifecycle management in background scheduler

## Test Architecture

```
tests/extensions/tn_digest/
├── test_tn_digest.sh          # Main test orchestrator
├── docker-compose.yml         # Test infrastructure definition
├── digest_e2e_test.go        # Go test implementation
├── configs/
│   ├── kwild-config-override.toml  # Node configuration
│   └── custom-entrypoint.sh      # Docker entrypoint
└── README.md                 # This file
```

## Running the Tests

### Prerequisites

- Docker and docker-compose installed
- Go 1.21+ for test compilation

### Full Test Run

```bash
# Run complete E2E test (builds, starts services, runs tests)
./test_tn_digest.sh

# The script will:
# 1. Build Docker image with tn_digest extension
# 2. Start PostgreSQL and kwild services
# 3. Run migrations to set up digest schema and actions
# 4. Configure digest extension via SQL
# 5. Execute Go test suite (TestDigestE2E)
# 6. Keep services running for inspection
```

## Test Scenarios

### 1. SchedulerIntegration Test
- Deploys test stream: `stdigsch123456789012345678901234`
- Inserts single primitive event (timestamp: 259200, value: 42)
- Waits for scheduler to trigger (every 30 seconds)
- Verifies type flag 15 in primitive_event_type table
- Confirms pending_prune_days is empty after processing
- Tests complete E2E scheduler workflow

### Current Test Coverage
- ✅ Extension registration and precompile initialization
- ✅ Database configuration via main.digest_config
- ✅ Scheduler execution and auto_digest broadcasting
- ✅ Transaction nonce management and error handling
- ✅ OHLC type flag verification (15 = OPEN+HIGH+LOW+CLOSE)
- ✅ Query result parsing from ExportToStringMap()
- ✅ End-to-end scheduler reliability testing

## Configuration

### Extension Configuration

The test configures the digest extension via SQL:

```sql
INSERT INTO main.digest_config (id, enabled, digest_schedule) 
VALUES (1, true, '*/30 * * * * *');
```

### Test Data

The test creates a single test record:

```
Stream: stdigsch123456789012345678901234
Timestamp: 259200 (Day 3)
Value: 42
Expected Result: Type flag 15 (OPEN+HIGH+LOW+CLOSE for single record)
```

## Expected Results

### Success Criteria

- ✅ Extension registered and visible in logs
- ✅ Scheduler executes every 30 seconds
- ✅ Auto_digest transactions broadcast successfully
- ✅ Type flag 15 found in primitive_event_type table
- ✅ Pending_prune_days empty after processing
- ✅ No transaction nonce errors or context cancellation issues

### Current Performance

- **Test Duration**: ~2 minutes (24 attempts × 5 seconds)
- **Scheduler Interval**: 30 seconds (configurable)
- **Query Response**: Instant with proper ExportToStringMap() parsing
- **Test Reliability**: Consistent results with proper error handling

## References

- [Digest Actions SQL](../../../internal/migrations/020-digest-actions.sql)
- [Extension Code](../../../extensions/tn_digest/)
- [Similar Test Pattern](../tn_cache_metrics/)