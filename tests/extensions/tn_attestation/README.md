# Attestation Extension End-to-End Test

End-to-end test that verifies the complete attestation workflow using real Docker containers.

## What This Tests

Validates the end-of-block queue processing workflow in a production-like environment.

**Complete flow:**
1. Submit `request_attestation` → stores in database
2. `queue_for_signing` precompile → adds hash to in-memory queue
3. End-of-block hook → dequeues and processes hashes
4. Query database → finds unsigned attestations
5. Sign → validator generates signature
6. Broadcast → submits `sign_attestation` transaction
7. Retrieve → `get_signed_attestation` returns signed payload

This test uses real Docker containers to verify the queue → end-of-block → signature workflow without mocking.

## Quick Start

```bash
# Run the complete test (builds, starts, tests, cleans up)
./test_tn_attestation.sh
```

## Other Commands

```bash
# Run tests only (containers must be running)
./test_tn_attestation.sh test-only

# View logs
./test_tn_attestation.sh logs

# Stop and cleanup
./test_tn_attestation.sh stop
```

## Test Flow Diagram

```
request_attestation (SQL)
    ↓
queue_for_signing (precompile adds to queue)
    ↓
onLeaderEndBlock (dequeues at end of block)
    ↓
processAttestationHashes (queries DB)
    ↓
sign & broadcast
    ↓
get_signed_attestation (retrieves payload)
```

## Success Criteria

- ✅ Request confirmed and stored in database
- ✅ Signature generated within 90 seconds (proves end-of-block worked)
- ✅ Signed payload retrieved successfully
- ✅ Payload structure valid: `canonical || 65-byte signature`

## Limitations

- Single validator (no multi-node consensus)
- Fixed validator key (deterministic for testing)
- No network failure simulation
- Assumes node is always leader

For more complex scenarios, see the integration test suite in `../../extensions/tn_attestation/`.

