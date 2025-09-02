#!/usr/bin/env bash
set -euo pipefail

# this script will process pending_prune_days using the `auto_digest` action

# ========================================
# CONFIGURATION - EDIT THESE VALUES
# ========================================

# RPC URL for kwil node
PROVIDER="http://localhost:8484"

# Wallet private key to sign exec-sql transactions
PRIVATE_KEY="0000000000000000000000000000000000000000000000000000000000000001"

# Delete cap (maximum rows to delete per auto_digest call)
DELETE_CAP=10000

# Expected records per stream per day (used for batch size calculation)
EXPECTED_RECORDS_PER_STREAM=24

# Milliseconds to sleep between auto_digest calls (to avoid overwhelming the node)
SLEEP_MS=2000

# Optional namespace (leave empty to use default)
NAMESPACE=""

# ========================================
# SCRIPT START
# ========================================

# Start timing
SCRIPT_START_TIME=$(date +%s)
echo "🚀 Starting pending_prune_days processing at $(date)"

# Validate required configuration
echo "🚀 Starting auto_digest processing..."

TOTAL_PROCESSED_DAYS=0
TOTAL_DELETED_ROWS=0
DIGEST_CALLS=1

# Process all pending records using auto_digest
while true; do
  DIGEST_START_TIME=$(date +%s)

  echo "🔄 Digest call $DIGEST_CALLS (delete_cap=$DELETE_CAP, expected_records=$EXPECTED_RECORDS_PER_STREAM)"

  # Execute auto_digest action (capture JSON output and errors distinctly)
  OUT_FILE=$(mktemp)
  ERR_FILE=$(mktemp)
  NS_ARG=()
  if [[ -n "$NAMESPACE" ]]; then NS_ARG+=(--namespace "$NAMESPACE"); fi
  set +e
  kwil-cli exec-action auto_digest int:$DELETE_CAP int:$EXPECTED_RECORDS_PER_STREAM "${NS_ARG[@]}" \
    --provider "$PROVIDER" --private-key "$PRIVATE_KEY" --sync --output json \
    1>"$OUT_FILE" 2>"$ERR_FILE"
  STATUS=$?
  set -e
  RESULT="$(cat "$OUT_FILE" 2>/dev/null || true)"
  ERRMSG="$(cat "$ERR_FILE" 2>/dev/null || true)"
  rm -f "$OUT_FILE" "$ERR_FILE"

  if [[ $STATUS -ne 0 ]]; then
    echo "  ❌ Failed to execute auto_digest (exit $STATUS)"
    if [[ -n "$ERRMSG" ]]; then
      echo "  Error (stderr): $ERRMSG"
    else
      echo "  Error: unknown (no stderr captured)"
    fi
    if [[ -n "$RESULT" ]]; then
      echo "  Output (stdout): $RESULT"
    fi
    echo "  Command context: provider=$PROVIDER delete_cap=$DELETE_CAP expected=$EXPECTED_RECORDS_PER_STREAM namespace=${NAMESPACE:-<default>}"
    exit 1
  fi

  # Extract NOTICE log line emitted by the action and parse its JSON payload
  # We expect tx_result.log to contain lines like: "auto_digest:{...json...}"
  # Be robust to multi-line logs: pick the last line starting with our prefix
  # Extract the last auto_digest notice without causing pipeline failures when not present
  LOG_JSON=$(echo "$RESULT" \
    | jq -r '.result.tx_result.log // ""' 2>/dev/null \
    | tr '\r' '\n' \
    | sed -n 's/.*auto_digest://p' \
    | tail -1 || true)
  if [[ -n "$LOG_JSON" ]]; then
    PROCESSED_DAYS=$(echo "$LOG_JSON" | jq -r '.processed_days' 2>/dev/null || echo "0")
    DELETED_ROWS=$(echo "$LOG_JSON" | jq -r '.total_deleted_rows' 2>/dev/null || echo "0")
    HAS_MORE=$(echo "$LOG_JSON" | jq -r '.has_more_to_delete' 2>/dev/null || echo "false")
  else
    # Fallback to zeros if no log found
    echo "  ⚠️  No auto_digest NOTICE found in tx_result.log; raw result:" >&2
    echo "  $RESULT" >&2
    PROCESSED_DAYS=0
    DELETED_ROWS=0
    HAS_MORE=false
  fi


  

  # Calculate timing
  DIGEST_END_TIME=$(date +%s)
  DIGEST_DURATION=$((DIGEST_END_TIME - DIGEST_START_TIME))

  # Update totals
  TOTAL_PROCESSED_DAYS=$((TOTAL_PROCESSED_DAYS + PROCESSED_DAYS))
  TOTAL_DELETED_ROWS=$((TOTAL_DELETED_ROWS + DELETED_ROWS))

  echo "  ✅ Digest call $DIGEST_CALLS completed in ${DIGEST_DURATION}s"
  echo "     📅 Processed days: $PROCESSED_DAYS"
  echo "     🗑️  Deleted rows: $DELETED_ROWS"
  echo "     🔄 Has more to process: $HAS_MORE"

  # Check if we're done
  if [[ "$HAS_MORE" != "true" ]]; then
    echo "  🎯 All pending days have been processed!"
    break
  fi

  ((DIGEST_CALLS++))

  # Sleep between digest calls
  echo "  😴 Sleeping for ${SLEEP_MS}ms before next digest call..."
  python3 - "$SLEEP_MS" <<'PY'
import sys, time
ms=int(sys.argv[1]); time.sleep(ms/1000)
PY

  echo ""
done

# Calculate total execution time
SCRIPT_END_TIME=$(date +%s)
SCRIPT_DURATION=$((SCRIPT_END_TIME - SCRIPT_START_TIME))

# Calculate performance metrics
if [[ $SCRIPT_DURATION -gt 0 ]]; then
  DAYS_PER_SECOND=$((TOTAL_PROCESSED_DAYS * 100 / SCRIPT_DURATION))  # Using integer math
  DAYS_PER_SECOND_DECIMAL=$((DAYS_PER_SECOND / 100)).$((DAYS_PER_SECOND % 100))
  ROWS_PER_SECOND=$((TOTAL_DELETED_ROWS / SCRIPT_DURATION))
  CALLS_PER_SECOND=$((DIGEST_CALLS * 100 / SCRIPT_DURATION))  # Using integer math
  CALLS_PER_SECOND_DECIMAL=$((CALLS_PER_SECOND / 100)).$((CALLS_PER_SECOND % 100))
else
  DAYS_PER_SECOND_DECIMAL="N/A"
  ROWS_PER_SECOND=0
  CALLS_PER_SECOND_DECIMAL="N/A"
fi

echo "🎉 Digest processing completed!"
echo "📊 Final Summary:"
echo "   ⏱️  Total execution time: ${SCRIPT_DURATION} seconds"
echo "   🔄 Digest calls made: $DIGEST_CALLS"
echo "   📅 Total days processed: $TOTAL_PROCESSED_DAYS"
echo "   🗑️  Total rows deleted: $TOTAL_DELETED_ROWS"
echo "   📈 Average speed: ${DAYS_PER_SECOND_DECIMAL} days/second"
echo "   🚀 Deletion rate: ${ROWS_PER_SECOND} rows/second"
echo "   ⚡ Call frequency: ${CALLS_PER_SECOND_DECIMAL} calls/second"
echo ""
echo "✅ All pending_prune_days have been processed by auto_digest"
