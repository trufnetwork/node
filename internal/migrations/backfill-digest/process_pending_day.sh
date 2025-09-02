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
DELETE_CAP=1000000

# Expected records per stream per day (used for batch size calculation)
EXPECTED_RECORDS_PER_STREAM=24

# Milliseconds to sleep between auto_digest calls (to avoid overwhelming the node)
SLEEP_MS=10

# Optional namespace (leave empty to use default)
NAMESPACE=""

# ========================================
# ADAPTIVE CONTROL (simple & robust)
# ========================================

# Target per-call runtime (seconds)
TARGET_SEC=10

# Baseline
INITIAL_DELETE_CAP=10000  # 10k baseline

# Absolute bounds for the cap
DELETE_CAP_MIN=5000
DELETE_CAP_MAX=1000000

# Smoothing and step limits
ADJUST_ALPHA=0.50           # 0..1; 0.5 is a good default
MAX_STEP_UP=1.50            # at most +50% increase per call
MAX_STEP_DOWN=0.60          # at most -40% decrease per call

# Use a separate variable for the current cap; start from baseline
CURRENT_DELETE_CAP="$INITIAL_DELETE_CAP"

# ========================================
# SCRIPT START
# ========================================

# Start timing
SCRIPT_START_TIME=$(date +%s)
echo "🚀 Starting pending_prune_days processing at $(date)"

# Initialize CSV log file in the same directory as the script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV_LOG="$SCRIPT_DIR/process_log.csv"

# Only write header if file doesn't exist or is empty
if [[ ! -f "$CSV_LOG" ]] || [[ ! -s "$CSV_LOG" ]]; then
  echo "processed_days,seconds_to_complete,has_more,deleted_rows,cap,expected_records" > "$CSV_LOG"
fi

# Validate required configuration
echo "🚀 Starting auto_digest processing..."

TOTAL_PROCESSED_DAYS=0
TOTAL_DELETED_ROWS=0
DIGEST_CALLS=1

# Process all pending records using auto_digest
while true; do
  DIGEST_START_TIME=$(date +%s%3N)

  echo "🔄 Digest call $DIGEST_CALLS (delete_cap=$CURRENT_DELETE_CAP, expected_records=$EXPECTED_RECORDS_PER_STREAM)"

  # Execute auto_digest action (capture JSON output and errors distinctly)
  OUT_FILE=$(mktemp)
  ERR_FILE=$(mktemp)
  NS_ARG=()
  if [[ -n "$NAMESPACE" ]]; then NS_ARG+=(--namespace "$NAMESPACE"); fi
  set +e
  kwil-cli exec-action auto_digest int:$CURRENT_DELETE_CAP int:$EXPECTED_RECORDS_PER_STREAM "${NS_ARG[@]}" \
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
    echo "  Command context: provider=$PROVIDER delete_cap=$CURRENT_DELETE_CAP expected=$EXPECTED_RECORDS_PER_STREAM namespace=${NAMESPACE:-<default>}"
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
  DIGEST_END_TIME=$(date +%s%3N)
  DIGEST_DURATION_MS=$((DIGEST_END_TIME - DIGEST_START_TIME))
  DIGEST_DURATION=$(echo "scale=3; $DIGEST_DURATION_MS / 1000" | bc)

  # Update totals
  TOTAL_PROCESSED_DAYS=$((TOTAL_PROCESSED_DAYS + PROCESSED_DAYS))
  TOTAL_DELETED_ROWS=$((TOTAL_DELETED_ROWS + DELETED_ROWS))

  echo "  ✅ Digest call $DIGEST_CALLS completed in ${DIGEST_DURATION}s"
  echo "     📅 Processed days: $PROCESSED_DAYS"
  echo "     🗑️  Deleted rows: $DELETED_ROWS"
  echo "     🔄 Has more to process: $HAS_MORE"

  # Log to CSV
  echo "$PROCESSED_DAYS,$DIGEST_DURATION,$HAS_MORE,$DELETED_ROWS,$CURRENT_DELETE_CAP,$EXPECTED_RECORDS_PER_STREAM" >> "$CSV_LOG"

  # -------------------------------
  # ADAPT: choose NEXT cap from last duration
  # -------------------------------

  # Safety: avoid divide by zero
  if [[ "$DIGEST_DURATION" == "0" ]]; then
    DIGEST_DURATION="0.001"
  fi

  # 1) Ratio controller: what cap would hit TARGET_SEC if scaling ~linear?
  RAW_NEXT=$(awk -v cap="$CURRENT_DELETE_CAP" -v t="$TARGET_SEC" -v d="$DIGEST_DURATION" \
    'BEGIN{ print cap * (t/d) }')

  # 2) Smooth the jump
  SMOOTH_NEXT=$(awk -v cap="$CURRENT_DELETE_CAP" -v raw="$RAW_NEXT" -v a="$ADJUST_ALPHA" \
    'BEGIN{ print cap + a * (raw - cap) }')

  # 3) Limit step size relative to current
  UPPER=$(awk -v cap="$CURRENT_DELETE_CAP" -v u="$MAX_STEP_UP"   'BEGIN{ print cap * u }')
  LOWER=$(awk -v cap="$CURRENT_DELETE_CAP" -v dn="$MAX_STEP_DOWN" 'BEGIN{ print cap * dn }')
  STEP_CLAMPED=$(awk -v x="$SMOOTH_NEXT" -v lo="$LOWER" -v hi="$UPPER" \
    'BEGIN{ if (x<lo) x=lo; if (x>hi) x=hi; print x }')

  # 4) Clamp to absolute bounds
  NEXT_CAP=$(awk -v x="$STEP_CLAMPED" -v lo="$DELETE_CAP_MIN" -v hi="$DELETE_CAP_MAX" \
    'BEGIN{ if (x<lo) x=lo; if (x>hi) x=hi; printf "%.0f", x }')

  # 5) Outlier guard: if runtime is way above target, cut more aggressively
  IS_OUTLIER=$(awk -v D="$DIGEST_DURATION" -v T="$TARGET_SEC" 'BEGIN{ print (D > 3*T) ? 1 : 0 }')
  if [[ "$IS_OUTLIER" -eq 1 ]]; then
    NEXT_CAP=$(awk -v cap="$CURRENT_DELETE_CAP" 'BEGIN{ printf "%.0f", cap/2 }')
  fi

  echo "     🔧 Adaptive cap: current=$CURRENT_DELETE_CAP next=$NEXT_CAP (duration=${DIGEST_DURATION}s, target=${TARGET_SEC}s)"
  CURRENT_DELETE_CAP="$NEXT_CAP"

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
