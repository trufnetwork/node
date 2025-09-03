#!/usr/bin/env bash
set -euo pipefail

# ========================================
# CONFIGURATION - EDIT THESE VALUES
# ========================================

# RPC URL for kwil node
PROVIDER="http://localhost:8484"

# Wallet private key to sign exec-sql transactions
PRIVATE_KEY="0000000000000000000000000000000000000000000000000000000000000001"

# Directory containing chunk files (created by split_into_chunks.sh)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHUNK_DIR="$SCRIPT_DIR/chunk"

# Milliseconds to sleep between batches (to avoid overwhelming the node)
SLEEP_MS=200

# ========================================
# SCRIPT START
# ========================================

# Start timing
SCRIPT_START_TIME=$(date +%s)
echo "🚀 Starting batch insert process at $(date)"

if [[ ! -d "$CHUNK_DIR" ]]; then
  echo "ERROR: Chunk directory not found: $CHUNK_DIR" >&2
  echo "Please run ./split_into_chunks.sh first to create chunks" >&2
  exit 1
fi

# Count existing chunks
CHUNK_COUNT=$(ls "$CHUNK_DIR"/chunk_*.sql 2>/dev/null | wc -l)
if [[ $CHUNK_COUNT -eq 0 ]]; then
  echo "ERROR: No SQL chunk files found in $CHUNK_DIR" >&2
  echo "Please run ./split_into_chunks.sh first to create SQL chunks" >&2
  exit 1
fi

echo "Found $CHUNK_COUNT existing SQL chunk files in $CHUNK_DIR"

CHUNK_COUNTER=0
TOTAL_ROWS_PROCESSED=0

for chunk in "$CHUNK_DIR"/chunk_*.sql; do
  [[ -e "$chunk" ]] || { echo "No chunks found"; break; }

  # Start chunk timing
  CHUNK_START_TIME=$(date +%s)

  CHUNK_COUNTER=$((CHUNK_COUNTER + 1))
  CHUNK_NAME=$(basename "$chunk")

  echo "📄 Processing SQL chunk $CHUNK_COUNTER/$CHUNK_COUNT: $CHUNK_NAME"

  # Count INSERT values in this chunk (fastest method)
  CHUNK_ROWS=$(tr -cd '(' < "$chunk" | wc -c)
  TOTAL_ROWS_PROCESSED=$((TOTAL_ROWS_PROCESSED + CHUNK_ROWS))


  # Execute the SQL
  kwil-cli exec-sql --file "$chunk" --provider "$PROVIDER" --private-key "$PRIVATE_KEY" --sync

  # Calculate chunk timing
  CHUNK_END_TIME=$(date +%s)
  CHUNK_DURATION=$((CHUNK_END_TIME - CHUNK_START_TIME))

  echo "✅ SQL chunk $CHUNK_COUNTER completed in ${CHUNK_DURATION}s (${CHUNK_ROWS} rows)"

  # Throttle between chunks (except for the last one)
  if [[ $CHUNK_COUNTER -lt $CHUNK_COUNT ]]; then
    python3 - "$SLEEP_MS" <<'PY'
import sys, time
ms=int(sys.argv[1]); time.sleep(ms/1000)
PY
  fi

done

# Calculate total execution time
SCRIPT_END_TIME=$(date +%s)
SCRIPT_DURATION=$((SCRIPT_END_TIME - SCRIPT_START_TIME))

# Calculate performance metrics
if [[ $SCRIPT_DURATION -gt 0 ]]; then
  ROWS_PER_SECOND=$((TOTAL_ROWS_PROCESSED / SCRIPT_DURATION))
  CHUNKS_PER_SECOND=$((CHUNK_COUNTER * 100 / SCRIPT_DURATION))  # Using integer math for chunks/second
  CHUNKS_PER_SECOND_DECIMAL=$((CHUNKS_PER_SECOND / 100)).$((CHUNKS_PER_SECOND % 100))
else
  ROWS_PER_SECOND=0
  CHUNKS_PER_SECOND_DECIMAL="N/A"
fi

echo ""
echo "🎉 Batch insert process completed!"
echo "📊 Performance Summary:"
echo "   ⏱️  Total execution time: ${SCRIPT_DURATION} seconds"
echo "   📄 Chunks processed: $CHUNK_COUNTER"
echo "   📈 Total rows inserted: $TOTAL_ROWS_PROCESSED"
echo "   🚀 Average speed: ${ROWS_PER_SECOND} rows/second"
echo "   📦 Chunk processing rate: ${CHUNKS_PER_SECOND_DECIMAL} chunks/second"
echo ""
echo "✅ All SQL chunks processed successfully"