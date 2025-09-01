#!/usr/bin/env bash
set -euo pipefail

# Required env:
#   PROVIDER   - RPC URL for kwil node
#   PRIVATE_KEY- Wallet to sign exec-sql
# Optional env:
#   CSV_FILE   - Path to candidates CSV (default: ./pending_prune_candidates.csv)
#   BATCH_SIZE - Rows per batch insert (default: 10000)
#   SLEEP_MS   - Milliseconds to sleep between batches (default: 200)

CSV_FILE=${CSV_FILE:-pending_prune_candidates.csv}
BATCH_SIZE=${BATCH_SIZE:-10000}
SLEEP_MS=${SLEEP_MS:-200}

if [[ -z "${PROVIDER:-}" || -z "${PRIVATE_KEY:-}" ]]; then
  echo "ERROR: PROVIDER and PRIVATE_KEY must be set" >&2
  exit 1
fi

if [[ ! -f "$CSV_FILE" ]]; then
  echo "ERROR: CSV file not found: $CSV_FILE" >&2
  exit 1
fi

mkdir -p .ppd_chunks
rm -f .ppd_chunks/* || true

# Split into chunk files (skip header)
awk 'NR>1 {print > ".ppd_chunks/chunk_" int((NR-2)/ENVIRON["BATCH_SIZE"]) ".csv" }' "$CSV_FILE"

echo "Generated $(ls .ppd_chunks | wc -l) chunk(s)"

for chunk in .ppd_chunks/*.csv; do
  [[ -e "$chunk" ]] || { echo "No chunks created"; break; }
  echo "Processing $chunk"
  VALUES=$(awk -F, '{printf "(%s,%s),", $1, $2}' "$chunk" | sed 's/,$//')
  if [[ -z "$VALUES" ]]; then
    echo "Empty chunk, skipping"
    continue
  fi
  SQL="INSERT INTO pending_prune_days (stream_ref, day_index) VALUES ${VALUES} ON CONFLICT (stream_ref, day_index) DO NOTHING;"
  kwil-cli exec-sql --stmt "$SQL" --provider "$PROVIDER" --private-key "$PRIVATE_KEY"
  # Throttle
  python3 - "$SLEEP_MS" <<'PY'
import sys, time
ms=int(sys.argv[1]); time.sleep(ms/1000)
PY

done

echo "Done."






