#!/bin/bash

# Function to check if an environment variable is set and exit if not
check_env_var() {
    local var_name="$1"
    # Use indirect expansion to get the value of the variable named by var_name
    if [ -z "${!var_name}" ]; then
        echo "$var_name is not set"
        exit 1
    fi
}

# Parse flags. --prod switches the migration set from "all *.sql except
# *.prod.sql" (default, used for dev/testnet) to "*.prod.sql only" (used
# for mainnet-specific overrides). The two sets are deliberately disjoint;
# nothing applies both in one run.
PROD_ONLY=false
while [ $# -gt 0 ]; do
    case "$1" in
        --prod)
            PROD_ONLY=true
            shift
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 [--prod]" >&2
            exit 1
            ;;
    esac
done

# as it's a very important script, let's make sure that our environment is set up correctly
check_env_var "PRIVATE_KEY"
check_env_var "PROVIDER"

# Set SYNC to true by default if not set. SYNC=true makes EVERY file wait
# for block inclusion before proceeding — without it, kwil-cli exec-sql
# returns as soon as the tx is broadcast, so an execution-time failure
# (e.g., a multi-statement migration that aborts mid-tx) silently passes
# and the script claims success. The 2026-04 mainnet tx_id outage was
# caused by this exact mode (only the last file used to get --sync).
# Override with SYNC=false at your own risk.
SYNC="${SYNC:-true}"

# Retry knobs — every kwil-cli call goes through run_with_retry so a
# transient blip (DNS hiccup, dial timeout, leader rotation) doesn't abort
# a multi-file migration midway. Override via env for flaky networks.
MAX_ATTEMPTS="${MAX_ATTEMPTS:-5}"
RETRY_INITIAL_SECONDS="${RETRY_INITIAL_SECONDS:-2}"

# Poll knobs for the post-timeout "did the in-flight tx eventually land?"
# check. Sized for mainnet-grade block intervals: ~5s blocks × ~36 tries
# = ~180s upper bound, which covers leader rotation or a brief mempool
# stall without making the operator wait forever.
POLL_MAX_SECONDS="${POLL_MAX_SECONDS:-180}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"

# wait_for_tx polls `kwil-cli utils query-tx <hash>` until the tx appears
# in a block or POLL_MAX_SECONDS elapses. Returns 0 if found, 1 if it
# never landed.
#
# Why this exists: `kwil-cli exec-sql --sync` can hit a "timed out waiting
# for tx to be included in a block" while the tx is still legitimately in
# flight (the hash is already in the leader's mempool). Re-broadcasting
# it produces a "tx already in mempool / nonce reused" error, which the
# old retry loop would then exhaust attempts on. Polling first lets a
# slow-but-OK leader catch up before we declare the migration a failure.
wait_for_tx() {
    local hash="$1"
    local elapsed=0
    echo "  in-flight tx $hash — polling for inclusion (up to ${POLL_MAX_SECONDS}s)..." >&2
    while [ "$elapsed" -lt "$POLL_MAX_SECONDS" ]; do
        if kwil-cli utils query-tx "$hash" --provider "$PROVIDER" >/dev/null 2>&1; then
            echo "  tx $hash landed after ${elapsed}s" >&2
            return 0
        fi
        sleep "$POLL_INTERVAL_SECONDS"
        elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
    done
    echo "  tx $hash did not land within ${POLL_MAX_SECONDS}s" >&2
    return 1
}

# run_with_retry runs "$@" with retries on failure, plus dedicated handling
# for `--sync` timeouts where the tx is already broadcast. Behavior:
#   - exit 0 → return 0
#   - exit non-zero AND output looks like a sync-timeout (contains
#     "timed out waiting for tx to be included") → extract the 64-char
#     hex hash from the output and poll wait_for_tx; on success treat the
#     attempt as having succeeded so we don't re-broadcast a duplicate
#   - any other failure (or hash-poll also fails) → backoff and retry up
#     to MAX_ATTEMPTS, doubling the delay each time
#
# Output is captured (not streamed) so we can pattern-match for the
# timeout case; it's printed in full after each attempt so the operator
# still sees everything.
run_with_retry() {
    local attempt=1
    local delay=$RETRY_INITIAL_SECONDS
    local output rc hash
    while true; do
        output=$("$@" 2>&1)
        rc=$?
        printf '%s\n' "$output"

        if [ "$rc" -eq 0 ]; then
            return 0
        fi

        if printf '%s' "$output" | grep -q "timed out waiting for tx to be included"; then
            hash=$(printf '%s' "$output" | grep -oE '[a-f0-9]{64}' | head -n1)
            if [ -n "$hash" ] && wait_for_tx "$hash"; then
                return 0
            fi
        fi

        if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
            echo "  failed after $attempt attempt(s); aborting" >&2
            return 1
        fi
        echo "  attempt $attempt/$MAX_ATTEMPTS failed; retrying in ${delay}s..." >&2
        sleep "$delay"
        attempt=$((attempt + 1))
        delay=$((delay * 2))
    done
}

# Collect the migration files to apply. Default mode skips *.prod.sql
# (the embedded migration loader in internal/migrations/migration.go skips
# them too, so this keeps dev/testnet aligned). --prod mode does the
# opposite: ONLY *.prod.sql, for mainnet-specific overrides that have to
# be applied by hand against the production node.
files=()
for f in ./internal/migrations/*.sql; do
    if [ "$PROD_ONLY" = "true" ]; then
        [[ "$f" == *.prod.sql ]] || continue
    else
        [[ "$f" == *.prod.sql ]] && continue
    fi
    files+=("$f")
done
num_files=${#files[@]}

if [ "$num_files" -eq 0 ]; then
    if [ "$PROD_ONLY" = "true" ]; then
        echo "No *.prod.sql files found under ./internal/migrations/" >&2
    else
        echo "No migration files found under ./internal/migrations/" >&2
    fi
    exit 1
fi

if [ "$PROD_ONLY" = "true" ]; then
    echo "Applying $num_files prod migration file(s)"
else
    echo "Applying $num_files migration file(s)"
fi

# Run them with kwil-cli exec-sql. Every file gets --sync (when SYNC=true,
# the default) so an execution-time failure surfaces immediately instead
# of being masked by the next file's broadcast. See the SYNC comment above
# for the incident this guards against.
for i in "${!files[@]}"; do
    file="${files[$i]}"
    echo "Running $file"

    cmd=(kwil-cli exec-sql --file "$file" --private-key "$PRIVATE_KEY" --provider "$PROVIDER")

    if [ "$SYNC" = "true" ]; then
        cmd+=(--sync)
    fi

    if ! run_with_retry "${cmd[@]}"; then
        exit 1
    fi
done

# After all migrations are run, grant the network_writers_manager role to the ADMIN_WALLET if provided.
# This allows environment-specific wallet assignments without hardcoding them in SQL migrations.
if [ -n "$ADMIN_WALLET" ]; then
    # Convert the wallet address to lowercase
    local_admin_wallet=$(echo "$ADMIN_WALLET" | tr '[:upper:]' '[:lower:]')

    echo "Granting network_writers_manager role to $local_admin_wallet"
    if ! run_with_retry kwil-cli exec-sql \
        "INSERT INTO role_members (owner, role_name, wallet, granted_at, granted_by) VALUES ('system', 'network_writers_manager', \$wallet, 0, 'system') ON CONFLICT (owner, role_name, wallet) DO NOTHING;" \
        --param wallet:text="$local_admin_wallet" \
        --private-key "$PRIVATE_KEY" \
        --provider "$PROVIDER" --sync; then
        exit 1
    fi
fi