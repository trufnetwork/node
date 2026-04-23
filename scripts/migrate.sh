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

# as it's a very important script, let's make sure that our environment is set up correctly
check_env_var "PRIVATE_KEY"
check_env_var "PROVIDER"

# Set SYNC to true by default if not set
SYNC="${SYNC:-true}"

# Retry knobs — every kwil-cli call goes through run_with_retry so a
# transient blip (DNS hiccup, dial timeout, leader rotation) doesn't abort
# a multi-file migration midway. Override via env for flaky networks.
MAX_ATTEMPTS="${MAX_ATTEMPTS:-5}"
RETRY_INITIAL_SECONDS="${RETRY_INITIAL_SECONDS:-2}"

# run_with_retry runs "$@" until it succeeds or MAX_ATTEMPTS is reached.
# Any non-zero exit triggers a retry; broken SQL still aborts once the
# attempts are exhausted (default budget ~30s of backoff).
run_with_retry() {
    local attempt=1
    local delay=$RETRY_INITIAL_SECONDS
    while true; do
        if "$@"; then
            return 0
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

# Get all .sql files in ./internal/migrations folder, skipping *.prod.sql.
# *.prod.sql files are manual-apply mainnet overrides — the embedded
# migration loader (internal/migrations/migration.go) skips them, and so
# does this script to keep its behavior aligned. Apply them by hand AFTER
# this script via `kwil-cli exec-sql --file <path> --sync`.
files=()
for f in ./internal/migrations/*.sql; do
    [[ "$f" == *.prod.sql ]] && continue
    files+=("$f")
done
num_files=${#files[@]}

# Run them with kwil-cli exec-sql
for i in "${!files[@]}"; do
    file="${files[$i]}"
    echo "Running $file"

    cmd=(kwil-cli exec-sql --file "$file" --private-key "$PRIVATE_KEY" --provider "$PROVIDER")

    # Add --sync only for the last file if SYNC is true
    if [ "$SYNC" = "true" ] && [ $((i + 1)) -eq "$num_files" ]; then
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