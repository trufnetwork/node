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

# Get all .sql files in ./internal/migrations folder
files=(./internal/migrations/*.sql)
num_files=${#files[@]}

# Run them with kwil-cli exec-sql
for i in "${!files[@]}"; do
    file="${files[$i]}"
    echo "Running $file"
        
    cmd="kwil-cli exec-sql --file $file --private-key \"$PRIVATE_KEY\" --provider \"$PROVIDER\""

    # Add --sync only for the last file if SYNC is true
    if [ "$SYNC" = "true" ] && [ $((i + 1)) -eq "$num_files" ]; then
        cmd="$cmd --sync"
    fi
    
    eval $cmd
done

# After all migrations are run, grant the network_writers_manager role to the ADMIN_WALLET if provided.
# This allows environment-specific wallet assignments without hardcoding them in SQL migrations.
if [ -n "$ADMIN_WALLET" ]; then
    # Convert the wallet address to lowercase
    local_admin_wallet=$(echo "$ADMIN_WALLET" | tr '[:upper:]' '[:lower:]')

    echo "Granting network_writers_manager role to $local_admin_wallet"
    kwil-cli exec-sql \
        "INSERT INTO role_members (owner, role_name, wallet, granted_at, granted_by) VALUES ('system', 'network_writers_manager', \$wallet, 0, 'system') ON CONFLICT (owner, role_name, wallet) DO NOTHING;" \
        --param wallet:text="$local_admin_wallet" \
        --private-key "$PRIVATE_KEY" \
        --provider "$PROVIDER" --sync
fi