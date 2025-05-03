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

# Get all .sql files in ./internal/migrations folder and run them with kwil-cli exec-sql --file /path/to/file.sql
for file in ./internal/migrations/*.sql; do
    echo "Running $file"
    kwil-cli exec-sql --file $file --sync --private-key "$PRIVATE_KEY" --provider "$PROVIDER"
done