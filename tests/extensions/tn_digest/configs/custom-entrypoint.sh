#!/bin/sh
set -e

echo "Starting custom entrypoint for tn_digest E2E test..."

# Initialize configuration if it doesn't exist
if [ ! -f /root/.kwild/config.toml ]; then
    echo "Initializing kwild configuration..."
    ./kwild setup init --chain-id ${CHAIN_ID:-tn-digest-test} --db-owner ${DB_OWNER} -r /root/.kwild/
    echo "Configuration initialized successfully"
    
    # Add tn_digest extension configuration with fast reload for testing
    echo "" >> /root/.kwild/config.toml
    echo "[extensions.tn_digest]" >> /root/.kwild/config.toml
    echo "reload_interval_blocks = \"1\"" >> /root/.kwild/config.toml
    echo "Added tn_digest extension configuration with 1-block reload interval"
else
    echo "Configuration already exists"
fi

echo "Configuration ready. Starting kwild..."

# Execute the main application
exec ./kwild start --root /root/.kwild