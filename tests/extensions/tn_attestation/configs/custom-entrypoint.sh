#!/bin/bash
set -e

echo "Starting Kwild with attestation test configuration..."

# Ensure kwild directory exists
mkdir -p /root/.kwild

# Generate or use fixed validator key for deterministic testing
# This ensures the validator can sign attestations
if [ ! -f /root/.kwild/private_key ]; then
    echo "Generating validator private key..."
    # Use a fixed key for deterministic testing (same as deployer)
    echo "0000000000000000000000000000000000000000000000000000000000000001" > /root/.kwild/private_key
fi

# Start kwild
echo "Starting kwild node..."
exec kwild --root-dir=/root/.kwild

