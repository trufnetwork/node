#!/bin/sh

# Check and set CORS_ARGS based on CORS_ALLOWED_ORIGINS
if [ -z "$CORS_ALLOWED_ORIGINS" ]; then
    CORS_ARGS="--cors-allow-origins $CORS_ALLOWED_ORIGINS"
else
    CORS_ARGS=""
fi

# we know that curl here should return:
# {"chain_id":"kwil-chain-qG6KXYD3", "height":"782", "hash":"ee1ee0964b76f79b48652b22a253b20bd72cd45cecb441e08ba2dee7aa845cac"}
export CHAIN_ID=$(curl http://tsn-db:8080/api/v1/chain_info -s | jq -r '.chain_id')

# Append CORS_ARGS to the passed command if not empty
if [ -n "$CORS_ARGS" ]; then
    set -- "$@" $CORS_ARGS
fi

exec "$@"