#!/bin/sh

# Run the configuration script
/app/config.sh

if [ -z "$CONFIG_PATH" ]; then
    echo "No config path set, using default"
    CONFIG_PATH="/root/.kwild"
else
    echo "Config path set to $CONFIG_PATH"
fi

# Execute kwild with extension configuration after the double dash
exec /app/kwild start --root $CONFIG_PATH \
       --db.read-timeout "60s" \
       --snapshots.enable \
       -- \
       --extensions.tn_cache.enabled "true" \
       --extensions.tn_cache.max_block_age "0" \
       --extensions.tn_cache.resolution_schedule "0 0 * * *" \
       --extensions.tn_cache.streams_inline '[{"data_provider":"0x7e5f4552091a69125d5dfcb7b8c2659029395bdf","stream_id":"stcomposed1234567890123456789001","cron_schedule":"*/1 * * * *","from":1609459200},{"data_provider":"0x7e5f4552091a69125d5dfcb7b8c2659029395bdf","stream_id":"stcomposed1234567890123456789002","cron_schedule":"*/2 * * * *","from":1609459200},{"data_provider":"0x7e5f4552091a69125d5dfcb7b8c2659029395bdf","stream_id":"stcomposed1234567890123456789003","cron_schedule":"*/3 * * * *","from":1609459200}]'