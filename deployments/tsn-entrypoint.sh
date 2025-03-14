#!/bin/sh

# other configuration that are dynamic for each node should be set using env variables as specified here:
# https://docs.kwil.com/docs/daemon/config/settings#config-override
# remember: flags > env variables > config.toml > defaults

# Run the configuration script
/app/config.sh

if [ -z "$CONFIG_PATH" ]; then
    echo "No config path set, using default"
    CONFIG_PATH="/root/.kwild"
else
    echo "Config path set to $CONFIG_PATH"
fi

exec /app/kwild start --root $CONFIG_PATH \
       --db.read-timeout "60s"\
       --snapshots.enable