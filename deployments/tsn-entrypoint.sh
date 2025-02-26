#!/bin/sh

# other configuration that are dynamic for each node should be set using env variables as specified here:
# https://docs.kwil.com/docs/daemon/config/settings#config-override
# remember: flags > env variables > config.toml > defaults

# Run the configuration script
/app/config.sh

exec /app/kwild start --root $CONFIG_PATH \
       --db.read-timeout "60s"\
       --snapshots.enable