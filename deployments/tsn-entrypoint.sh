#!/bin/sh

# other configuration that are dynamic for each node should be set using env variables as specified here:
# https://docs.kwil.com/docs/daemon/config/settings#config-override
# remember: flags > env variables > config.toml > defaults

# TODO: remove the --autogen flag when we can generate the proper config.toml file
# TODO: see tsn-config.dockerfile comments for more information
exec /app/kwild start --autogen --root $CONFIG_PATH \
       --db.read-timeout "60s"\
       --snapshots.enable