#!/bin/sh

# other configuration that are dynamic for each node should be set using env variables as specified here:
# https://docs.kwil.com/docs/daemon/config/settings#config-override
# remember: flags > env variables > config.toml > defaults

exec /app/kwild --root-dir $CONFIG_PATH \
       --app.http-listen-addr "0.0.0.0:8080"\
       --app.jsonrpc-listen-addr "0.0.0.0:8484"\
       --app.db-read-timeout "60s"\
       --app.snapshots.enabled\
       --chain.p2p.listen-addr "tcp://0.0.0.0:26656"\
       --chain.rpc.listen-addr "tcp://0.0.0.0:26657"
