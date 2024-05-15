# This file is used to generate the configuration files for any test network
# it's the easiest way to generate the configuration files in any environment
# being this an image, we can share the volume between containers
FROM busybox:1.35.0-uclibc as busybox

WORKDIR /app

# mandatory arguments
ARG CHAIN_ID
RUN test -n "$CHAIN_ID"

ARG NUMBER_OF_NODES
RUN test -n "$NUMBER_OF_NODES"

ARG CONFIG_PATH
RUN test -n "$CONFIG_PATH"

ARG HOSTNAMES
RUN test -n "$HOSTNAMES"

COPY ./.build/kwil-admin /app/kwil-admin

RUN ./kwil-admin setup testnet -v $NUMBER_OF_NODES --chain-id $CHAIN_ID -o $CONFIG_PATH --hostnames $HOSTNAMES