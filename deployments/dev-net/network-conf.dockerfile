# This file is used to generate the configuration files for any test network
# it's the easiest way to generate the configuration files in any environment
# being this an image, we can share the volume between containers
FROM golang:1.22.1-alpine3.19 AS build-kwil

WORKDIR /app

# copy download the kwil binaries to container
COPY ./scripts/download-binaries.sh ./scripts/download-binaries.sh
RUN chmod +x ./scripts/download-binaries.sh
#download kwil binaries to extract kwil-admin
RUN sh ./scripts/download-binaries.sh

FROM busybox:1.35.0-uclibc as busybox

WORKDIR /app

# mandatory arguments
ARG NUMBER_OF_NODES
RUN test -n "$NUMBER_OF_NODES"

ARG CONFIG_PATH
RUN test -n "$CONFIG_PATH"

# Copy the kwil-admin binary from the pre-built stage in Dockerfile
COPY --from=build-kwil /app/.build/kwild /app/kwild
RUN chmod +x /app/kwild

RUN ./kwild setup testnet -v $NUMBER_OF_NODES -o $CONFIG_PATH