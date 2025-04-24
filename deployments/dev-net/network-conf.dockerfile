# This file is used to generate the configuration files for any test network
# it's the easiest way to generate the configuration files in any environment
# being this an image, we can share the volume between containers
FROM golang:1.23.7-alpine3.21 AS build-kwild

WORKDIR /app

# fetch dependencies and build kwild from source
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/.build/kwild /app/cmd/kwild/main.go

FROM busybox:1.35.0-uclibc as busybox

WORKDIR /app

# mandatory arguments
ARG NUMBER_OF_NODES
RUN test -n "$NUMBER_OF_NODES"

ARG CONFIG_PATH
RUN test -n "$CONFIG_PATH"

ARG HOSTNAMES
RUN test -n "$HOSTNAMES"

# allow overriding chain and DB owner
ARG CHAIN_ID=${CHAIN_ID:-trufnetwork-dev}

# Copy the kwild binary from the pre-built stage
COPY --from=build-kwild /app/.build/kwild /app/kwild
RUN chmod +x /app/kwild

# DB OWNER will not be provided to configuration setup

RUN ./kwild setup testnet --chain-id $CHAIN_ID --hostnames $HOSTNAMES -v $NUMBER_OF_NODES -o $CONFIG_PATH

# --consensus.empty-block-timeout 60s by default
ARG KWILD_CONSENSUS_EMPTY_BLOCK_TIMEOUT=60s

# Update each config.toml in-place with the new timeout via sed
RUN find "$CONFIG_PATH" -name 'config.toml' -type f -print0 | \
    xargs -0 -I{} sh -c \
      "sed -i \"s/^empty_block_timeout[[:space:]]*=[[:space:]]*'.*'/empty_block_timeout = '${KWILD_CONSENSUS_EMPTY_BLOCK_TIMEOUT}'/\" \"{}\""
