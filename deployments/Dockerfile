FROM golang:1.22.1-alpine3.19 AS build

WORKDIR /app
COPY go.mod go.sum ./

RUN go mod download
RUN go mod verify

COPY ./scripts/download-binaries.sh ./scripts/download-binaries.sh
RUN chmod +x ./scripts/download-binaries.sh
# download the kwil binaries
RUN sh ./scripts/download-binaries.sh

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/.build/kwild /app/cmd/kwild/main.go
# owner public address for this PK is 7e5f4552091a69125d5dfcb7b8c2659029395bdf

FROM busybox:1.35.0-uclibc as busybox
# busy box will provide us with a shell to run commands in distroless

FROM gcr.io/distroless/static-debian12

# make every needed tool available here to simplify development and
# debugging
COPY --from=busybox /bin/* /bin/

WORKDIR /app

# move .build content to /app
COPY --from=build /app/.build/* /app/

# create entrypoint file
RUN echo "#!/bin/sh" > /app/entrypoint.sh
RUN echo "set -e" >> /app/entrypoint.sh
RUN echo "set -x" >> /app/entrypoint.sh
# test if CONFIG_PATH and DB_HOST are provided at runtime
RUN echo "test -n \"\$CONFIG_PATH\"" >> /app/entrypoint.sh
RUN echo "test -n \"\$DB_HOST\"" >> /app/entrypoint.sh
RUN echo "exec /app/kwild --root-dir \$CONFIG_PATH \
  --app.hostname \"\$HOSTNAME\"\
  --app.http-listen-addr \"0.0.0.0:8080\"\
  --app.jsonrpc-listen-addr \"0.0.0.0:8484\"\
  --chain.p2p.listen-addr \"tcp://0.0.0.0:26656\"\
  --app.pg-db-host \$DB_HOST \\" >> /app/entrypoint.sh

# TODO when https://github.com/kwilteam/kwil-db/pull/706 is published, we can remove all these workarounds
#   and configure from the docker-compose file instead

# NOTE:
# we're hardcoding chain.p2p.listen-addr to override any value provided in the config file
# this bypasses the need to change kwil-admin command that generates network config
# this becomes needed as the current behavior is to setup incremental p2p ports for each node, while we
# want this to be fixed at 26656 for all nodes in the network

# P2P config
# KWILD_CHAIN_P2P_PERSISTENT_PEERS and KWILD_CHAIN_P2P_EXTERNAL_ADDRESS env directly

# if PERSISTENT_PEERS is provided, use with --chain.p2p.persistent-peers
RUN if [ -n "\$PERSISTENT_PEERS" ];\
    then \
    echo "  --chain.p2p.persistent-peers \"\$PERSISTENT_PEERS\" \\" >> /app/entrypoint.sh;\
    fi

# if EXTERNAL_ADDRESS is provided, use with --chain.p2p.external-address
RUN if [ -n "\$EXTERNAL_ADDRESS" ];\
    then \
    echo "  --chain.p2p.external-address \"\$EXTERNAL_ADDRESS\" \\" >> /app/entrypoint.sh;\
    fi

# close entrypoint file
RUN echo "" >> /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh


EXPOSE 50051 50151 8080 8484 26656 26657
ENTRYPOINT ["/app/entrypoint.sh"]
