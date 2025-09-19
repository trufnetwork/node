FROM golang:1.24.3-alpine3.21 AS build

WORKDIR /app
COPY go.mod go.sum ./

RUN go mod download
RUN go mod verify

# we only need app, cmd, and internal dir contents
# then test modifications won't affect the cached image
COPY app/ ./app/
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY extensions/ ./extensions/
COPY deployments/tn-entrypoint.sh ./deployments/tn-entrypoint.sh

# todo: incorporate task build process, otherwise images will lack information about the build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/.build/kwild /app/cmd/kwild/main.go

FROM busybox:1.35.0-uclibc as busybox
# busy box will provide us with a shell to run commands in distroless

FROM alpine:latest

ENV SETUP_CHAIN_ID=truflation-dev
ENV SETUP_DB_OWNER=
# Provide SETUP_DB_OWNER at runtime to override the derived owner; otherwise kwild derives one from the generated node key
ENV CONFIG_PATH=/root/.kwild

WORKDIR /app

# add postgres client tools
RUN apk add --no-cache postgresql16-client

# move .build content to /app
COPY --from=build /app/.build/* /app/

# move the entrypoint script to /app
COPY --from=build /app/deployments/tn-entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

# create the configuration script; note that we are NOT running it here
RUN echo "#!/bin/sh" > /app/config.sh && \
    echo "set -xe" >> /app/config.sh && \
    echo "if [ ! -f $CONFIG_PATH/config.toml ]; then" >> /app/config.sh && \
    echo "    echo 'Configuration does not exist';" >> /app/config.sh && \
    echo "    echo 'Creating configuration';" >> /app/config.sh && \
    echo "    ./kwild setup init --chain-id \"\$SETUP_CHAIN_ID\" --db-owner \"\$SETUP_DB_OWNER\" -r '/root/.kwil-new';" >> /app/config.sh && \
    echo "    mkdir -p $CONFIG_PATH;" >> /app/config.sh && \
    echo "    cp /root/.kwil-new/* $CONFIG_PATH;" >> /app/config.sh && \
    echo "    rm -rf /root/.kwil-new;" >> /app/config.sh && \
    echo "    echo 'Configuration created';" >> /app/config.sh && \
    echo "else" >> /app/config.sh && \
    echo "    echo 'Configuration already exists';" >> /app/config.sh && \
    echo "fi" >> /app/config.sh

RUN chmod +x /app/config.sh

EXPOSE 50051 50151 8080 8484 26656 26657
ENTRYPOINT /app/entrypoint.sh
