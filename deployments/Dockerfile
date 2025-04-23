FROM golang:1.23.7-alpine3.21 AS build

WORKDIR /app
COPY go.mod go.sum ./

RUN go mod download
RUN go mod verify

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/.build/kwild /app/cmd/kwild/main.go

FROM busybox:1.35.0-uclibc as busybox
# busy box will provide us with a shell to run commands in distroless

FROM alpine:latest

ARG CHAIN_ID=truflation-dev

# DB_OWNER will be provided as an environment variable at runtime

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
    echo "set -e" >> /app/config.sh && \
    echo "if [ ! -f /root/.kwild/nodekey.json ]; then" >> /app/config.sh && \
    echo "    echo 'Configuration does not exist';" >> /app/config.sh && \
    echo "    echo 'Creating configuration';" >> /app/config.sh && \
    echo "    ./kwild setup init --chain-id $CHAIN_ID --db-owner \$DB_OWNER -r /root/.kwil-new;" >> /app/config.sh && \
    echo "    mkdir -p /root/.kwild;" >> /app/config.sh && \
    echo "    cp /root/.kwil-new/* /root/.kwild;" >> /app/config.sh && \
    echo "    rm -rf /root/.kwil-new;" >> /app/config.sh && \
    echo "    echo 'Configuration created';" >> /app/config.sh && \
    echo "else" >> /app/config.sh && \
    echo "    echo 'Configuration already exists';" >> /app/config.sh && \
    echo "fi" >> /app/config.sh

RUN chmod +x /app/config.sh

EXPOSE 50051 50151 8080 8484 26656 26657
ENTRYPOINT /app/entrypoint.sh
