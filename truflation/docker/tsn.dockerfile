FROM golang:alpine AS build

# required for scripts
RUN apk add --no-cache bash uuidgen python3~3.11 py3-pip~23 py3-pandas~2

ARG version
ARG build_time
ARG git_commit
ARG go_build_tags

WORKDIR /app
RUN mkdir -p /var/run/kwil
RUN chmod 777 /var/run/kwil
RUN apk update && apk add git ca-certificates-bundle

COPY . .
RUN test -f go.work && rm go.work || true

RUN GOWORK=off GIT_VERSION=$version GIT_COMMIT=$git_commit BUILD_TIME=$build_time CGO_ENABLED=0 TARGET="/app/.build" GO_BUILDTAGS=$go_build_tags ./scripts/build/binary kwild
RUN GIT_VERSION=$version GIT_COMMIT=$git_commit BUILD_TIME=$build_time CGO_ENABLED=0 TARGET="/app/.build" ./scripts/build/binary kwil-admin
RUN GIT_VERSION=$version GIT_COMMIT=$git_commit BUILD_TIME=$build_time CGO_ENABLED=0 TARGET="/app/.build" ./scripts/build/binary kwil-cli
RUN chmod +x /app/.build/kwild /app/.build/kwil-admin /app/.build/kwil-cli

# owner public address for this PK is 7e5f4552091a69125d5dfcb7b8c2659029395bdf
ENV PRIVATE_KEY="0000000000000000000000000000000000000000000000000000000000000001"

RUN /app/truflation/scripts/setup.sh

FROM alpine:3.17

WORKDIR /app
RUN mkdir -p /var/run/kwil && chmod 777 /var/run/kwil
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/.build/kwild ./kwild
COPY --from=build /app/.build/kwil-admin ./kwil-admin

# copy the produced data
COPY --from=build /root/.kwild /root/.kwild
COPY --from=build /app/truflation/docker/config.toml /root/.kwild/config.toml

EXPOSE 50051 50151 8080 26656 26657
ENTRYPOINT ["/app/kwild"]
