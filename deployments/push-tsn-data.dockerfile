FROM golang:1.22.1-alpine3.19 AS build

RUN mkdir /firstrun

WORKDIR /app

# install dependencies
RUN apk add --no-cache bash uuidgen python3~3.11 py3-pip~23 py3-pandas~2

# required for -P option, otherwise some scripts (e.g. retrying on nonce error) may fail
RUN apk add --upgrade grep

# download kwil-cli
RUN wget -O kwil-db.tar.gz https://github.com/kwilteam/kwil-db/releases/download/v0.7.0/kwil-db_0.7.0_linux_amd64.tar.gz\
      && tar -xzvf kwil-db.tar.gz 'kwil-cli'\
      && mkdir -p ./.build\
      && mv ./kwil-cli .build\
      && rm ./kwil-db.tar.gz


COPY go.mod .
COPY go.sum .

COPY ./go.mod ./go.sum ./
COPY ./scripts/ ./scripts/

COPY ./internal/schemas/ ./internal/schemas/

RUN go mod download
RUN go mod verify

ENV GRPC_URL="http://tsn-db:8080"

RUN echo -e "#!/bin/sh\n\
\n\
set -e\n\
CONTAINER_ALREADY_STARTED=\"/firstrun/CONTAINER_ALREADY_STARTED_PLACEHOLDER\"\n\
if [ ! -e \$CONTAINER_ALREADY_STARTED ]; then\n\
    echo \"-- First container startup. Let's add data --\"\n\
    /app/scripts/setup.sh\n\
    touch \$CONTAINER_ALREADY_STARTED\n\
else\n\
    echo \"-- Not first container startup. Let's just make sure it's working --\"\n\
    /app/scripts/wait_kwild.sh\n\
fi" > ./start-in-docker.sh

RUN chmod +x ./start-in-docker.sh

CMD ["./start-in-docker.sh"]