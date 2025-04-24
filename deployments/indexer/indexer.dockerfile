FROM alpine:latest AS build-indexer

WORKDIR /app

RUN apk update && apk add --no-cache wget

# Copy and run the binary download script to fetch kwil-indexer
COPY ./scripts/download-binaries-dev.sh ./scripts/download-binaries-dev.sh
RUN chmod +x ./scripts/download-binaries-dev.sh && \
    sh ./scripts/download-binaries-dev.sh --indexer

FROM alpine:latest AS runtime

RUN apk add --no-cache curl bash

WORKDIR /app

# Copy the downloaded kwil-indexer from the build stage
COPY --from=build-indexer /app/.build/kwil-indexer ./kwil-indexer
RUN chmod +x ./kwil-indexer

# set default env variables
ENV NODE_RPC_ENDPOINT="http://127.0.0.1:8484"
ENV INDEXER_PG_CONN="postgresql://postgres:password@localhost:5432/postgres?sslmode=disable"
ENV LISTEN_ADDRESS=":1337"
ENV LOG_LEVEL="info"
ENV MAX_BLOCK_PAGINATION=50
ENV MAX_TX_PAGINATION=50
ENV POLL_FREQUENCY=30
ENV SEEDS=""
ENV SEED_DIR="~/.kwil-indexer"

# Create entrypoint script
RUN echo '#!/bin/sh' > entrypoint.sh && \
    echo '/app/kwil-indexer run --rpc-endpoint "$NODE_RPC_ENDPOINT" --pg-conn "$INDEXER_PG_CONN" --listen-address "$LISTEN_ADDRESS" --log-level "$LOG_LEVEL" --max-block-pagination "$MAX_BLOCK_PAGINATION" --max-tx-pagination "$MAX_TX_PAGINATION" --poll-frequency "$POLL_FREQUENCY" --seeds "$SEEDS" --seed-dir "$SEED_DIR"' >> entrypoint.sh && \
    chmod +x entrypoint.sh

EXPOSE 1337

ENTRYPOINT ["/app/entrypoint.sh"]
