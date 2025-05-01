FROM alpine:latest

# install curl
RUN apk add --no-cache curl

# Set working directory inside the container
WORKDIR /app

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
