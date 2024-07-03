FROM alpine:latest

# Set working directory inside the container
WORKDIR /app

# Copy the binary file into the container
COPY kwil-indexer /app/kwil-indexer

# Make the binary executable (if needed)
RUN chmod +x /app/kwil-indexer

# Define environment variables
# example: http://tsn-db:26657
ARG NODE_COMETBFT_ENDPOINT
# example: postgresql://kwild@kwil-postgres:5432/kwild?sslmode=disable
ARG KWIL_PG_CONN
# example: postgresql://postgres@kwil-postgres:5432/postgres?sslmode=disable
ARG INDEXER_PG_CONN
# example: nodeID1@IP:port,nodeID2@IP:Port
ARG SEEDS

ENV COMETBFT_ENDPOINT=$NODE_COMETBFT_ENDPOINT
ENV KWIL_PG_CONN=$KWIL_PG_CONN
ENV INDEXER_PG_CONN=$INDEXER_PG_CONN

# Create entrypoint script
RUN echo '#!/bin/sh' > /app/entrypoint.sh
RUN echo '/app/kwil-indexer run \
  --cometbft-endpoint "$COMETBFT_ENDPOINT" \
  --pg-conn "$INDEXER_PG_CONN" \
  --kwil-pg-conn "$KWIL_PG_CONN"' >> /app/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

EXPOSE 1337

# Command to run the binary when the container starts
ENTRYPOINT ["/app/entrypoint.sh"]
