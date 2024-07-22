FROM alpine:latest

# install curl
RUN apk add --no-cache curl

# Set working directory inside the container
WORKDIR /app

# Copy the binary file into the container
COPY kwil-indexer /app/kwil-indexer

# Make the binary executable (if needed)
RUN chmod +x /app/kwil-indexer

# Create entrypoint script
RUN echo '#!/bin/sh' > /app/entrypoint.sh
RUN echo '/app/kwil-indexer run \
  --cometbft-endpoint "$NODE_COMETBFT_ENDPOINT" \
  --pg-conn "$INDEXER_PG_CONN" \
  --kwil-pg-conn "$KWIL_PG_CONN"' >> /app/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

EXPOSE 1337

# Command to run the binary when the container starts
ENTRYPOINT ["/app/entrypoint.sh"]
