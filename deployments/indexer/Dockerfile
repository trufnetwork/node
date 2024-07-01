FROM alpine:latest

# Set working directory inside the container
WORKDIR /app

# Copy the binary file into the container
COPY kwil-indexer /app/kwil-indexer

# Make the binary executable (if needed)
RUN chmod +x /app/kwil-indexer

# Expose port 1337
EXPOSE 1337

# Command to run the binary when the container starts
CMD ["/app/kwil-indexer", "run", \
     "--cometbft-endpoint", "http://tsn-db:26657", \
     "--pg-conn", "postgresql://postgres@kwil-postgres:5432/postgres?sslmode=disable", \
     "--kwil-pg-conn", "postgresql://kwild@kwil-postgres:5432/kwild?sslmode=disable"]