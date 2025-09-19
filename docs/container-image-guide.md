# TN Node Container Quickstart

Run the TRUF.NETWORK node container with Docker Compose while keeping the standard operator layout for data and configuration.

## Prerequisites
- Docker Engine 24+ with the `docker compose` plugin.
- Pull access to `ghcr.io/trufnetwork/tn-db` and `kwildb/postgres`.
- Optional: the [`kwild` CLI](https://github.com/trufnetwork/node/releases) if you want to pre-populate configuration files instead of using the container’s auto-initialisation path.

## 1. Prepare the workspace
```bash
mkdir -p ~/truf-node/tn_data
cd ~/truf-node
```
The `tn_data` directory becomes the bind mount that stores keys, logs, and the blockstore on the host so they persist across container rebuilds.

## 2. Save the Compose stack
Create `docker-compose.yml` in `~/truf-node` using the minimal stack below.

<details>
<summary>Minimal docker-compose.yml</summary>

```yaml
services:
  postgres:
    image: kwildb/postgres:16.8-1
    restart: unless-stopped
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    volumes:
      # this will create a volume called pg-data in the current directory
      - ./pg-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  tn-node:
    image: ghcr.io/trufnetwork/tn-db:${TN_NODE_TAG:-latest}
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      KWILD_DB_HOST: postgres
      KWILD_DB_PORT: 5432
      SETUP_CHAIN_ID: ${SETUP_CHAIN_ID:-truflation-dev}
      SETUP_DB_OWNER: ${SETUP_DB_OWNER:-}
    volumes:
      # this will create a volume called tn_data in the current directory, or reuse your pre-generated config if it exists
      - ./tn_data:/root/.kwild
    ports:
      - "8484:8484"   # JSON-RPC
      - "6600:6600" # P2P
```

</details>

Compose automatically loads variables from a `.env` file in the same directory. At a minimum set the node image tag you want to run (for example the latest release) and optionally pre-fill auto-setup values:

```dotenv
TN_NODE_TAG=v0.15.0
SETUP_CHAIN_ID=truflation-dev
# Leave empty to let the container derive the owner from the generated node key
SETUP_DB_OWNER=
```

## 3. Launch the services
```bash
docker compose up -d
docker compose ps
```
The node waits for Postgres to pass its health check before it starts. Use the bind mount (`~/truf-node/tn_data`) to inspect the generated config, keys, and logs.

### Verify the node
```bash
curl http://localhost:8484/api/v1/health
docker compose logs -f tn-node
```

## Optional: Pre-populate the configuration
You can generate `config.toml`, `genesis.json`, and keys ahead of time so the container skips auto-initialisation.

1. Clone the operator repository (once):
   ```bash
   git clone https://github.com/trufnetwork/truf-node-operator.git ~/truf-node-operator
   ```
2. Install or download the matching `kwild` release binary and add it to your `PATH`.
3. Generate the config with the official genesis (adjust flags as needed for your environment, just like the [Node Operator Guide](node-operator-guide.md)):
   ```bash
   kwild setup init \
     --genesis ~/truf-node-operator/configs/network/v2/genesis.json \
     --root    ~/truf-node/tn_data \
     --p2p.bootnodes "4e0b5c952be7f26698dc1898ff3696ac30e990f25891aeaf88b0285eab4663e1#ed25519@node-1.mainnet.truf.network:26656,0c830b69790eaa09315826403c2008edc65b5c7132be9d4b7b4da825c2a166ae#ed25519@node-2.mainnet.truf.network:26656" \
     --state-sync.enable \
     --state-sync.trusted-providers "0c830b69790eaa09315826403c2008edc65b5c7132be9d4b7b4da825c2a166ae#ed25519@node-2.mainnet.truf.network:26656" \
     --rpc.private
   ```
4. Update the generated database host so it matches the Compose service name. Open `~/truf-node/tn_data/config.toml` and set:
   ```toml
   [db]
   host = 'postgres'
   ```
   (This ensures the node reaches the Postgres container without relying on environment overrides.)

The next `docker compose up -d` start reuses this configuration instead of triggering auto-setup.

### Relying on auto-generation
For throwaway networks you can skip the steps above. Leave `SETUP_DB_OWNER` blank to let the container derive an identifier from the generated node key. Override `SETUP_CHAIN_ID` and `SETUP_DB_OWNER` in `.env` only when you want deterministic values for the generated genesis.

## Maintenance tips
- Back up the `~/truf-node/tn_data` directory; it contains the node identity, admin certificates, and blockstore.
- To rotate configuration, stop the stack, edit files under `~/truf-node/tn_data`, then start the stack again. The entrypoint leaves existing files untouched.
- Testing a new image? Set `TN_NODE_TAG` to the candidate tag and point the compose stack at a fresh host directory so you can diff auto-generated config before promoting it to production.
