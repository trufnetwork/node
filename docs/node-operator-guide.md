# Node Operator Guide

This guide will walk you through the process of setting up and running a TRUF.NETWORK (TN) node. By following these steps, you'll be able to deploy a node, optionally become a validator, and contribute to the TN.

This guide provides instructions for both Debian-based Linux (e.g., Ubuntu) and macOS. Please follow the steps corresponding to your operating system.

## Prerequisites

Before you begin, ensure you have the following installed.

### 1. Docker & Docker Compose

#### For Linux (Ubuntu/Debian)

These commands will install Docker and the Docker Compose plugin.

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release build-essential
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

- After installation, enable and start Docker:

```bash
sudo systemctl enable docker
sudo systemctl start docker
```

- Add your user to the docker group to run Docker commands without sudo:

```bash
sudo usermod -aG docker $USER
newgrp docker
```

#### For macOS

1.  **Download and Install Docker Desktop**: Docker Desktop for Mac includes Docker Engine, the Docker CLI client, Docker Compose, and all other necessary components.
    - Download it from the official Docker website: [**Docker Desktop for Mac**](https://www.docker.com/products/docker-desktop)
2.  **Start Docker Desktop**: Launch the Docker Desktop application from your Applications folder. You should see a whale icon in your Mac's menu bar, indicating that Docker is running. No further setup is needed.

### 2. PostgreSQL Client

The PostgreSQL client (`psql`) is required for database operations, and the `pg_dump` utility, which is included in this installation, is required for state sync.

#### For Linux (Ubuntu/Debian)

```bash
sudo apt-get install -y postgresql-client-16
```

#### For macOS

You can install the PostgreSQL client using [Homebrew](https://brew.sh/). If you don't have Homebrew, install it first by following the instructions on their website.

```bash
brew install postgresql@16
```

To use it from any terminal, you may need to add it to your `PATH`. For `zsh` (the default in modern macOS):

```bash
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

> **Note**: If you use Bash, add the export line to `~/.bash_profile` instead. The path may also be `/usr/local/opt/postgresql@16/bin` on Intel-based Macs.

### 3. Go

#### For Linux (Ubuntu/Debian)

```bash
LATEST_GO_VERSION=$(curl -sSL https://go.dev/VERSION?m=text | head -n1)
echo "Installing ${LATEST_GO_VERSION}..."
curl -fsSL "https://go.dev/dl/${LATEST_GO_VERSION}.linux-amd64.tar.gz" \
  -o "${LATEST_GO_VERSION}.linux-amd64.tar.gz"
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf "${LATEST_GO_VERSION}.linux-amd64.tar.gz"
rm "${LATEST_GO_VERSION}.linux-amd64.tar.gz"
```

- After installation, add Go to your `PATH`:

```bash
grep -qxF 'export GOPATH=$HOME/go' ~/.bashrc \
  || echo 'export GOPATH=$HOME/go' >> ~/.bashrc
grep -qxF 'export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin' ~/.bashrc \
  || echo 'export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin' >> ~/.bashrc
```

- Reload your terminal:

```bash
source ~/.bashrc
```

#### For macOS

The simplest way to install Go on macOS is with Homebrew.

```bash
brew install go
```

This will automatically configure the necessary `PATH` environment variables.

### 4. Taskfile (go-task)

This tool is required to build the `kwild` binary from source. The command is the same for both Linux and macOS, and requires Go to be installed.

```bash
go install github.com/go-task/task/v3/cmd/task@latest
```

5. **Kwild Binary**

You have two options to get the `kwild` binary:

1. **Download from Releases**:

- Visit [TRUF.NETWORK Node Releases](https://github.com/trufnetwork/node/releases)
- Download the latest release for your operating system (e.g., `tn_2.0.1_linux_amd64.tar.gz`)
- Extract the binary and move it to your system path:

```bash
tar -xzf tn_2.0.1_linux_amd64.tar.gz
sudo mv kwild /usr/local/bin/
```

2. **Build from Source**:

```bash
git clone https://github.com/trufnetwork/node.git
cd node
task build
```

The built binary will be in the `.build` directory. Move it to your system path:

```bash
sudo mv .build/kwild /usr/local/bin/
```

Apply new docker group:

```bash
newgrp docker
```

## Verify Installation

Before you move forward to Node Setup, verify that everything installed correctly and of the right version:

```bash
docker --version
docker compose version
pg_dump --version
go version
task --version
kwild version
```

## Node setup

### 1. Clone TN Node Operator Repository

From your root directory, clone the TRUF.NETWORK node operator repository:

```bash
cd # Return to home directory if not already there
git clone https://github.com/trufnetwork/truf-node-operator.git
cd truf-node-operator
```

### 2. Generate Initial Configuration

Use `kwild` to create your initial configuration file:

```bash
kwild setup init \
  --genesis ./configs/network/v2/genesis.json \
  --root ./my-node-config \
  --p2p.bootnodes "4e0b5c952be7f26698dc1898ff3696ac30e990f25891aeaf88b0285eab4663e1#ed25519@node-1.mainnet.truf.network:26656,0c830b69790eaa09315826403c2008edc65b5c7132be9d4b7b4da825c2a166ae#ed25519@node-2.mainnet.truf.network:26656"
```

For detailed instructions on configuration options more relevant to a production setup, refer to our [Configuration Guide](https://github.com/trufnetwork/truf-node-operator/blob/main/docs/creating-config.md).

### 3. Enable State Sync

This will configure your node to use state sync for faster synchronization with the network. Edit the `config.toml` file using the appropriate command for your OS.

#### For Linux (Ubuntu/Debian)

```bash
sed -i '/\[state_sync\]/,/^\[/ s/enable = false/enable = true/' ./my-node-config/config.toml
sed -i 's/trusted_providers = \[\]/trusted_providers = ["0c830b69790eaa09315826403c2008edc65b5c7132be9d4b7b4da825c2a166ae#ed25519@node-2.mainnet.truf.network:26656"]/' ./my-node-config/config.toml
```

### 4. Set Up PostgreSQL

For a quick setup, run Kwil's pre-configured PostgreSQL Docker image:

```bash
docker run -d -p 127.0.0.1:5432:5432 --name tn-postgres \
    -e "POSTGRES_HOST_AUTH_METHOD=trust" \
    -v tn-pgdata:/var/lib/postgresql/data \
    --shm-size=1gb \
    kwildb/postgres:latest
```

<Warning>
**Critical Security Requirements:**
1. `kwild` requires a "superuser" role to perform various tasks that require elevated privileges, such as creating triggers and publications for logical replication. The PostgreSQL database cluster should be dedicated to `kwild`, and should not be used for any other purpose.
2. NEVER expose PostgreSQL port (5432) to the public internet. Always bind to localhost (127.0.0.1) as shown in the example above.
</Warning>

<Tip>
**Securing PostgreSQL Port:**
1. Use UFW (Uncomplicated Firewall) to block port 5432:
   ```bash
   sudo ufw deny 5432/tcp
   ```

2. Verify no external access to port 5432:
   ```bash
   sudo ss -tulpn | grep 5432
   # Should only show 127.0.0.1:5432, not 0.0.0.0:5432
   ```
   </Tip>

The command above:

- `-v tn-pgdata:/var/lib/postgresql/data`: Creates a persistent volume named 'tn-pgdata' to store database data
- `--shm-size=1gb`: Allocates 1GB of shared memory for PostgreSQL operations (recommended for better performance)

### 5. Create `systemd` service for `kwild` and PostgreSQL

```bash
# Create systemd service for kwild
sudo tee /etc/systemd/system/kwild.service << EOF
[Unit]
Description=TRUF.NETWORK Node Service
After=network.target tn-postgres.service
Requires=tn-postgres.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$HOME/truf-node-operator
ExecStart=$(which kwild) start -r $HOME/truf-node-operator/my-node-config
Restart=always
RestartSec=10
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
```

> **Note:** The paths in the service file above use `$HOME/truf-node-operator` as the base directory. Make sure to adjust this path according to where you cloned the repository.

```bash
# Create systemd service for PostgreSQL
sudo tee /etc/systemd/system/tn-postgres.service << EOF
[Unit]
Description=TRUF.NETWORK PostgreSQL Service
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=$USER
ExecStart=docker start -a tn-postgres
ExecStop=docker stop tn-postgres
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
```

### 6. Run TN Node

Before you proceed, ensure your firewall allows incoming connections on:

- JSON-RPC port (default: 8484)
- P2P port (default: 6600)

```bash
sudo systemctl daemon-reload
sudo systemctl enable tn-postgres
sudo systemctl enable kwild
sudo systemctl start tn-postgres
sudo systemctl start kwild
```

**Security Warning**: It is recommended to not expose port 5432 publicly in production environments.

### 7. Verify Node Synchronization

To become a validator, ensure your node is fully synced with the network:

Use this command to check node sync status. Look for `syncing: false` in the output, and check that your `best_block_height` is close to the current network height.

```bash
kwild admin status
```

> **Note**: If you see the error `dial unix /tmp/kwild.socket: connect: connection refused`, this is normal during:
>
> - Initial database setup
> - Database restoration
> - State sync operations
>
> The service will become available once these operations complete. You can monitor the progress using:
>
> ```bash
> sudo journalctl -u kwild -f
> ```

### 8. Become a Validator (Optional)

To upgrade your node to a validator:

1. Ensure your node is fully synced with the network.
2. Submit a validator join request:

```bash
kwild validators join
```

3. Wait for approval from existing validators. You can check your join request status with:

```bash
kwild validators list-join-requests
```

Existing validators must approve your request. For each existing validator needed to approve:

```bash
kwild validators approve <your-node-id>
```

The node ID format for validator operations is: `<public key>#<key type>`. For example:

```bash
kwild validators approve 03dbe22b9922b5c0f8f60c230446feaa1c132a93caa9dae83b5d4fab16c3404a22#secp256k1
```

You can find your node's public key and key type by running:

```bash
kwild key info --key-file ./my-node-config/nodekey.json
```

Note: If you used a different directory name during setup (not `./my-node-config`), replace the path with your actual node configuration directory path.

You can always reach out to the community for help with the validator process.

### 9. Submit Your Node to Available Node List (Optional)

To help others discover your node:

1. Fork the TN repository.
2. Add your node information to the `configs/network/available_nodes.json` file.
3. Submit a Pull Request with your changes.

We'll review and merge your PR to include your node in the network's seed list.

## Network Configuration Files

Essential network configuration files are located in the `configs/network/` directory:

- `v2/genesis.json`: The network's genesis file.
- `v2/network-nodes.csv`: List of available nodes for peer discovery.

When setting up your node, refer to these files for network-specific parameters and peer information.

## Node ID Format

Node IDs in TRUF.NETWORK follow the format: `<public key>#<key type>@<IP address>:<port>`

You can find your node ID by running:

```bash
kwild key info --key-file ./my-node-config/nodekey.json
```

## Additional Resources

- [Quick Installation Guide](https://github.com/trufnetwork/truf-node-operator/blob/main/docs/installation-guide.md) - Step-by-step commands for a fresh server setup
- [Kwil Documentation](https://docs.kwil.com)
- [Production Network Deployment Guide](https://docs.kwil.com/docs/node/production)

For further assistance, join our [Discord community](https://discord.com/invite/5AMCBYxfW4) or open an issue on our [GitHub repository](https://github.com/trufnetwork/truf-node-operator/issues).

Welcome to the TRUF.NETWORK! Your participation helps build a more robust and decentralized data infrastructure.

## Troubleshooting

### Installing `pg_dump` for Snapshots

To enable state sync functionality, you'll need `pg_dump` installed. Here's how to install it:

For Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install postgresql-client-16
```

For CentOS/RHEL:

```bash
sudo yum install postgresql16
```

For macOS (using Homebrew):

```bash
brew install postgresql@16
```

Verify the installation:

```bash
pg_dump --version
```

**Security Warning**: It is recommended to not expose port 5432 publicly in production environments.

## Status

Use this command to view node logs in real-time

```bash
sudo journalctl -u kwild -f
```

This command will provide last 100 lines of logs

```bash
sudo journalctl -u kwild -n 100
```

To view logs with precise timestamp use

```bash
sudo journalctl -u kwild -f --output=short-precise
```

Use this command to check service status

```bash
sudo systemctl status kwild
```

## Docker Container Status

To check the status of your Docker containers:

```bash
docker ps
```

## PostgreSQL Logs

To view PostgreSQL container logs:

```bash
docker logs tn-postgres
```

## Accessing PostgreSQL

To access the PostgreSQL database directly:

```bash
# Using the Docker container
docker exec -it tn-postgres psql -U postgres -d kwild
```

Common useful PostgreSQL commands:

```sql
-- List all databases
\l

-- Connect to kwild database
\c kwild

-- Exit psql
\q
```

## System Boot Logs

To view logs since the last system boot:

```bash
sudo journalctl -u kwild -b
```

## Clean Removal

> **Warning**: The following steps will completely remove your node setup, including all data and configuration. This is irreversible and should only be done if:
>
> - You need to start fresh due to configuration issues
> - You're moving to a different server
> - You're troubleshooting persistent problems that require a clean slate
>
> Make sure to backup any important data before proceeding.

To completely remove the node setup:

```bash
# Stop and remove services
sudo systemctl stop kwild tn-postgres
sudo systemctl disable kwild tn-postgres
sudo rm /etc/systemd/system/kwild.service /etc/systemd/system/tn-postgres.service
sudo systemctl daemon-reload

# Remove Docker resources
docker stop tn-postgres
docker rm tn-postgres
docker volume rm tn-pgdata

# Remove node configuration
rm -rf $HOME/truf-node-operator/my-node-config
```
