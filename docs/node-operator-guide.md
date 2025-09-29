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
sudo apt-get install -y postgresql-client-16 postgresql-16-repack
```

#### For macOS

You can install the PostgreSQL client using [Homebrew](https://brew.sh/). If you don't have Homebrew, install it first by following the instructions on their website.

```bash
brew install postgresql@16 pg_repack
```

To use it from any terminal, you may need to add it to your `PATH`. For `zsh` (the default in modern macOS):

```bash
# Apple Silicon (M1/M2) Macs:
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc

# Intel Macs:
echo 'export PATH="/usr/local/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc

# Apply changes
source ~/.zshrc
```

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

### 5. Kwild Binary

You have two options to get the `kwild` binary.

1.  **Download from Releases**:

    - Visit [TRUF.NETWORK Node Releases](https://github.com/trufnetwork/node/releases)
    - Download the latest release for your operating system and architecture (e.g., `..._linux_amd64.tar.gz` for Linux or `..._darwin_amd64.tar.gz` for macOS on Intel).
    - Extract the binary and move it to your system path:

    ```bash
    # Example for Linux:
    # tar -xzf tn_2.0.1_linux_amd64.tar.gz
    # sudo mv kwild /usr/local/bin/

    # Example for macOS Intel:
    # tar -xzf tn_2.0.1_darwin_amd64.tar.gz
    # sudo mv kwild /usr/local/bin/
    ```

2.  **Build from Source** (Recommended):

    Building from source is recommended because it ensures you have the latest features and security updates directly from the development branch. It also allows you to verify the code you're running and make any necessary customizations. Most importantly, building from source ensures the binary is compiled specifically for your machine's architecture and operating system, which can lead to better performance and compatibility.

    ```bash
    # Clone the repository
    git clone https://github.com/trufnetwork/node.git
    cd node
    # Pull latest changes to ensure we have the most up-to-date code
    git pull
    # Build the binary
    task build
    ```

    The built binary will be in the `.build` directory. Move it to your system path:

    ```bash
    sudo mv .build/kwild /usr/local/bin/
    sudo chmod +x /usr/local/bin/kwild
    ```

    For Linux users, apply new docker group:

    ```bash
    newgrp docker
    ```

### Verify Installation

Before you move forward to Node Setup, verify that everything installed correctly and is the right version.

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

From your home directory, clone the TRUF.NETWORK node operator repository:

```bash
cd # Return to home directory if not already there
git clone https://github.com/trufnetwork/truf-node-operator.git
cd truf-node-operator
```

### 2. Generate Initial Configuration

Use `kwild` to create your initial configuration file. This command is the same for all operating systems.

```bash
kwild setup init \
  --genesis ./configs/network/v2/genesis.json \
  --root ./my-node-config \
  --p2p.bootnodes "4e0b5c952be7f26698dc1898ff3696ac30e990f25891aeaf88b0285eab4663e1#ed25519@node-1.mainnet.truf.network:26656,0c830b69790eaa09315826403c2008edc65b5c7132be9d4b7b4da825c2a166ae#ed25519@node-2.mainnet.truf.network:26656" \
  --state-sync.enable \
  --state-sync.trusted-providers "0c830b69790eaa09315826403c2008edc65b5c7132be9d4b7b4da825c2a166ae#ed25519@node-2.mainnet.truf.network:26656" \
  --rpc.private
```

These flags enable state sync for faster initial synchronization using snapshots from the specified trusted provider, and enable private RPC mode for enhanced security.

For detailed instructions on configuration options more relevant to a production setup, refer to our [Configuration Guide](https://github.com/trufnetwork/truf-node-operator/blob/main/docs/creating-config.md).

### 3. Set Up PostgreSQL

For a quick setup, run Kwil's pre-configured PostgreSQL Docker image. The command is the same for Linux and macOS. For macOS, ensure Docker Desktop is running.

```bash
docker run -d -p 127.0.0.1:5432:5432 --name tn-postgres \
    -e "POSTGRES_HOST_AUTH_METHOD=trust" \
    -v tn-pgdata:/var/lib/postgresql/data \
    --shm-size=1gb \
    kwildb/postgres:latest
```

> **Warning**: Critical Security Requirements
>
> 1. `kwild` requires a "superuser" role to perform various tasks that require elevated privileges, such as creating triggers and publications for logical replication. The PostgreSQL database cluster should be dedicated to `kwild`, and should not be used for any other purpose.
>
> 2. NEVER expose PostgreSQL port (5432) to the public internet. Always bind to localhost (127.0.0.1) as shown in the example above.

> **Securing PostgreSQL Port on Linux:**
>
> 1. Use UFW (Uncomplicated Firewall) to block port 5432:
>
>    ```bash
>    sudo ufw deny 5432/tcp
>    ```
>
> 2. Verify no external access to port 5432:
>    ```bash
>    sudo ss -tulpn | grep 5432
>    # Should only show 127.0.0.1:5432, not 0.0.0.0:5432
>    ```

The command above:

- `-v tn-pgdata:/var/lib/postgresql/data`: Creates a persistent volume named 'tn-pgdata' to store database data
- `--shm-size=1gb`: Allocates 1GB of shared memory for PostgreSQL operations (recommended for better performance)

### 4. Create background services for `kwild` and PostgreSQL

#### For Linux (using `systemd`)

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

#### For macOS (using `launchd`)

On macOS, `launchd` is used instead of `systemd` to manage background services. The following steps will create services that start `kwild` and PostgreSQL automatically and keep them running. These services will be created for the current user.

> **Note on Mac Architecture:** The `PATH` environment variable in the `kwild` service file below is configured for Apple Silicon (M1/M2) Macs, where Homebrew installs to `/opt/homebrew`. If you are on an Intel-based Mac, Homebrew installs to `/usr/local`, and you will need to modify the `PATH` string inside the `com.trufnetwork.kwild.plist` file after creating it.
>
> **For Intel Macs**, change this line:
> `<string>/opt/homebrew/opt/postgresql@16/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>`
> to:
> `<string>/usr/local/opt/postgresql@16/bin:/usr/local/bin:/usr/bin:/bin</string>`

> **Important for macOS Users:** The following commands create service files with absolute paths to your home directory. Before you proceed, please find your home directory path by running `echo $HOME` in your terminal. You will need to replace all instances of `{REPLACE_WITH_YOUR_HOME}` (including the curly braces) in the following scripts with the output of that command. For example, if `echo $HOME` returns `/Users/johndoe`, you would replace `{REPLACE_WITH_YOUR_HOME}` with `/Users/johndoe`.

**1. Create `kwild` Service**

This creates a `launchd` service file for the `kwild` node. It will be configured to restart automatically if it stops.

```bash
cat > ~/Library/LaunchAgents/com.trufnetwork.kwild.plist << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.trufnetwork.kwild</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/kwild</string>
        <string>start</string>
        <string>-r</string>
        <string>{REPLACE_WITH_YOUR_HOME}/truf-node-operator/my-node-config</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/opt/postgresql@16/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>HOME</key>
        <string>{REPLACE_WITH_YOUR_HOME}</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>WorkingDirectory</key>
    <string>{REPLACE_WITH_YOUR_HOME}/truf-node-operator</string>
    <key>StandardOutPath</key>
    <string>{REPLACE_WITH_YOUR_HOME}/Library/Logs/kwild.log</string>
    <key>StandardErrorPath</key>
    <string>{REPLACE_WITH_YOUR_HOME}/Library/Logs/kwild.error.log</string>
</dict>
</plist>
EOF
```

**2. Create PostgreSQL Service**

This creates a `launchd` service file to manage the `tn-postgres` Docker container.

```bash
cat > ~/Library/LaunchAgents/com.trufnetwork.tn-postgres.plist << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.trufnetwork.tn-postgres</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/docker</string>
        <string>start</string>
        <string>-a</string>
        <string>tn-postgres</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>WorkingDirectory</key>
    <string>{REPLACE_WITH_YOUR_HOME}/truf-node-operator</string>
    <key>StandardOutPath</key>
    <string>{REPLACE_WITH_YOUR_HOME}/Library/Logs/tn-postgres.log</string>
    <key>StandardErrorPath</key>
    <string>{REPLACE_WITH_YOUR_HOME}/Library/Logs/tn-postgres.error.log</string>
</dict>
</plist>
EOF
```

> **Note:** For these services to work, Docker Desktop must be configured to start on login. You can enable this in Docker Desktop's settings (`Settings > General > Start Docker Desktop when you log in`).

### 5. Run TN Node

Before you proceed, ensure your firewall allows incoming connections on:

- JSON-RPC port (default: 8484)
- P2P port (default: 6600)

#### For Linux

```bash
sudo systemctl daemon-reload
sudo systemctl enable tn-postgres
sudo systemctl enable kwild
sudo systemctl start tn-postgres
sudo systemctl start kwild
```

**Security Warning**: It is recommended to not expose port 5432 publicly in production environments.

#### For macOS

Load the services to enable them to start automatically, then start them manually for the first time.

```bash
launchctl load ~/Library/LaunchAgents/com.trufnetwork.tn-postgres.plist
launchctl start com.trufnetwork.tn-postgres

launchctl load ~/Library/LaunchAgents/com.trufnetwork.kwild.plist
launchctl start com.trufnetwork.kwild
```

### 6. Verify Node Synchronization

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
> **For Linux:**
>
> ```bash
> sudo journalctl -u kwild -f
> ```
>
> **For macOS:**
>
> ```bash
> # View kwild logs in real-time
> tail -f ~/Library/Logs/kwild.log
>
> # View kwild error logs in real-time
> tail -f ~/Library/Logs/kwild.error.log
>
> # View last 100 lines of kwild logs
> tail -n 100 ~/Library/Logs/kwild.log
>
> # View PostgreSQL logs in real-time
> tail -f ~/Library/Logs/tn-postgres.log
> ```

### Cache Extension (tn_cache)

The `tn_cache` extension provides **node-local caching** for expensive stream queries, making reads on deep composed streams as fast as on simple primitive streams while remaining isolated from network consensus.  Enabling it is optional and **affects only your node**.

**Quick enable**

```toml
[extensions.tn_cache]
enabled = "true"
# add stream configs here – see detailed guide for examples
```

After editing `config.toml`, restart `kwild` for the change to take effect.

**Caveats (when cache is ignored)**
- `frozen_at` or `base_time` parameters set → falls back to full computation
- Primitive streams (`*_primitive` actions) are never cached
- `get_index_change` relies on underlying cache via `get_index`; same rules apply

For complete configuration options (stream lists, schedules, metrics, troubleshooting) see the operator-focused section of the tn_cache documentation:

[extensions/tn_cache/README.md#operations--monitoring](../extensions/tn_cache/README.md#operations--monitoring)

### Vacuum Extension (tn_vacuum)

The `tn_vacuum` extension provides **automated database maintenance** through periodic vacuuming operations. It helps reclaim disk space and optimize database performance by removing dead tuples using `pg_repack`.

> **Note:** If you're using the official TrufNetwork node image, `pg_repack` is already included. For custom installations, see the installation guide in the extension documentation.

**Quick enable**

```toml
[extensions.tn_vacuum]
enabled = true
block_interval = 50000  # runs vacuum every 50k blocks
```

After editing `config.toml`, restart `kwild` for the change to take effect.

For tuning guidance, metrics, and troubleshooting, see the full documentation:

[extensions/tn_vacuum/README.md](../extensions/tn_vacuum/README.md)

### 7. Become a Validator (Optional)

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

### 8. Submit Your Node to Available Node List (Optional)

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
kwild key info -o ./my-node-config/nodekey.json
```

note: You can also access the user address, public / private key for defining your genesis file.

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
sudo apt-get install postgresql-client-16 postgresql-16-repack
```

For CentOS/RHEL:

```bash
sudo yum install postgresql16 pg_repack_16
```

For macOS (using Homebrew):

```bash
brew install postgresql@16 pg_repack
```

Verify the installation:

```bash
pg_dump --version
```

**Security Warning**: It is recommended to not expose port 5432 publicly in production environments.

### Missing Logs on Linux (`journalctl` shows no entries)

If you're running `kwild` as a `systemd` service but encounter the following when checking logs:

```bash
sudo journalctl -u kwild -n 100
# Output:
# No journal files were found.
# -- No entries --
````

This usually means that your system is not configured to persist logs by default. This behavior is common on some cloud images or **non-LTS Ubuntu releases** such as Ubuntu 24.10 (oracular), which may skip creating the persistent journal directory `/var/log/journal`.

#### ✅ Solution: Enable persistent logging

1. **Configure journald to use persistent storage**:

```bash
sudo nano /etc/systemd/journald.conf
```

Add or uncomment the following line under the `[Journal]` section:

```
Storage=persistent
```
2. **Restart the journald service** to apply the change:

```bash
sudo systemctl restart systemd-journald
```

3. **(Optional)** If logs still do not appear, create the persistent log directory manually:

```bash
sudo mkdir -p /var/log/journal
sudo systemd-tmpfiles --create --prefix /var/log/journal
sudo systemctl restart systemd-journald
```

4. **Restart the kwild service** so its output is reattached to the journal:

```bash
sudo systemctl restart kwild
```

Once complete, you should be able to view `kwild` logs with:

```bash
sudo journalctl -u kwild -n 100
```

This setup ensures that logs are written to disk and available after reboot.

## Viewing Logs and Status

### For Linux

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

### For macOS

On macOS, with our service configuration, logs are written to dedicated log files, making them easy to access and monitor.

To view `kwild` logs in real-time:

```bash
# View kwild logs in real-time
tail -f ~/Library/Logs/kwild.log

# View kwild error logs in real-time
tail -f ~/Library/Logs/kwild.error.log

# View last 100 lines of kwild logs
tail -n 100 ~/Library/Logs/kwild.log

# View PostgreSQL logs in real-time
tail -f ~/Library/Logs/tn-postgres.log
```

To check the status of the services (a non-zero status in the second column may indicate an issue):

```bash
# Check kwild service (PID, Status, Label)
launchctl list | grep com.trufnetwork.kwild

# Check postgres service
launchctl list | grep com.trufnetwork.tn-postgres
```

#### Understanding launchctl Output Format

The `launchctl list` command shows three columns:

- **Process ID (PID)**: Shows the process ID if the service is running, or "-" if not running
- **Exit Code**: Shows the exit code if the service has stopped (0 indicates successful termination)
- **Label**: The service identifier

Expected output when services are running properly:

```12345   0   com.trufnetwork.kwild
12346   0   com.trufnetwork.tn-postgres
```

If a service is not running, you'll see:

```
-       0   com.trufnetwork.kwild
```

### Network Connection Issues

If you see peer disconnection messages like `[WRN] PEERS: Failed to get peers from ...: failed to read and decode peers: stream reset` or `[INF] PEERS: Disconnected from peer ...` in your logs, this usually indicates VPN or firewall issues. Try disabling your VPN temporarily or check your firewall settings to resolve connectivity problems.

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

## Resetting Your Node Instance

Sometimes you may need to reset your node to sync from a specific point or recover from an inconsistent state. This is different from a complete removal - you preserve your configuration but reset the blockchain data.

### When to Reset Your Node

- Network fork or critical incident requiring new genesis (follow the [Node Upgrade Guide](node-upgrade-guide.md#network-fork-recovery) if you intend to create a fork)
- Database corruption or inconsistent state
- Joining the network from a specific block height
- Following network recovery procedures

### Reset Procedure

1. **Stop and disable services**:
   ```bash
   # For Linux
   sudo systemctl stop kwild tn-postgres
   sudo systemctl disable kwild tn-postgres
   sudo systemctl daemon-reload
   
   # For macOS
   launchctl stop com.trufnetwork.kwild
   launchctl stop com.trufnetwork.tn-postgres
   ```

2. **Backup node identity and configuration** (to preserve them):
   ```bash
   # Create backup directory
   mkdir -p ~/truf-node-operator/backup-reset
   
   # Backup nodekey to preserve node identity
   cp ~/truf-node-operator/my-node-config/nodekey.json ~/truf-node-operator/backup-reset/
   
   # Backup config.toml if you want to preserve custom settings (optional)
   cp ~/truf-node-operator/my-node-config/config.toml ~/truf-node-operator/backup-reset/
   ```

3. **Remove PostgreSQL data and node configuration**:
   ```bash
   # Remove Docker resources
   docker stop tn-postgres
   docker rm tn-postgres
   docker volume rm tn-pgdata
   
   # Remove entire node configuration directory
   rm -rf ~/truf-node-operator/my-node-config
   ```

4. **Regenerate node configuration**:
   ```bash
   # Pull latest changes (for network forks)
   cd ~/truf-node-operator
   git pull
   
   # Reinitialize configuration
   kwild setup init \
     --genesis ./configs/network/v2/genesis.json \
     --root ./my-node-config \
     --p2p.bootnodes "4e0b5c952be7f26698dc1898ff3696ac30e990f25891aeaf88b0285eab4663e1#ed25519@node-1.mainnet.truf.network:26656,0c830b69790eaa09315826403c2008edc65b5c7132be9d4b7b4da825c2a166ae#ed25519@node-2.mainnet.truf.network:26656" \
     --state-sync.enable \
     --state-sync.trusted-providers "0c830b69790eaa09315826403c2008edc65b5c7132be9d4b7b4da825c2a166ae#ed25519@node-2.mainnet.truf.network:26656" \
     --rpc.private
   ```

   > **Note**: 
    - If you are also using a new snapshot, you'll need to update the `genesis_state` parameter in the `config.toml` file to point to the new snapshot.
    - `p2p.bootnodes`, will define peer nodes, if you have none then please delete from command
    - `trusted-providers`, will define the main provider of the distributed data, if you are the provider, then please delete from command

5. **Update kwild binary** *(optional but recommended)*:
   ```bash
   # Option 1: Download latest release
   curl -L "https://github.com/trufnetwork/node/releases/latest/download/tn_<VERSION>_linux_amd64.tar.gz" \
       -o kwild.tgz
   tar -xzf kwild.tgz kwild
   sudo mv kwild /usr/local/bin/kwild
   sudo chmod +x /usr/local/bin/kwild
   
   # Option 2: Build from source
   cd ~/node # assuming you cloned the node repository here, please adjust if different
   git pull && task build
   sudo mv .build/kwild /usr/local/bin/kwild
   sudo chmod +x /usr/local/bin/kwild
   
   # Verify version
   kwild version
   ```

6. **Restore node identity and custom configuration** *(optional)*:
   
   > **Note**: Skip this step if you want to start with a completely fresh node identity and default configuration. Only restore if you need to preserve your existing validator status or custom settings.
   
   ```bash
   # Restore nodekey to preserve node identity
   cp ~/truf-node-operator/backup-reset/nodekey.json ~/truf-node-operator/my-node-config/
   
   # Restore config.toml if you want to keep custom settings (optional)
   cp ~/truf-node-operator/backup-reset/config.toml ~/truf-node-operator/my-node-config/
   ```

7. **Recreate PostgreSQL**:
   ```bash
   docker run -d -p 127.0.0.1:5432:5432 --name tn-postgres \
       -e "POSTGRES_HOST_AUTH_METHOD=trust" \
       -v tn-pgdata:/var/lib/postgresql/data \
       --shm-size=1gb \
       kwildb/postgres:latest
   ```

8. **Re-enable and start services**:
   ```bash
   # For Linux
   sudo systemctl enable tn-postgres kwild
   sudo systemctl start tn-postgres
   sudo systemctl start kwild
   
   # For macOS
   launchctl start com.trufnetwork.tn-postgres
   launchctl start com.trufnetwork.kwild
   ```

9. **Monitor synchronization**:
   ```bash
   kwild admin status
   ```
   Wait for `syncing: false` and ensure your `best_block_height` is advancing.

### Important Notes

- This process preserves your node identity (nodekey.json) and configuration files
- Your validator status (if applicable) is preserved
- The node will resync from the genesis block specified in the new genesis.json
- State sync (if enabled in your config) allows fast resynchronization
- For critical network incidents, always check the [Node Upgrade Guide](node-upgrade-guide.md#breaking-changes--migrations) for specific instructions

## Clean Removal

> **Warning**: The following steps will completely remove your node setup, including all data and configuration. This is irreversible and should only be done if:
>
> - You need to start fresh due to configuration issues
> - You're moving to a different server
> - You're troubleshooting persistent problems that require a clean slate
>
> Make sure to backup any important data before proceeding.

### For Linux

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

# Remove node configuration (optional - only if you want to regenerate nodekey)
# This will create a new node identity on next init
rm -rf $HOME/truf-node-operator/my-node-config
```

### For macOS

```bash
launchctl unload ~/Library/LaunchAgents/com.trufnetwork.kwild.plist
launchctl unload ~/Library/LaunchAgents/com.trufnetwork.tn-postgres.plist

rm ~/Library/LaunchAgents/com.trufnetwork.kwild.plist
rm ~/Library/LaunchAgents/com.trufnetwork.tn-postgres.plist

rm -f ~/Library/Logs/kwild.log
rm -f ~/Library/Logs/kwild.error.log
rm -f ~/Library/Logs/tn-postgres.log
rm -f ~/Library/Logs/tn-postgres.error.log

docker stop tn-postgres
docker rm tn-postgres
docker volume rm tn-pgdata

# Remove node configuration (optional - only if you want to regenerate nodekey)
# This will create a new node identity on next init
rm -rf $HOME/truf-node-operator/my-node-config
```

## Security Recommendations

We've enabled RPC private mode in the init command above to prevent arbitrary SQL executions. This reduces the risk of unintended SQL-based DoS attacks without enabling unauthorized writes.

If you need to modify this setting, edit your `config.toml`:

```toml
[rpc]

# Enforce data privacy: authenticate JSON-RPC call requests using challenge-based
# authentication. the node will only accept JSON-RPC requests that has a valid signed
# challenge response. This also disables ad hoc queries, and no raw transaction retrieval.
private = true
```

For more details, see the [Kwil Private RPC documentation](http://docs.kwil.com/docs/node/private-rpc).
