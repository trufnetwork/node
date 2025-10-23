# AMI Deployment Guide

Deploy a TRUF.NETWORK node in minutes using our pre-configured Amazon Machine Image (AMI). This guide provides a quick and easy alternative to manual setup, reducing deployment time from 45-60 minutes to just 5-10 minutes.

## Prerequisites

- **AWS Account** with EC2 access
- **SSH Key Pair** in your target AWS region
- **Basic AWS Console** familiarity (launching instances, security groups)

> **New to AWS?** Check out the [AWS EC2 Getting Started Guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html) first.

## Quick Start

### 1. Find the AMI

In the AWS EC2 Console:

1. Navigate to **EC2 Dashboard** → **Launch Instance**
2. Click **Browse more AMIs**
3. Select the **Community AMIs** tab
4. Search for: `TRUF.NETWORK node` or `tn-ami-recipe`
5. Select the latest AMI (look for the most recent creation date)

### 2. Configure Instance

#### Instance Settings
- **Instance type**: `t3.medium` (minimum) or `t3.large` (recommended)
- **Key pair**: Select your existing SSH key pair
- **VPC/Subnet**: Use default or your preferred network setup

#### Security Groups

**Required Inbound Rules** for your node to participate in the network:

| Type | Protocol | Port Range | Source | Description                               |
|------|----------|------------|--------|-------------------------------------------|
| SSH | TCP | 22 | Your IP | Remote access                             |
| Custom TCP | TCP | 6600 | 0.0.0.0/0 | P2P node communication (recommended) |
| Custom TCP | TCP | 8484 | 0.0.0.0/0 | RPC service (if running public node)      |

**Optional Inbound Rules:**

| Type | Protocol | Port Range | Source | Description |
|------|----------|------------|--------|-------------|
| Custom TCP | TCP | 8000 | Your IP or 0.0.0.0/0 | MCP Server for AI integration |

> **Important**:
> - **Port 6600**: Enables two-way P2P communication. Your node can sync without this (via outbound connections), but opening it helps network health by accepting incoming peer connections
> - **Port 8484**: Needed if you want users/applications to query data from your node
> - **Port 8000**: Only needed for MCP/AI integration (like Claude Code)
> - **Port 22** (SSH): Should be restricted to your IP for security

#### Storage
- **Root volume**: 30 GB minimum (50+ GB recommended)
- **Volume type**: gp3 (recommended)

### 3. Launch and Connect

1. **Launch the instance** and wait for it to reach "running" state
2. **Connect via SSH**:
```bash
ssh -i ~/.ssh/your-key.pem ubuntu@<your-instance-public-ip>
```

### 4. Initial Configuration

Upon first login, you'll see a welcome message with configuration options. The AMI uses command-line configuration:

**Basic configuration (Mainnet)**:
```bash
# Basic setup (auto-generated private key, no MCP)
sudo tn-node-configure

# With your own private key
sudo tn-node-configure --private-key "your-64-character-hex-key"

# With MCP enabled for AI integration
sudo tn-node-configure --enable-mcp

# Full mainnet configuration
sudo tn-node-configure \
  --private-key "your-key" \
  --enable-mcp
```

### 5. Verify Node Operation

Check that your node is running and syncing:

```bash
# Check Docker containers status
docker ps

# Should show containers like:
# - tn-postgres (PostgreSQL database)
# - tn-node (TN node)
# - tn-mcp (MCP server, if enabled)

# Check systemd service status
sudo systemctl status tn-node

# Check node status (once containers are running)
docker exec tn-node ./kwild admin status

# Expected output (once synced):
# {
#   "node_info": {
#     "version": "...",
#     "syncing": false,
#     "best_block_height": "12345"
#   }
# }
```

### 6. MCP Server Setup (Optional)

If you enabled MCP during configuration, test the connection:

```bash
# Check MCP server status
docker ps | grep tn-mcp

# Test MCP endpoint locally
curl -s http://localhost:8000/sse || echo "MCP server not accessible"
```

#### Connecting Claude Code to Your Node

To connect Claude Code from your local machine:

1. **Get your instance public IP** from AWS Console or:
```bash
curl http://checkip.amazonaws.com
```

2. **Ensure port 8000 is open** in your security group (see Security Groups section above)

3. **Configure Claude Code** by editing `claude_desktop_config.json`:
   ```json
   {
     "mcpServers": {
       "truf-postgres": {
         "command": "mcp-remote",
         "args": [
           "http://<your-instance-public-ip>:8000/sse",
           "--allow-http"
         ]
       }
     }
   }
   ```

4. **Test the connection** from your local machine:
```bash
curl -s http://<your-instance-public-ip>:8000/sse
```

5. **Restart Claude Code** to apply the configuration

## Advanced Configuration

### Enable Cache Extension

The `tn_cache` extension provides **node-local caching** for expensive stream queries, making reads on deep composed streams as fast as on simple primitive streams. Enabling it is optional and **affects only your node**.

To enable the cache extension on the AMI deployment:

1. **Access the container shell**:
```bash
docker exec -it tn-node sh
```

2. **Edit the config file** (you can use `vi` or `sed`):
```bash
# Using nano
nano /root/.kwild/config.toml

# Or using sed to append the configuration
cat >> /root/.kwild/config.toml << 'EOF'

[extensions.tn_cache]
enabled = "true"
# Optional: Add stream configs here - see detailed guide for examples
EOF
```

3. **Exit the container**:
```bash
exit
```

4. **Restart the node** to apply changes:
```bash
sudo systemctl restart tn-node
```

5. **Verify the cache is enabled**:
```bash
docker logs tn-node | grep -i cache
```

**Caveats (when cache is ignored)**:
- `frozen_at` or `base_time` parameters set → falls back to full computation
- Primitive streams (`*_primitive` actions) are never cached
- `get_index_change` relies on underlying cache via `get_index`; same rules apply

For complete configuration options (stream lists, schedules, metrics, troubleshooting), see:
[extensions/tn_cache/README.md](https://github.com/trufnetwork/node/blob/main/extensions/tn_cache/README.md)

### Become a Validator

Once your node is fully synced:

```bash
# Submit validator join request (via Docker)
docker exec tn-node ./kwild validators join

# Check request status
docker exec tn-node ./kwild validators list-join-requests

# Get your node info for validator approval
docker exec tn-node ./kwild key info --key-file /root/.kwild/nodekey.json
```

## Monitoring and Maintenance

### View Logs

```bash
# System service logs
sudo journalctl -u tn-node -f

# Individual container logs
docker logs -f tn-node      # Node logs
docker logs -f tn-postgres   # PostgreSQL logs
docker logs -f tn-mcp        # MCP server logs (if enabled)

# All container logs
cd /opt/tn
sudo -u tn docker compose logs -f
```

### Health Checks

```bash
# Node sync status
docker exec tn-node ./kwild admin status | grep syncing

# Database connectivity
docker exec -it tn-postgres psql -U postgres -c "SELECT version();"

# MCP server access
curl -s http://localhost:8000/sse

# Check all container status
docker ps
sudo systemctl status tn-node
```

### Updates

```bash
# Update to latest Docker images
sudo tn-node-update

# Manual update process
cd /opt/tn
sudo -u tn docker compose pull
sudo -u tn docker compose up -d --force-recreate

# Check status after update
sudo systemctl status tn-node
docker ps
```

## Troubleshooting

### Common Issues

**Node not syncing**
```bash
# Check peer connections
docker exec tn-node ./kwild admin status | grep peer

# Check container status
docker ps
sudo systemctl status tn-node
```

**MCP server not accessible**
```bash
# Check if container is running
docker ps | grep tn-mcp

# Verify security group allows port 8000
# Check AWS Console → Security Groups
```

**Database connection errors**
```bash
# Restart PostgreSQL
docker restart tn-postgres

# Check database logs
docker logs tn-postgres
```

### Getting Help

- **GitHub**: [Node Repository Issues](https://github.com/trufnetwork/node/issues)
- **Documentation**: [Manual Setup Guide](./node-operator-guide.md) for advanced customization

## Comparison with Manual Setup

| Aspect | AMI Deployment | Manual Setup |
|--------|----------------|---------------|
| **Time to Deploy** | 5-10 minutes | 45-60 minutes |
| **Prerequisites** | AWS account only | 5+ tools to install |
| **Complexity** | Simple (guided setup) | Complex (25+ commands) |
| **Customization** | Basic options | Full control |
| **Error Prone** | Low (pre-tested) | Higher (many steps) |
| **MCP Integration** | Pre-configured | Manual setup required |

## Migration from Manual Setup

Already running a manual node? You can migrate your identity:

1. **Export your node identity**: Backup `nodekey.json` from your existing setup
2. **Launch AMI instance**: Follow this guide
3. **Configure with existing private key**: Use `sudo tn-node-configure --private-key "your-key"`
4. **The AMI will handle the rest**: Docker containers will use your existing identity

**Note**: The AMI uses Docker Compose instead of direct binary installation.

## Next Steps

1. **Join the Network**: Your node will automatically start syncing and participating
2. **Become a Validator**: Submit a validator join request once synced
3. **Connect AI Tools**: Use the MCP server for AI integration with your node

---

**Need more control?** Consider the [Manual Setup Guide](./node-operator-guide.md) for full customization options.

**Questions?** Feel free to post them to our [GitHub Issues](https://github.com/trufnetwork/node/issues).