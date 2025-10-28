# Digital Ocean Deployment Guide

This guide helps you deploy a TRUF.NETWORK node on Digital Ocean and expose it to external clients. Follow these steps to configure networking, firewall rules, and optional DNS settings for your node.

## Prerequisites

- **Digital Ocean Droplet** running your TN node (see [Node Operator Guide](./node-operator-guide.md) for initial setup)
- **Node running** and syncing with the network
- **Basic familiarity** with Digital Ocean control panel

> **New to Digital Ocean?** Check out the [Digital Ocean Getting Started Guide](https://docs.digitalocean.com/products/getting-started/) first.

## Overview

This guide focuses on the networking configuration required to make your node accessible to external applications and the network. You'll configure:

1. **Cloud Firewall rules** to allow necessary ports
2. **Public IP discovery** for SDK integration
3. **Optional DNS setup** for custom domains
4. **Connectivity testing** to verify your setup

## Port Requirements

Before configuring your firewall, understand which ports your node needs:

### Required Ports

| Port | Protocol | Purpose | Source |
|------|----------|---------|--------|
| **6600** | TCP | P2P node communication | 0.0.0.0/0 (All IPv4) |
| **8484** | TCP | RPC service for queries | 0.0.0.0/0 (All IPv4) |

### Optional Ports

| Port | Protocol | Purpose | Source |
|------|----------|---------|--------|
| **8000** | TCP | MCP Server for AI integration | Your IP or 0.0.0.0/0 |
| **22** | TCP | SSH access | Your IP (restricted) |

### Ports to NEVER Expose

| Port | Protocol | Why |
|------|----------|-----|
| **5432** | TCP | PostgreSQL database - **CRITICAL SECURITY RISK** if exposed |

> **Important Notes:**
> - **Port 6600**: Enables two-way P2P communication. Your node can sync without this (via outbound connections), but opening it helps network health by accepting incoming peer connections.
> - **Port 8484**: Required if you want users/applications to query data from your node.
> - **Port 8000**: Only needed for MCP/AI integration (like Claude Code).
> - **Port 5432**: Should ONLY bind to localhost (127.0.0.1). Never allow external access.

## Step 1: Configure Digital Ocean Cloud Firewall

Digital Ocean Cloud Firewalls are network-based, stateful firewalls provided at no additional cost.

### Option A: Create a New Firewall (Recommended)

1. **Navigate to Firewalls**:
   - Go to the [Digital Ocean Control Panel](https://cloud.digitalocean.com)
   - Click **Networking** → **Firewalls**
   - Click **Create Firewall**

2. **Name your firewall**:
   - Enter a descriptive name (e.g., `tn-node-firewall`)

3. **Configure Inbound Rules**:

   Click **New rule** and add the following:

   **Rule 1: P2P Communication**
   - Type: **Custom**
   - Protocol: **TCP**
   - Port Range: `6600`
   - Sources: **All IPv4** and **All IPv6** (or use `0.0.0.0/0` and `::/0`)

   **Rule 2: RPC Service**
   - Type: **Custom**
   - Protocol: **TCP**
   - Port Range: `8484`
   - Sources: **All IPv4** and **All IPv6**

   **Rule 3: SSH Access** (if not already present)
   - Type: **SSH** (preset)
   - Protocol: **TCP** (auto-filled)
   - Port Range: `22` (auto-filled)
   - Sources: **Your IP address** (recommended) or **All IPv4** (less secure)

   **Optional Rule 4: MCP Server** (only if using AI integration)
   - Type: **Custom**
   - Protocol: **TCP**
   - Port Range: `8000`
   - Sources: **Your IP address** or **All IPv4** (depending on your use case)

4. **Apply to Droplets**:
   - In the **Apply to Droplets** section, select your TN node Droplet
   - Click **Create Firewall**

### Option B: Add Rules to Existing Firewall

If you already have a firewall attached to your Droplet:

1. Navigate to **Networking** → **Firewalls**
2. Select your existing firewall
3. Click the **Rules** tab
4. Under **Inbound Rules**, click **New rule** and add the rules from Option A above
5. Click **Save**

### Verify Firewall Configuration

```bash
# SSH into your Droplet
ssh root@your-droplet-ip

# Check that ports 6600 and 8484 are listening
sudo ss -tulpn | grep -E '6600|8484'

# Expected output should show kwild listening on these ports:
# tcp   LISTEN 0  4096  0.0.0.0:6600  0.0.0.0:*
# tcp   LISTEN 0  4096  0.0.0.0:8484  0.0.0.0:*
```

## Step 2: Find Your Droplet's Public IP Address

Your node's endpoint URL requires the Droplet's public IP address.

### Via Digital Ocean Control Panel

1. Go to [Digital Ocean Control Panel](https://cloud.digitalocean.com)
2. Navigate to **Droplets**
3. Locate your TN node Droplet
4. The **Public IPv4** address is displayed in the Droplet's information panel

### Via Command Line (SSH into Droplet)

```bash
# Method 1: Query external service
curl -4 ifconfig.co

# Method 2: Using Digital Ocean metadata service
curl -s http://169.254.169.254/metadata/v1/interfaces/public/0/ipv4/address

# Method 3: Using ip command
ip -4 addr show eth0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}'
```

### Via doctl (Digital Ocean CLI)

If you have [doctl](https://docs.digitalocean.com/reference/doctl/how-to/install/) installed:

```bash
# List all droplets with their IPs
doctl compute droplet list --format Name,PublicIPv4

# Get specific droplet info
doctl compute droplet get <droplet-id> --format Name,PublicIPv4
```

## Step 3: Construct Your Node Endpoint URL

Once you have your public IP address, your node endpoint follows this format:

```
http://YOUR_DROPLET_IP:8484
```

**Example:**
If your Droplet's public IP is `203.0.113.45`, your endpoint is:
```
http://203.0.113.45:8484
```

## Step 4: Test Node Connectivity

Verify that your node is accessible from external clients.

### Test Health Endpoint

From your **local machine** (not the Droplet):

```bash
# Test health check endpoint
curl http://YOUR_DROPLET_IP:8484/api/v1/health

# Expected response (if healthy):
# {
#   "healthy": true,
#   "services": {
#     "user": {
#       "syncing": false,
#       "block_height": "12345"
#     }
#   }
# }
```

### Test Node Status

```bash
# SSH into your Droplet
ssh root@YOUR_DROPLET_IP

# Check node sync status
kwild admin status

# Or if using Docker/systemd
# For systemd: sudo systemctl status kwild
# For Docker: docker exec <container-name> ./kwild admin status
```

### Test P2P Connectivity

```bash
# From your local machine, test if port 6600 is accessible
nc -zv YOUR_DROPLET_IP 6600

# Expected output:
# Connection to YOUR_DROPLET_IP 6600 port [tcp/*] succeeded!
```

If this test fails, verify your firewall rules are correctly configured.

## Step 5: Configure DNS (Optional)

Using a custom domain instead of an IP address makes your node endpoint more memorable and portable.

### Prerequisites

- A domain name you own
- Access to your domain registrar's settings

### Option A: Use Digital Ocean DNS (Recommended)

#### 1. Add Domain to Digital Ocean

1. Go to [Digital Ocean Control Panel](https://cloud.digitalocean.com)
2. Navigate to **Networking** → **Domains** tab
3. Click **Add Domain** (or **Create** → **Domains/DNS**)
4. Enter your domain name (e.g., `example.com`)
5. Click **Add Domain**

#### 2. Create A Record

1. On the domain's DNS records page, click **Create a record**
2. Select **A** as the record type
3. Configure the A record:
   - **Type**: A
   - **Hostname**: `@` (for apex domain) or `node` (for subdomain like `node.example.com`)
   - **Will Direct To**: Your Droplet's public IP address
   - **TTL**: 3600 (default)
3. Click **Create Record**

#### 3. Update Domain Registrar

At your domain registrar (e.g., Namecheap, GoDaddy, Google Domains):

1. Find the DNS/Nameserver settings
2. Change nameservers to Digital Ocean's:
   ```
   ns1.digitalocean.com
   ns2.digitalocean.com
   ns3.digitalocean.com
   ```
3. Save changes

> **Note**: DNS propagation can take 24-72 hours, though it's usually much faster (within a few hours).

### Option B: Use Your Current DNS Provider

If you prefer to keep your current DNS provider:

1. Log in to your DNS provider's control panel
2. Navigate to DNS management for your domain
3. Create an A record:
   - **Name/Host**: `@` (apex domain) or `node` (subdomain)
   - **Type**: A
   - **Value/Points To**: Your Droplet's public IP
   - **TTL**: 3600 (or default)
4. Save the record

### Verify DNS Configuration

```bash
# Wait a few minutes after creating the record, then test:
nslookup your-domain.com

# Or using dig:
dig your-domain.com A +short

# Expected output: Your Droplet's public IP address
```

### Updated Endpoint URL

With DNS configured, your endpoint becomes:

```
http://your-domain.com:8484
```

Or for a subdomain:
```
http://node.your-domain.com:8484
```

## Step 6: Using Your Node Endpoint with SDKs

Now that your node is accessible, you can use it with the TRUF.NETWORK SDKs.

### TypeScript SDK Example

```typescript
import { NodeTNClient, StreamId, EthereumAddress } from '@trufnetwork/sdk-js';
import { Wallet } from 'ethers';

// Initialize client with your node endpoint
const wallet = new Wallet(process.env.PRIVATE_KEY);
const client = new NodeTNClient({
  endpoint: 'http://YOUR_DROPLET_IP:8484', // or your domain
  signerInfo: {
    address: wallet.address,
    signer: wallet,
  },
  chainId: 'tn-v2.1',
});

// Query stream data
const streamAction = client.loadAction();
const records = await streamAction.getRecord({
  stream: {
    streamId: StreamId.fromString('st...').throw(),
    dataProvider: EthereumAddress.fromString('0x...').throw(),
  },
});
```

### Go SDK Example

```go
import (
    "context"
    "github.com/trufnetwork/kwil-db/core/crypto"
    "github.com/trufnetwork/kwil-db/core/crypto/auth"
    "github.com/trufnetwork/sdk-go/core/tnclient"
    "github.com/trufnetwork/sdk-go/core/types"
)

ctx := context.Background()

// Set up signer
pk, _ := crypto.Secp256k1PrivateKeyFromHex("your-private-key")
signer := &auth.EthPersonalSigner{Key: *pk}

// Connect to your node
tnClient, err := tnclient.NewClient(
    ctx,
    "http://YOUR_DROPLET_IP:8484", // or your domain
    tnclient.WithSigner(signer),
)

// Query stream data
composedActions, _ := tnClient.LoadComposedActions()
result, err := composedActions.GetRecord(ctx, types.GetRecordInput{
    DataProvider: "0x...",
    StreamId:     "st...",
})
```

## Security Best Practices

### PostgreSQL Security

**CRITICAL**: Never expose PostgreSQL port 5432 to the internet.

1. **Verify PostgreSQL is bound to localhost only**:
   ```bash
   sudo ss -tulpn | grep 5432
   # Should show 127.0.0.1:5432, NOT 0.0.0.0:5432
   ```

2. **If using UFW (Uncomplicated Firewall) on the Droplet**:
   ```bash
   # Ensure port 5432 is blocked from external access
   sudo ufw deny 5432/tcp

   # Verify UFW status
   sudo ufw status
   ```

3. **Docker users**: Ensure PostgreSQL container uses localhost binding:
   ```bash
   # Correct (localhost binding)
   docker run -p 127.0.0.1:5432:5432 ...

   # WRONG (exposed to internet)
   docker run -p 5432:5432 ...
   ```

### RPC Private Mode

For production deployments, consider enabling RPC private mode for enhanced security:

1. Edit your `config.toml`:
   ```toml
   [rpc]
   # Enforce data privacy: authenticate JSON-RPC call requests
   private = true
   ```

2. Restart your node:
   ```bash
   # For systemd
   sudo systemctl restart kwild

   # For Docker
   docker restart <your-container-name>
   # Or if using Docker Compose: docker compose restart
   ```

For more details, see the [Kwil Private RPC documentation](http://docs.kwil.com/docs/node/private-rpc).

### SSH Security

1. **Use SSH keys instead of passwords**:
   ```bash
   # Generate SSH key on your local machine (if you haven't already)
   ssh-keygen -t ed25519 -C "your_email@example.com"

   # Add to Digital Ocean Droplet
   ssh-copy-id root@YOUR_DROPLET_IP
   ```

2. **Disable password authentication**:
   ```bash
   # Edit SSH config
   sudo nano /etc/ssh/sshd_config

   # Set the following:
   PasswordAuthentication no

   # Restart SSH service
   sudo systemctl restart sshd
   ```

3. **Restrict SSH access in firewall**: Configure your firewall to only allow SSH from your IP address.

### Regular Updates

Keep your system and node software up to date:

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Update node binary (if building from source)
cd ~/node
git pull
task build
sudo mv .build/kwild /usr/local/bin/kwild
sudo systemctl restart kwild

# Or download latest release
# See: https://github.com/trufnetwork/node/releases
```

## Troubleshooting

### Port Not Accessible

**Problem**: `curl http://YOUR_DROPLET_IP:8484/api/v1/health` times out or connection refused.

**Solutions**:

1. **Check if kwild is running**:
   ```bash
   sudo systemctl status kwild
   # Or for Docker: docker ps
   ```

2. **Verify kwild is listening on the correct port**:
   ```bash
   sudo ss -tulpn | grep 8484
   ```

3. **Check firewall rules**:
   - Digital Ocean Cloud Firewall: Verify port 8484 is in inbound rules
   - UFW (if enabled): `sudo ufw status` and ensure 8484 is allowed

4. **Check kwild configuration**:
   ```bash
   # Print current config
   kwild print-config

   # Look for rpc.listen_addr (should be 0.0.0.0:8484 for external access)
   ```

### DNS Not Resolving

**Problem**: Domain doesn't resolve to your Droplet's IP.

**Solutions**:

1. **Check if DNS propagation is complete**:
   ```bash
   dig your-domain.com @8.8.8.8 A +short
   ```

2. **Verify nameservers** (if using Digital Ocean DNS):
   ```bash
   dig your-domain.com NS +short
   # Should show ns1.digitalocean.com, ns2.digitalocean.com, etc.
   ```

3. **Wait for propagation**: DNS changes can take up to 72 hours, though usually much faster.

### Node Not Syncing

**Problem**: Node shows `syncing: true` for extended periods.

**Solutions**:

1. **Check peer connections**:
   ```bash
   kwild admin status | grep -i peer
   ```

2. **Verify port 6600 is accessible**:
   - Check firewall allows inbound on 6600
   - Test: `nc -zv YOUR_DROPLET_IP 6600` from external machine

3. **Check node logs**:
   ```bash
   # For systemd
   sudo journalctl -u kwild -f

   # For Docker (replace with your container name)
   docker logs -f <container-name>
   ```

### MCP Server Not Accessible

**Problem**: MCP server not reachable on port 8000.

**Solutions**:

1. **Verify MCP is enabled and running**:
   ```bash
   # For Docker deployment
   docker ps | grep mcp

   # Check logs (replace with your container name)
   docker logs <mcp-container-name>
   ```

2. **Check firewall allows port 8000**:
   - Verify Digital Ocean Cloud Firewall has inbound rule for port 8000
   - Test: `nc -zv YOUR_DROPLET_IP 8000`

3. **Verify MCP server configuration**:
   ```bash
   # MCP should be listening on 0.0.0.0:8000 for external access
   sudo ss -tulpn | grep 8000
   ```

## Next Steps

1. **Monitor your node**: Regularly check sync status with `kwild admin status`
2. **Enable extensions**: Consider enabling `tn_cache` for better performance (see [Node Operator Guide](./node-operator-guide.md#cache-extension-tn_cache))

## Additional Resources

- **[Node Operator Guide](./node-operator-guide.md)**: Complete manual setup instructions
- **[Deployment Options Comparison](./deployment-options.md)**: Compare all deployment methods
- **[Digital Ocean Documentation](https://docs.digitalocean.com/)**: Official Digital Ocean resources
- **[TRUF.NETWORK SDKs](https://github.com/trufnetwork)**: SDK repositories and examples

## Support

Need help? We're here to assist:

- **GitHub Issues**: [Node Repository](https://github.com/trufnetwork/node/issues)
- **Documentation**: [Full documentation](https://docs.truf.network)

---

**Successfully deployed?** Consider sharing your node in the [Available Nodes List](https://github.com/trufnetwork/truf-node-operator/blob/main/configs/network/v2/network-nodes.csv) to help grow the network!
