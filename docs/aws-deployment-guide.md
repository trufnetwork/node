# AWS Deployment Guide

This guide helps you deploy a TRUF.NETWORK node on AWS and expose it to external clients. Follow these steps to configure networking, security groups, and optional DNS settings for your node.

## Prerequisites

- **AWS Account** with EC2 access
- **EC2 instance** running your TN node (see options below)
- **Node running** and syncing with the network
- **Basic familiarity** with AWS Console

> **New to AWS?** Check out the [AWS EC2 Getting Started Guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html) first.

## Deployment Options

Choose how you want to deploy your TN node on AWS:

- **[AMI Deployment](./ami-deployment-guide.md)** (Recommended) - Pre-configured Amazon Machine Image for 5-10 minute setup
- **[Manual Setup](./node-operator-guide.md)** - Full control with manual installation on any EC2 instance

> **Already deployed using AMI?** Your security groups are likely configured. Skip to [Step 2](#step-2-find-your-ec2-instances-public-ip-address) to find your endpoint URL.

## Overview

This guide focuses on the networking configuration required to make your node accessible to external applications and the network. You'll configure:

1. **Security Group rules** to allow necessary ports
2. **Public IP discovery** for SDK integration
3. **Optional Route53 DNS setup** for custom domains
4. **Connectivity testing** to verify your setup

## Port Requirements

Before configuring your security groups, understand which ports your node needs:

### Required Ports

| Port | Protocol | Purpose | Source |
|------|----------|---------|--------|
| **6600** | TCP | P2P node communication | 0.0.0.0/0 (All IPv4) |
| **8484** | TCP | RPC service for queries | 0.0.0.0/0 (All IPv4) |

### Optional Ports

| Port | Protocol | Purpose | Source |
|------|----------|---------|--------|
| **8000** | TCP | MCP Server for AI integration | Your IP or 0.0.0.0/0 |
| **22** | TCP | SSH access | Your IP (recommended) |

### Ports to NEVER Expose

| Port | Protocol | Why |
|------|----------|-----|
| **5432** | TCP | PostgreSQL database - **CRITICAL SECURITY RISK** if exposed |

> **Important Notes:**
> - **Port 6600**: Enables two-way P2P communication. Your node can sync without this (via outbound connections), but opening it helps network health by accepting incoming peer connections.
> - **Port 8484**: Required if you want users/applications to query data from your node.
> - **Port 8000**: Only needed for MCP/AI integration (like Claude Code).
> - **Port 5432**: Should ONLY bind to localhost (127.0.0.1). Never allow external access.

> **Security Best Practice**: When adding SSH access (port 22), use your specific IP address (e.g., `203.0.113.1/32`) instead of `0.0.0.0/0`. Allowing SSH from anywhere significantly increases security risk.

## Step 1: Configure AWS Security Groups

Security groups act as virtual firewalls for your EC2 instances, controlling inbound and outbound traffic.

### Option A: Add Rules to Existing Security Group (Most Common)

If your EC2 instance already has a security group:

1. **Navigate to Security Groups**:
   - Go to [AWS EC2 Console](https://console.aws.amazon.com/ec2/)
   - In the left navigation pane, under **Network & Security**, click **Security Groups**
   - Find and select the security group attached to your EC2 instance

2. **Add Inbound Rules**:
   - Click the **Inbound rules** tab
   - Click **Edit inbound rules**
   - Click **Add rule** for each port you need to open

3. **Configure Required Rules**:

   **Rule 1: P2P Communication**
   - **Type**: Custom TCP
   - **Port range**: `6600`
   - **Source type**: Anywhere-IPv4
   - **Source**: `0.0.0.0/0`
   - **Description**: `TN P2P communication`

   **Rule 2: RPC Service**
   - **Type**: Custom TCP
   - **Port range**: `8484`
   - **Source type**: Anywhere-IPv4
   - **Source**: `0.0.0.0/0`
   - **Description**: `TN RPC service`

   **Rule 3: SSH Access** (if not already present)
   - **Type**: SSH
   - **Port range**: `22` (auto-filled)
   - **Source type**: My IP (recommended) or Custom
   - **Source**: Your IP address (e.g., `203.0.113.1/32`) or `0.0.0.0/0` (less secure)
   - **Description**: `SSH access`

   **Optional Rule 4: MCP Server** (only if using AI integration)
   - **Type**: Custom TCP
   - **Port range**: `8000`
   - **Source type**: My IP or Anywhere-IPv4
   - **Source**: Your IP address or `0.0.0.0/0` (depending on your use case)
   - **Description**: `MCP Server for AI`

4. **Save Rules**:
   - Click **Save rules**
   - Changes take effect immediately

### Option B: Create a New Security Group

If you prefer to create a dedicated security group for your TN node:

1. Navigate to **EC2 Console** → **Network & Security** → **Security Groups**
2. Click **Create security group**
3. Configure:
   - **Security group name**: `tn-node-sg` (or your preferred name)
   - **Description**: `Security group for TRUF.NETWORK node`
   - **VPC**: Select the same VPC as your EC2 instance
4. Add the inbound rules from Option A above
5. Click **Create security group**
6. Attach the security group to your EC2 instance:
   - Go to **EC2 Dashboard** → **Instances**
   - Select your instance
   - **Actions** → **Security** → **Change security groups**
   - Select your new security group
   - Click **Save**

### Verify Security Group Configuration

```bash
# SSH into your EC2 instance
ssh -i ~/.ssh/your-key.pem ubuntu@your-instance-ip

# Check that ports 6600 and 8484 are listening
sudo ss -tulpn | grep -E '6600|8484'

# Expected output should show kwild listening on these ports:
# tcp   LISTEN 0  4096  0.0.0.0:6600  0.0.0.0:*
# tcp   LISTEN 0  4096  0.0.0.0:8484  0.0.0.0:*
```

## Step 2: Find Your EC2 Instance's Public IP Address

Your node's endpoint URL requires the EC2 instance's public IP address.

### Via AWS Console

1. Go to [AWS EC2 Console](https://console.aws.amazon.com/ec2/)
2. In the left navigation, click **Instances**
3. Find your TN node instance
4. Look at the **Public IPv4 address** column or in the **Details** tab

### Via AWS CLI

If you have [AWS CLI](https://aws.amazon.com/cli/) installed and configured:

```bash
# List all instances with their public IPs
aws ec2 describe-instances \
  --query 'Reservations[*].Instances[*].[InstanceId,PublicIpAddress,Tags[?Key==`Name`].Value|[0]]' \
  --output table

# Get specific instance public IP by instance ID
aws ec2 describe-instances \
  --instance-ids i-1234567890abcdef0 \
  --query 'Reservations[*].Instances[*].PublicIpAddress' \
  --output text
```

### Via Command Line (SSH into Instance)

```bash
# Method 1: Query external service
curl -4 ifconfig.co

# Method 2: Using AWS EC2 metadata service
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4

# Method 3: Using dig
dig +short myip.opendns.com @resolver1.opendns.com
```

### Using Elastic IP (Optional)

For a persistent IP address that doesn't change if you stop/start your instance:

1. Navigate to **EC2 Console** → **Network & Security** → **Elastic IPs**
2. Click **Allocate Elastic IP address**
3. Click **Allocate**
4. Select the newly allocated IP
5. Click **Actions** → **Associate Elastic IP address**
6. Select your EC2 instance
7. Click **Associate**

> **Note**: Elastic IPs are free when associated with a running instance, but you're charged $0.005/hour ($3.60/month) for unassociated Elastic IPs to prevent IP hoarding.

## Step 3: Construct Your Node Endpoint URL

Once you have your public IP address, your node endpoint follows this format:

```
http://YOUR_EC2_IP:8484
```

**Example:**
If your EC2 instance's public IP is `54.210.123.45`, your endpoint is:
```
http://54.210.123.45:8484
```

## Step 4: Test Node Connectivity

Verify that your node is accessible from external clients.

### Test Health Endpoint

From your **local machine** (not the EC2 instance):

```bash
# Test health check endpoint
curl http://YOUR_EC2_IP:8484/api/v1/health

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
# SSH into your EC2 instance
ssh -i ~/.ssh/your-key.pem ubuntu@YOUR_EC2_IP

# If using AMI deployment (Docker):
docker exec tn-node ./kwild admin status

# If using manual setup (systemd):
kwild admin status
```

### Test P2P Connectivity

```bash
# From your local machine, test if port 6600 is accessible
nc -zv YOUR_EC2_IP 6600

# Expected output:
# Connection to YOUR_EC2_IP 6600 port [tcp/*] succeeded!
```

If this test fails, verify your security group rules are correctly configured.

## Step 5: Configure DNS with Route53 (Optional)

Using a custom domain instead of an IP address makes your node endpoint more memorable and portable.

### Prerequisites

- A domain name (can be registered through Route53 or external registrar)
- Access to your domain's DNS settings

### Option A: Use Route53 (Recommended for AWS)

#### 1. Create Hosted Zone (if not already exists)

1. Go to [Route53 Console](https://console.aws.amazon.com/route53/)
2. In the left navigation, click **Hosted zones**
3. Click **Create hosted zone**
4. Enter your domain name (e.g., `example.com`)
5. Choose **Public hosted zone**
6. Click **Create hosted zone**
7. Note the **NS (Name Server) records** - you'll need these for your domain registrar

#### 2. Update Domain Registrar (if domain not registered with AWS)

If your domain is registered outside of AWS:

1. Log in to your domain registrar (e.g., GoDaddy, Namecheap, Google Domains)
2. Find DNS/Nameserver settings
3. Replace your current nameservers with Route53's NS records:
   ```
   ns-1234.awsdns-01.org
   ns-5678.awsdns-02.co.uk
   ns-9012.awsdns-03.com
   ns-3456.awsdns-04.net
   ```
   *(Your actual NS records will be different - use the ones from your hosted zone)*
4. Save changes

> **Note**: DNS propagation can take 24-72 hours, though it's usually much faster (within a few hours).

#### 3. Create A Record

1. In Route53 console, select your **Hosted zone**
2. Click **Create record**
3. Configure the record:
   - **Record name**: Leave empty for apex domain (e.g., `example.com`) or enter subdomain (e.g., `node` for `node.example.com`)
   - **Record type**: Select **A - Routes traffic to an IPv4 address**
   - **Value**: Enter your EC2 instance's public IP address
   - **TTL**: `300` (5 minutes - recommended) or `3600` (1 hour)
   - **Routing policy**: Simple routing
4. Click **Create records**

#### 4. Create Alias Record (Alternative for EC2 with Elastic IP)

If you want more resilience and are using an Elastic IP:

1. Click **Create record**
2. Enable **Alias** toggle
3. Configure:
   - **Record name**: Leave empty or enter subdomain
   - **Record type**: **A**
   - **Route traffic to**:
     - Select **Alias to Elastic IP address**
     - Choose your **AWS Region**
     - Select your **Elastic IP**
   - **Routing policy**: Simple routing
4. Click **Create records**

> **Advantage of Alias Records**: No charge for queries, automatic updates if resource changes, better AWS integration.

### Option B: Use External DNS Provider

If you prefer to keep your current DNS provider:

1. Log in to your DNS provider's control panel
2. Navigate to DNS management for your domain
3. Create an A record:
   - **Name/Host**: `@` (apex domain) or `node` (subdomain)
   - **Type**: A
   - **Value/Points To**: Your EC2 instance's public IP
   - **TTL**: 300 or 3600 seconds
4. Save the record

### Verify DNS Configuration

```bash
# Wait a few minutes after creating the record, then test:
nslookup your-domain.com

# Or using dig:
dig your-domain.com A +short

# Expected output: Your EC2 instance's public IP address
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
  endpoint: 'http://YOUR_EC2_IP:8484', // or your domain
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
    "http://YOUR_EC2_IP:8484", // or your domain
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

2. **Ensure security groups don't allow port 5432**:
   - Check your security group inbound rules
   - Port 5432 should NOT be in the list
   - If present, remove it immediately

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
   # If using AMI deployment (Docker):
   sudo systemctl restart tn-node
   # Or: docker restart tn-node

   # If using manual setup (systemd):
   sudo systemctl restart kwild
   ```

For more details, see the [Kwil Private RPC documentation](http://docs.kwil.com/docs/node/private-rpc).

### SSH Security

1. **Use SSH keys instead of passwords**:
   ```bash
   # AWS automatically uses SSH keys for EC2 instances
   # Ensure your key pair is secure and not shared
   chmod 400 ~/.ssh/your-key.pem
   ```

2. **Restrict SSH access in security group**: Configure your security group to only allow SSH from your IP address (not `0.0.0.0/0`).

3. **Use AWS Systems Manager Session Manager** (Alternative):
   - Eliminates need to expose port 22
   - Provides secure browser-based SSH
   - See [AWS Session Manager documentation](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html)

### AWS-Specific Security

1. **Enable VPC Flow Logs**: Monitor network traffic for unusual patterns
2. **Use AWS CloudWatch**: Set up alerts for unusual access patterns
3. **Enable CloudTrail**: Audit all AWS API calls for security monitoring
4. **Consider AWS WAF**: If using Application Load Balancer for HTTPS termination
5. **Regular security group audits**: Review and remove unnecessary rules

### Regular Updates

Keep your system and node software up to date:

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y
```

**If using AMI deployment**:
```bash
# Update Docker images
sudo tn-node-update
# Or manually: cd /opt/tn && sudo -u tn docker compose pull && sudo systemctl restart tn-node
```

**If using manual setup**:
```bash
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

**Problem**: `curl http://YOUR_EC2_IP:8484/api/v1/health` times out or connection refused.

**Solutions**:

1. **Check if kwild is running**:
   ```bash
   # If using AMI deployment (Docker):
   docker ps
   sudo systemctl status tn-node

   # If using manual setup (systemd):
   sudo systemctl status kwild
   ```

2. **Verify kwild is listening on the correct port**:
   ```bash
   sudo ss -tulpn | grep 8484
   ```

3. **Check security group rules**:
   - Go to EC2 Console → Security Groups
   - Verify port 8484 is in inbound rules with source `0.0.0.0/0`
   - Check that the security group is attached to your instance

4. **Check Network ACLs** (if using custom VPC):
   - Go to VPC Console → Network ACLs
   - Verify inbound/outbound rules allow port 8484

5. **Check kwild configuration**:
   ```bash
   # If using AMI deployment (Docker):
   docker exec tn-node ./kwild print-config

   # If using manual setup (systemd):
   kwild print-config

   # Look for rpc.listen_addr (should be 0.0.0.0:8484 for external access)
   ```

### DNS Not Resolving

**Problem**: Domain doesn't resolve to your EC2 instance's IP.

**Solutions**:

1. **Check if DNS propagation is complete**:
   ```bash
   dig your-domain.com @8.8.8.8 A +short
   ```

2. **Verify nameservers** (if using Route53):
   ```bash
   dig your-domain.com NS +short
   # Should show Route53 nameservers (ns-*.awsdns-*.*)
   ```

3. **Check Route53 hosted zone**:
   - Verify A record exists and points to correct IP
   - Check record name matches what you're querying

4. **Wait for propagation**: DNS changes can take up to 72 hours, though usually much faster.

### Node Not Syncing

**Problem**: Node shows `syncing: true` for extended periods.

**Solutions**:

1. **Check peer connections**:
   ```bash
   # If using AMI deployment (Docker):
   docker exec tn-node ./kwild admin status | grep -i peer

   # If using manual setup (systemd):
   kwild admin status | grep -i peer
   ```

2. **Verify port 6600 is accessible**:
   - Check security group allows inbound on 6600
   - Test: `nc -zv YOUR_EC2_IP 6600` from external machine

3. **Check node logs**:
   ```bash
   # If using AMI deployment (Docker):
   docker logs -f tn-node
   # Or: sudo journalctl -u tn-node -f

   # If using manual setup (systemd):
   sudo journalctl -u kwild -f
   ```

### Elastic IP Not Working

**Problem**: Elastic IP doesn't connect to instance.

**Solutions**:

1. **Verify Elastic IP is associated**:
   - EC2 Console → Elastic IPs
   - Check **Associated instance ID** column

2. **Check instance state**: Instance must be running

3. **Security groups follow Elastic IP**: Security groups remain attached even after Elastic IP association

### High AWS Costs

**Problem**: Unexpected AWS charges.

**Common Causes**:

1. **Unassociated Elastic IPs**: $0.005/hour each - release unused IPs
2. **Data transfer**: Significant inbound/outbound traffic - monitor CloudWatch metrics
3. **Instance type**: Consider right-sizing (t3.medium vs t3.large)
4. **EBS volumes**: gp3 is more cost-effective than gp2
5. **Snapshots**: Review and delete old EBS snapshots

**Cost optimization**:
- Use [AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/)
- Set up [AWS Budgets](https://aws.amazon.com/aws-cost-management/aws-budgets/) with alerts
- Consider [Reserved Instances](https://aws.amazon.com/ec2/pricing/reserved-instances/) for long-term nodes

## Next Steps

1. **Monitor your node**: Regularly check sync status
   - AMI deployment: `docker exec tn-node ./kwild admin status`
   - Manual setup: `kwild admin status`
2. **Enable extensions**: Consider enabling `tn_cache` for better performance (see [Node Operator Guide](./node-operator-guide.md#cache-extension-tn_cache) or [AMI Deployment Guide](./ami-deployment-guide.md#advanced-configuration))

## Additional Resources

- **[AMI Deployment Guide](./ami-deployment-guide.md)**: Quick 5-10 minute AWS setup
- **[Node Operator Guide](./node-operator-guide.md)**: Complete manual setup instructions
- **[Deployment Options Comparison](./deployment-options.md)**: Compare all deployment methods
- **[AWS EC2 Documentation](https://docs.aws.amazon.com/ec2/)**: Official AWS resources
- **[AWS Route53 Documentation](https://docs.aws.amazon.com/route53/)**: DNS management
- **[TRUF.NETWORK SDKs](https://github.com/trufnetwork)**: SDK repositories and examples

## Support

Need help? We're here to assist:

- **GitHub Issues**: [Node Repository](https://github.com/trufnetwork/node/issues)
- **Documentation**: [Full documentation](https://docs.truf.network)

---

**Successfully deployed?** Consider sharing your node in the [Available Nodes List](https://github.com/trufnetwork/truf-node-operator/blob/main/configs/network/v2/network-nodes.csv) to help grow the network!
