# TN-DB Infrastructure

This project contains the AWS CDK infrastructure code for deploying TN-DB nodes, a Kwil Gateway, and an Indexer.

## Overview

The infrastructure can be deployed in two ways:
1. Auto-generated configuration
2. Pre-configured setup

The `cdk.json` file tells the CDK toolkit how to execute your app.

## High-level Constructs
We have extracted three reusable L3 constructs under [deployments/infra/lib/constructs](https://github.com/trufnetwork/node/tree/main/deployments/infra/lib/constructs) to simplify stack logic:

- **ValidatorSet**: provisions a group of TN validator EC2 instances with Elastic IPs, DNS A records, one IAM role per node, and a shared security group. Each node role may read only that node's key in SSM.
  - Props: `Vpc`, `HostedDomain`, `Peers`, `NodeKeys` (public keys only), `GenesisAsset`, `KeyPair`, `Assets`, `InitElements` (e.g. custom EC2 user-data such as observer bootstrap), `CDKParams`, `Secrets` (where the node keys are in SSM)
  - Outputs: `Nodes []tn.TNInstance` (each with its own `Role`), `SecurityGroup awsec2.SecurityGroup`
  - Import:
    ```go
    import "github.com/trufnetwork/node/infra/lib/constructs/validator_set"
    ```
  - Usage:
    ```go
    vs := validator_set.NewValidatorSet(stack, "ValidatorSet", &validator_set.ValidatorSetProps{ ... })
    ```

- **KwilCluster**: provisions a Kwil Gateway and an Indexer.
  - Props: `Vpc`, `HostedDomain`, `Cert` (currently passed but not used for fronting), `CorsOrigins`, `Secrets` (where the session secret is in SSM), `ChainId`, `Validators`, `InitElements`, `KGWDirAsset`, `KGWBinaryAsset`, `IndexerDirAsset`
  - Outputs: `Gateway kwil_gateway.KGWInstance`, `Indexer kwil_indexer.IndexerInstance`
  - Import:
    ```go
    import "github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
    ```
  - Usage:
    ```go
    kc := kwil_cluster.NewKwilCluster(stack, "KwilCluster", &kwil_cluster.KwilClusterProps{ ... })
    ```

- **ObservabilitySuite**: deploys a Vector EC2 instance for logs/metrics ingestion and writes SSM parameters for observer configuration.
  - Props: `Vpc`, `ValidatorSg`, `GatewaySg`, `ParamsPrefix`
  - Outputs: `VectorInstance awsec2.Instance`, `ParamPaths []*string`
  - Import:
    ```go
    import "github.com/trufnetwork/node/infra/lib/constructs/observability_suite"
    ```
  - Usage:
    ```go
    obs := observability_suite.NewObservabilitySuite(stack, "ObservabilitySuite", &observability_suite.ObservabilitySuiteProps{ ... })
    ```

- **Fronting**: pluggable edge proxy for API routing & TLS termination.
  - Props: `HostedZone`, `Certificate`, `KGWEndpoint`, `IndexerEndpoint`, `RecordName`
  - Import:
    ```go
    import fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
    ```
  - Usage:
    ```go
    ag := fronting.NewApiGatewayFronting()
    apiDomain := ag.AttachRoutes(stack, "APIGateway", &fronting.FrontingProps{
      HostedZone:      zone,
      Certificate:     cert,
      KGWEndpoint:     kc.Gateway.InstanceDnsName,
      IndexerEndpoint: kc.Indexer.InstanceDnsName,
      RecordName:      jsii.String("api."+*prefix),
    })
    ```

## Deployment Methods

### Choosing the front-end

| Context key    | Values             | Default |
|----------------|--------------------|---------|
| `frontingType` | `api`, `cloudfront`| `api`   |

* `api` – deploys an **AWS HTTP API** with a **regional ACM certificate** generated automatically in the same region as the stack (e.g. us-east-2).
* `cloudfront` – retains the legacy CloudFront distribution with an edge certificate in us-east-1.

```bash
cdk deploy --context frontingType=api            # simplest, scale-to-zero, no hourly ALB
cdk deploy --context frontingType=cloudfront    # only if you really need CF
```

### 1. Auto-generated Configuration

This method dynamically generates the TN node configuration during deployment. It deploys both the launch templates and the EC2 instances from these templates.

Store the node keys and the session secret first (see [Node Keys and the Gateway Session Secret](#node-keys-and-the-gateway-session-secret)), then pass the node IDs the script printed.

#### Example Command:

```bash
KWILD_CLI_PATH=kwild \
CHAIN_ID=truflation-dev \
DB_OWNER=<DB-OWNER-ADDRESS> \
NODE_PUBLIC_KEYS="<node-1 ID>,<node-2 ID>" \
CDK_DOCKER="<YOUR-DIRECTORY>/tn/deployments/infra/buildx.sh" \
cdk deploy --profile <YOUR-AWS-PROFILE> --all --asset-parallelism=false --notices false \
  -c stage=dev \
  -c devPrefix=<YOUR-DEV-PREFIX>
```

### 2. Pre-configured Setup

This method uses a pre-existing genesis file and the public keys of the TN nodes. It deploys only the launch templates, not the instances. Store each node's key with `put-node-key` and the session secret with `put-session-secret` first (see [Node Keys and the Gateway Session Secret](#node-keys-and-the-gateway-session-secret)).

#### Example Command:

```bash
KWILD_CLI_PATH=kwild \
CHAIN_ID=truflation-dev \
CDK_DOCKER="<YOUR-DIRECTORY>/tn/deployments/infra/buildx.sh" \
NODE_PUBLIC_KEYS="<node-1 ID>,<node-2 ID>,<node-3 ID>" \
GENESIS_PATH="/path/to/genesis.json" \
cdk deploy --profile <YOUR-AWS-PROFILE> TN-From-Config* TN-Cert* \
  -c stage=dev \
  -c devPrefix=<YOUR-DEV-PREFIX>
```

## Redeploying Instances from Launch Templates

In our stack, the launch templates for the Kwil Gateway (kgw) and Indexer instances are created. For the **TnAutoStack**, the instances themselves are also automatically provisioned. For the **TnFromConfigStack**, the instances are **not** automatically provisioned. This section primarily applies to the `TnFromConfigStack` or manual redeployment scenarios.

Below are the steps to redeploy an instance using the launch templates:

1. **Deploy a New Instance from the Launch Template**
   
   Use the AWS Management Console, AWS CLI, or infrastructure as code tools to launch a new EC2 instance using the existing launch template for either the kgw or Indexer.

2. **Detach the Elastic IP from the Running Instance**
   
   Identify the Elastic IP (EIP) associated with the current instance and detach it.

3. **Attach the Elastic IP to the New Instance**
   
   Associate the detached EIP with the newly launched instance.

4. **Delete the Old Instance**
   
   Once the EIP is successfully attached to the new instance and you've verified its operation, terminate the old instance to avoid unnecessary costs.

## Upgrading Nodes

For the prod environment, which uses the pre-configured setup, upgrading a node involves the following steps:

1. Deploy the stack to update the launch template with the new image:

    ```bash
    cdk deploy --profile <YOUR-AWS-PROFILE> TN-From-Config* TN-Cert*
    ```

2. After deployment, SSH into the instance you want to upgrade.

3. Pull the latest image from the ECR repository (you may need to login to the ECR repository first, please see the correct commands at the AWS console)

    ```bash
    docker pull <latest-image>
    ```

4. Tag the image as `tn:local`

    ```bash
    docker tag <latest-image> tn:local
    ```

5. Restart the systemd service that runs the TN node:

    ```bash
    sudo systemctl restart tn-db-app.service
    ```
This process ensures that your node is running the latest version of the software while maintaining the pre-configured setup.

## Environment Variables

- `KWILD_CLI_PATH`: Path to the `kwild` CLI binary (use `kwild` if it's in your PATH)
- `CHAIN_ID`: Chain ID for the Kwil network
- `CDK_DOCKER`: Path to docker buildx script
- `NODE_PUBLIC_KEYS`: Comma-separated node IDs of the validators (`<public key hex>#<key type>`, as printed by `kwild key info` or `scripts/ssm-secrets.sh`), in node order: the first entry is node-1. A bare public key means `secp256k1`. The auto-generated stack needs exactly `numOfNodes` entries (2 in `cdk.json`).
- `DB_OWNER`: Database owner written into the generated genesis (only for auto-generated configuration)
- `GENESIS_PATH`: Path to the genesis file (only for pre-configured setup)

Synth takes no private key and no session secret. They are in SSM Parameter Store, see below.

## Node Keys and the Gateway Session Secret

Each node reads its own `nodekey.json` from an SSM SecureString parameter at first boot, and the gateway reads its
session secret from SSM every time its service starts. The values never appear in the CloudFormation template, in
user data or in a CDK asset. The parameters are:

| Parameter | Value | Read by |
|-----------|-------|---------|
| `<prefix>/node/<i>/nodekey` | `nodekey.json` of node `i` (0-based: node-1 is `0`) | that node only |
| `<prefix>/kgw/session_secret` | gateway session secret | the gateway |

`<prefix>` is `/tsn/<stage>`, or `/tsn/<stage>/<devPrefix>` when the stack has a `devPrefix`. Two optional CDK
context keys change this:

| Context key | Meaning | Default |
|-------------|---------|---------|
| `secretsPrefix` | SSM path prefix, such as `/tsn/prod` | `/tsn/<stage>[/<devPrefix>]` |
| `secretsKmsKeyArn` | customer managed KMS key ARN that encrypts the parameters | the AWS managed `aws/ssm` key |

Of these parameters, each instance role may read only its own (`ssm:GetParameter` on that one parameter), plus
`kms:Decrypt` through SSM when `secretsKmsKeyArn` is set.

CloudFormation cannot create SecureString parameters, so `scripts/ssm-secrets.sh` stores them before the first
deploy. It reads every value from a file or stdin and never prints one:

```bash
# New stack: generate the node keys in SSM and print NODE_PUBLIC_KEYS
scripts/ssm-secrets.sh --stage dev --dev-prefix <YOUR-DEV-PREFIX> gen-node-keys 2

# Existing node key (a nodekey.json, or a private key in hex): prints the node ID
scripts/ssm-secrets.sh --stage prod put-node-key 0 ./node-1/nodekey.json

# Gateway session secret
openssl rand -hex 32 | scripts/ssm-secrets.sh --stage prod put-session-secret -

# Show the parameter names only, with no AWS call
scripts/ssm-secrets.sh --stage prod --dry-run gen-node-keys 3
```

Pass the same `--prefix` and `--kms-key-arn` as the stack's `secretsPrefix` and `secretsKmsKeyArn`, and `--region`
when the AWS CLI's default region is not the stack's. The script stops before writing when a parameter already
exists. `--overwrite` replaces it. Run `scripts/ssm-secrets.sh --help` for every option. The script needs the AWS
CLI and `jq`, and `kwild` (or `KWILD_CLI_PATH`) for the node key commands. The caller needs `ssm:PutParameter` on
the parameters, `ssm:DescribeParameters`, and `kms:Encrypt` on a customer managed key.

A node reads its key only at first boot, so a running node keeps the key it started with. A new session secret
takes effect when the gateway service restarts (`sudo systemctl restart kgw`).

## AWS Profile

Use the `--profile` option to specify your AWS profile.

## Deployment Stage

- Stacks now read `stage` and `devPrefix` from CDK context (not CloudFormation parameters).
- Example: `cdk deploy -c stage=dev -c devPrefix=<prefix>`.

## RPC Port Sources

The validators' security group opens the TN RPC port (8484) only to the sources listed for the stack's
`stackSuffix` under `rpcSources` in `cdk.json`, each an IPv4 CIDR mapped to the host behind it:

```json
"rpcSources": {
  "prod": {
    "203.0.113.10/32": "mainnet gateway"
  }
}
```

A stack whose suffix has no entry keeps the port open to any address, except on the `prod` stage, where the synth
stops instead. An empty entry (`{}`) closes the port. CDK drops `null` values from context, so a `null` entry counts
as missing and a `null` host name drops its source. The host name goes into the rule description, so it may hold only
letters, digits, spaces and `._-:/()#,@[]+=&;{}!$*`, up to 200 characters.

`lib/tn/security_group_test.go` pins the `prod` list, so change it there as well. Run `cdk diff` after changing the list.

## Useful Commands

- `cdk deploy`: Deploy this stack to your default AWS account/region
- `cdk diff`: Compare deployed stack with current state
- `cdk synth`: Emit the synthesized CloudFormation template
- `go test`: Run unit tests

## Note for Windows Users

If you're using Windows with WSL2, you may need to disable your Windows firewall to allow cdk-cli to forward the port to the docker container. Open PowerShell as an administrator and run:

```powershell
netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=22 connectaddress=localhost connectport=22
```

## Benchmark Stack

The Benchmark Stack is designed to test and measure the performance of smart contracts across multiple EC2 instance types. It deploys resources necessary for conducting benchmarks on different AWS EC2 instances to evaluate contract execution efficiency.


### Features

- Supports multiple EC2 instance types (t3.micro, t3.small, t3.medium, t3.large)
- Uses an S3 asset for the binary
- Uses S3 buckets for storing results
- Implements a Step Functions state machine to orchestrate the benchmark process
- Parallel execution of benchmarks across different instance types

### Deployment

To deploy the Benchmark Stack:


```bash
cdk deploy --profile <YOUR-AWS-PROFILE> TSN-Benchmark-Stack-<environment> --exclusively
```

Replace `<environment>` with your target environment (e.g., dev, prod).

### Usage

See [Getting Benchmarks](./docs/getting-benchmarks.md) for more information.

---

## 🚀 AMI Pipeline Infrastructure

This directory contains infrastructure for automated AMI building using AWS EC2 Image Builder. This creates a Docker-in-AMI approach where the infrastructure is baked into the AMI and applications run via Docker containers.

### Features

- **Automated AMI Building**: EC2 Image Builder pipeline for creating TrufNetwork node AMIs
- **Docker-in-AMI**: Pre-installed Docker with docker-compose for container orchestration
- **Always-Latest Strategy**: Containers pull latest images on startup
- **MCP Server Integration**: PostgreSQL MCP server for AI agent access via SSE transport
- **Current Region Distribution**: AMI distribution in deployment region (multi-region planned)
- **GitHub Actions Integration**: Automated builds triggered by releases

### Quick Start

#### Prerequisites
- AWS CLI configured with appropriate permissions
- CDK CLI installed: `npm install -g aws-cdk@latest`
- Go 1.22.x
- Docker installed and running

#### Deploy AMI Infrastructure

```bash
cd deployments/infra

# Deploy using isolated AMI CDK app (✅ Tested and Working)
cdk --app 'go run ami-cdk.go' deploy \
  --context stage=dev \
  --context devPrefix=test-$(whoami) \
  --require-approval never
```

#### Verify Deployment

```bash
# Check stack status
aws cloudformation describe-stacks \
  --stack-name AMI-Pipeline-default-Stack \
  --region us-east-2

# Get pipeline ARN
aws cloudformation describe-stacks \
  --stack-name AMI-Pipeline-default-Stack \
  --region us-east-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`AmiPipelineArnOutput`].OutputValue' \
  --output text

# Get S3 bucket name
aws cloudformation describe-stacks \
  --stack-name AMI-Pipeline-default-Stack \
  --region us-east-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`AmiArtifactsBucketOutput`].OutputValue' \
  --output text
```

#### Test AMI Functionality

```bash
# Use the latest AMI (replace with actual AMI ID from your build)
AMI_ID="ami-xxxxxxxxxxxxxxxxx"

# Launch test instance
aws ec2 run-instances \
  --image-id $AMI_ID \
  --instance-type t3.medium \
  --key-name your-key-pair \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=TRUF-Network-AMI-Test}]' \
  --region us-east-2

# SSH to instance and test
ssh ubuntu@<instance-ip>

# Configure TRUF.NETWORK node
sudo tn-node-configure --network testnet --enable-mcp

# Verify services
cd /opt/tn && docker-compose ps
curl http://localhost:8000/  # MCP server (returns 404 - normal)
```

#### Update Node Images

```bash
# On AMI instances, update to latest images
sudo update-node
```

### AMI Architecture

The AMI includes:

- **Base OS**: Ubuntu 24.04 LTS
- **Docker**: Latest Docker CE with docker-compose
- **TRUF.NETWORK Stack**:
  - PostgreSQL (ghcr.io/trufnetwork/kwil-postgres:16.8-1) 
  - TRUF.NETWORK Node - ⚠️ To be added when ghcr image is published
  - PostgreSQL MCP Server (crystaldba/postgres-mcp:latest) - Will need to be adjusted later on
- **Configuration Scripts**:
  - `/usr/local/bin/tn-node-configure` - Initial setup
  - `/usr/local/bin/update-node` - Update to latest images
- **SystemD Service**: `tn-node.service` for service management
- **File Structure**:
  - `/opt/tn/` - Main application directory
  - `/opt/tn/docker-compose.yml` - Container orchestration
  - `/opt/tn/.env` - Environment configuration

### Local Testing

Test the CDK stack locally before AWS deployment:

```bash
cd deployments/infra
./scripts/test-ami.sh
```

This runs:
- CDK synthesis validation
- Docker container tests
- YAML configuration validation

### Troubleshooting

**CDK Deployment Issues:**
```bash
cdk --version  # Ensure CDK is latest
aws sts get-caller-identity  # Verify AWS access
cdk bootstrap  # Re-run if needed
```

**AMI Build Issues:**
```bash
# Check Image Builder logs
aws logs describe-log-groups --log-group-name-prefix /aws/imagebuilder
```

### Files

- `stacks/ami_pipeline_stack.go` - Main CDK stack for AMI pipeline
- `ami-cdk.go` - Isolated AMI deployment application
- `cdk.test.json` - CDK configuration for AMI deployment

---

## Important

Always use these commands responsibly, especially in non-production environments. Remember to delete the stack after testing to avoid unnecessary AWS charges.
