# TSN-DB Infrastructure

This project contains the AWS CDK infrastructure code for deploying TSN-DB nodes, a Kwil Gateway, and an Indexer.

## Overview

The infrastructure can be deployed in two ways:
1. Auto-generated configuration
2. Pre-configured setup

The `cdk.json` file tells the CDK toolkit how to execute your app.

## Deployment Methods

### 1. Auto-generated Configuration

This method dynamically generates the TSN node configuration during deployment.

#### Example Command:

```bash
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
KWIL_ADMIN_BIN_PATH=kwil-admin \
CHAIN_ID=truflation-dev \
CDK_DOCKER="<YOUR-DIRECTORY>/tsn/deployments/infra/buildx.sh" \
RESTART_HASH=$(date +%s) \
cdk deploy --profile <YOUR-AWS-PROFILE> --all --asset-parallelism=false --notices false \
--context deploymentStage=DEV \
--parameters TSN-DB-Stack-dev:sessionSecret=abab
```

### RESTART_HASH Explanation

The `RESTART_HASH` environment variable controls the full redeployment of TSN instances, re-executing all processes. 

- Use a fixed value (e.g., `RESTART_HASH=v0`) to avoid redeployments.
- Use a timestamp (e.g., `RESTART_HASH=$(date +%s)`) to force redeployment on every execution.

### 2. Pre-configured Setup

This method uses a pre-existing genesis file and a list of private keys for the TSN nodes.

#### Example Command:

```bash
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
KWIL_ADMIN_BIN_PATH=kwil-admin \
CHAIN_ID=truflation-dev \
CDK_DOCKER="<YOUR-DIRECTORY>/tsn/deployments/infra/buildx.sh" \
NODE_PRIVATE_KEYS="key1,key2,key3" \
GENESIS_PATH="/path/to/genesis.json" \
cdk deploy --profile <YOUR-AWS-PROFILE> TSN-From-Config* TSN-Cert* \
--parameters TSN-From-Config-<environment>-Stack:sessionSecret=abab
```

## Environment Variables

- `PRIVATE_KEY`: Ethereum private key for the admin account
- `KWIL_ADMIN_BIN_PATH`: Path to kwil-admin binary (use `kwil-admin` if it's in your PATH)
- `CHAIN_ID`: Chain ID for the Kwil network
- `CDK_DOCKER`: Path to docker buildx script
- `NODE_PRIVATE_KEYS`: Comma-separated list of private keys for TSN nodes (only for pre-configured setup)
- `GENESIS_PATH`: Path to the genesis file (only for pre-configured setup)
- `RESTART_HASH`: Controls redeployment of TSN instances (for auto-generated configuration)

## AWS Profile

Use the `--profile` option to specify your AWS profile.

## Deployment Stage

Set the deployment stage using the `--context deploymentStage=<STAGE>` option, or by editing the `cdk.json` file. Valid stages are:
- `DEV`
- `STAGING`
- `PROD`

You may also edit the number of nodes for auto-generated configuration in the `cdk.json` file.

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

## Important

Always use these commands responsibly, especially in non-production environments. Remember to delete the stack after testing to avoid unnecessary AWS charges.