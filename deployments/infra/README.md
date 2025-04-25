# TN-DB Infrastructure

This project contains the AWS CDK infrastructure code for deploying TN-DB nodes, a Kwil Gateway, and an Indexer.

## Overview

The infrastructure can be deployed in two ways:
1. Auto-generated configuration
2. Pre-configured setup

The `cdk.json` file tells the CDK toolkit how to execute your app.

## Deployment Methods

### 1. Auto-generated Configuration

This method dynamically generates the TN node configuration during deployment. It deploys both the launch templates and the instances from these templates.

#### Example Command:

```bash
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
KWILD_CLI_PATH=kwild \
CHAIN_ID=truflation-dev \
CDK_DOCKER="<YOUR-DIRECTORY>/tn/deployments/infra/buildx.sh" \
cdk deploy --profile <YOUR-AWS-PROFILE> --all --asset-parallelism=false --notices false \
--parameters TN-DB-Stack-dev:stage=dev \
--parameters TN-DB-Stack-dev:devPrefix=<YOUR-DEV-PREFIX> \
--parameters TN-DB-Stack-dev:sessionSecret=abab
```

### 2. Pre-configured Setup

This method uses a pre-existing genesis file and a list of private keys for the TN nodes. It deploys only the launch templates, not the instances.

#### Example Command:

```bash
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
KWILD_CLI_PATH=kwild \
CHAIN_ID=truflation-dev \
CDK_DOCKER="<YOUR-DIRECTORY>/tn/deployments/infra/buildx.sh" \
NODE_PRIVATE_KEYS="key1,key2,key3" \
GENESIS_PATH="/path/to/genesis.json" \
cdk deploy --profile <YOUR-AWS-PROFILE> TN-From-Config* TN-Cert* \
--parameters TN-From-Config-<environment>-Stack:stage=dev \
--parameters TN-From-Config-<environment>-Stack:devPrefix=<YOUR-DEV-PREFIX> \
--parameters TN-From-Config-<environment>-Stack:sessionSecret=abab
```

## Redeploying Instances from Launch Templates

In our stack, the launch templates for the Kwil Gateway (kgw) and Indexer instances are created, and the instances themselves are **not** automatically provisioned. This approach provides greater flexibility and control over instance deployment and updates. Below are the steps to redeploy an instance using the launch templates:

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
    cdk deploy --profile <YOUR-AWS-PROFILE> TSN-From-Config* TSN-Cert* \
    --parameters TSN-From-Config-<environment>-Stack:sessionSecret=<SESSION-SECRET>
    ```

2. After deployment, SSH into the instance you want to upgrade.

3. Pull the latest image from the ECR repository (you may need to login to the ECR repository first, please see the correct commands at the AWS console)

    ```bash
    docker pull <latest-image>
    ```

4. Tag the image as `tsn:local`

    ```bash
    docker tag <latest-image> tsn:local
    ```

5. Restart the systemd service that runs the TSN node:

    ```bash
    sudo systemctl restart tsn-db-app.service
    ```

This process ensures that your node is running the latest version of the software while maintaining the pre-configured setup.

## Environment Variables

- `PRIVATE_KEY`: Ethereum private key for the admin account
- `KWILD_CLI_PATH`: Path to the `kwild` CLI binary (use `kwild` if it's in your PATH)
- `CHAIN_ID`: Chain ID for the Kwil network
- `CDK_DOCKER`: Path to docker buildx script
- `NODE_PRIVATE_KEYS`: Comma-separated list of private keys for TSN nodes (only for pre-configured setup)
- `GENESIS_PATH`: Path to the genesis file (only for pre-configured setup)

## AWS Profile

Use the `--profile` option to specify your AWS profile.

## Deployment Stage

- Stacks use CFN parameters for `stage` and `devPrefix`, not context.
- Example: `--parameters <stack>:stage=dev [--parameters <stack>:devPrefix=<prefix>]`.
- Valid stages are:
- `dev`
- `prod`

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

## Important

Always use these commands responsibly, especially in non-production environments. Remember to delete the stack after testing to avoid unnecessary AWS charges.