# TN-DB Infrastructure

This project contains the AWS CDK infrastructure code for deploying TN-DB nodes, a Kwil Gateway, and an Indexer.

## Overview

The infrastructure can be deployed in two ways:
1. Auto-generated configuration
2. Pre-configured setup

The `cdk.json` file tells the CDK toolkit how to execute your app.

## High-level Constructs
We have extracted three reusable L3 constructs under [deployments/infra/lib/constructs](https://github.com/trufnetwork/node/tree/main/deployments/infra/lib/constructs) to simplify stack logic:

- **ValidatorSet**: provisions a group of TN validator EC2 instances with Elastic IPs, DNS A records, a shared IAM role, and security group.
  - Props: `Vpc`, `HostedDomain`, `NodesConfig`, `KeyPair`, `ImageAssets`, `InitElements` (e.g. custom EC2 user-data such as observer bootstrap)
  - Outputs: `Nodes []tn.TNInstance`, `Role awsiam.IRole`, `SecurityGroup awsec2.SecurityGroup`
  - Import:
    ```go
    import "github.com/trufnetwork/node/infra/lib/constructs/validator_set"
    ```
  - Usage:
    ```go
    vs := validator_set.NewValidatorSet(stack, "ValidatorSet", &validator_set.ValidatorSetProps{ ... })
    ```

- **KwilCluster**: provisions a Kwil Gateway and an Indexer.
  - Props: `Vpc`, `HostedDomain`, `Cert` (currently passed but not used for fronting), `CorsOrigins`, `SessionSecret`, `ChainId`, `Validators`, `InitElements`, `KGWDirAsset`, `KGWBinaryAsset`, `IndexerDirAsset`
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

#### Example Command:

```bash
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
KWILD_CLI_PATH=kwild \
CHAIN_ID=truflation-dev \
CDK_DOCKER="<YOUR-DIRECTORY>/tn/deployments/infra/buildx.sh" \
cdk deploy --profile <YOUR-AWS-PROFILE> --all --asset-parallelism=false --notices false \
  -c stage=dev \
  -c devPrefix=<YOUR-DEV-PREFIX> \
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
  -c stage=dev \
  -c devPrefix=<YOUR-DEV-PREFIX> \
  --parameters TN-From-Config-<environment>-Stack:sessionSecret=abab
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
    cdk deploy --profile <YOUR-AWS-PROFILE> TN-From-Config* TN-Cert* \
    --parameters TN-From-Config-<environment>-Stack:sessionSecret=<SESSION-SECRET>
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

- `PRIVATE_KEY`: Ethereum private key for the admin account
- `KWILD_CLI_PATH`: Path to the `kwild` CLI binary (use `kwild` if it's in your PATH)
- `CHAIN_ID`: Chain ID for the Kwil network
- `CDK_DOCKER`: Path to docker buildx script
- `NODE_PRIVATE_KEYS`: Comma-separated list of private keys for TSN nodes (only for pre-configured setup)
- `GENESIS_PATH`: Path to the genesis file (only for pre-configured setup)

## AWS Profile

Use the `--profile` option to specify your AWS profile.

## Deployment Stage

- Stacks now read `stage` and `devPrefix` from CDK context (not CloudFormation parameters).
- Example: `cdk deploy -c stage=dev -c devPrefix=<prefix>`.

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
