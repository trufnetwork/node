# Welcome to your CDK Go project!

This is a blank project for CDK development with Go.

The `cdk.json` file tells the CDK toolkit how to execute your app.

## Useful commands

 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk synth`       emits the synthesized CloudFormation template
 * `go test`         run unit tests

### Example

Example of how to deploy a stack:

```bash
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001 KWIL_ADMIN_BIN_PATH=kwil-admin CHAIN_ID=truflation-dev CDK_DOCKER="<YOUR-DIRECTORY>/tsn/deployments/infra/buildx.sh" cdk deploy --profile <YOUR-AWS-PROVILE> --all --asset-parallelism=false --notices false --parameters TSN-DB-Stack-dev:sessionSecret=abab
```

Legend:
- `PRIVATE_KEY` - any ETH private key
- `KWIL_ADMIN_BIN_PATH` - path to kwil-admin binary, if it already on PATH, you can just use `kwil-admin`
- `CHAIN_ID` - chain id for kwild
- `CDK_DOCKER` - path to docker buildx script
- `--profile` - AWS profile

Please use the command wisely for DEV environment only. i.e. Set `deploymentStage` to `DEV` (uppercase) in `cdk.json` file.
Configure the `stackName`, `deploymentStage` and other configurations in `cdk.json` file. Don't forget to delete the stack after testing.

#### Note for Windows users

If you are using Windows with WSL2, you may need to disable your Windows firewall to allow cdk-cli to forward the port to the docker container.
Then you can open PowerShell as an administrator and run the following command to forward the port:

```bash
netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=22 connectaddress=localhost connectport=22
```