package main

import (
	"fmt"
	domain_utils "github.com/truflation/tsn-db/infra/lib/domain_utils"
	kwil_indexer_instance "github.com/truflation/tsn-db/infra/lib/kwil-indexer"
	system_contract "github.com/truflation/tsn-db/infra/lib/system-contract"
	"os"
	"strconv"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/kwil-gateway"
	"github.com/truflation/tsn-db/infra/lib/tsn"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type CdkStackProps struct {
	awscdk.StackProps
	cert awscertificatemanager.Certificate
}

func TsnDBCdkStack(scope constructs.Construct, id string, props *CdkStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)

	cdkParams := config.NewCDKParams(stack)

	// ## Pre-existing resources

	// default vpc
	defaultVPC := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{
		IsDefault: jsii.Bool(true),
	})

	// Main Hosted Zone & Domain
	domain := config.Domain(stack)
	hostedZone := domain_utils.GetTSNHostedZone(stack)

	// ## ASSETS
	// ### TSN ASSETS

	// TSN docker image
	tsnImageAsset := tsn.NewTSNImageAsset(stack)

	// TSN docker compose file to be used by any TSN node
	tsnComposeAsset := awss3assets.NewAsset(stack, jsii.String("TsnComposeAsset"), &awss3assets.AssetProps{
		Path: jsii.String("../../compose.yaml"),
	})

	// TSN config image
	tsnConfigImageAsset := awss3assets.NewAsset(stack, jsii.String("TsnConfigImageAsset"), &awss3assets.AssetProps{
		Path: jsii.String("../tsn-config.dockerfile"),
	})

	// ### GATEWAY ASSETS

	// differently from tsn-db, the gateway docker images will be built in its own instance, not in GH actions.
	// that's why we don't use an asset for the gateway docker image
	kgwDirectoryAsset := awss3assets.NewAsset(stack, jsii.String("KgwDirectoryAsset"), &awss3assets.AssetProps{
		// gateway directory contains more than one file to configure the gateway, so we need to zip it
		Path: jsii.String("../gateway/"),
	})

	indexerDirectoryAsset := awss3assets.NewAsset(stack, jsii.String("IndexerDirectoryAsset"), &awss3assets.AssetProps{
		// indexer directory contains more than one file to configure the indexer, so we need to zip it
		Path: jsii.String("../indexer/"),
	})

	// we store KGW binary in S3, and that bucket lives outside the stack
	kgwBinaryS3Object := utils.S3Object{
		Bucket: awss3.Bucket_FromBucketName(
			stack,
			jsii.String("KwilGatewayBucket"),
			jsii.String("kwil-binaries"),
		),
		Key: jsii.String("gateway/kgw-v0.3.0-beta.1.zip"),
	}

	// ## Instances & Permissions

	// ### TSN INSTANCE
	tsnCluster := tsn.NewTSNCluster(stack, tsn.NewTSNClusterInput{
		NumberOfNodes:         config.NumOfNodes(stack),
		TSNDockerComposeAsset: tsnComposeAsset,
		TSNDockerImageAsset:   tsnImageAsset,
		Vpc:                   defaultVPC,
		TSNConfigImageAsset:   tsnConfigImageAsset,
	})

	tsnComposeAsset.GrantRead(tsnCluster.Role)
	tsnImageAsset.Repository().GrantPull(tsnCluster.Role)

	// ### GATEWAY INSTANCE

	kgwInstance := kwil_gateway.NewKGWInstance(stack, kwil_gateway.NewKGWInstanceInput{
		Vpc:            defaultVPC,
		KGWBinaryAsset: kgwBinaryS3Object,
		KGWDirAsset:    kgwDirectoryAsset,
		Config: kwil_gateway.KGWConfig{
			Domain:           domain,
			CorsAllowOrigins: cdkParams.CorsAllowOrigins.ValueAsString(),
			SessionSecret:    cdkParams.SessionSecret.ValueAsString(),
			ChainId:          jsii.String(config.GetEnvironmentVariables().ChainId),
			Nodes:            tsnCluster.Nodes,
		},
	})

	// add read permission to the kgw instance role
	kgwBinaryS3Object.GrantRead(kgwInstance.Role)
	kgwDirectoryAsset.GrantRead(kgwInstance.Role)

	// ### INDEXER INSTANCE
	indexerInstance := kwil_indexer_instance.NewIndexerInstance(stack, kwil_indexer_instance.NewIndexerInstanceInput{
		Vpc:             defaultVPC,
		TSNInstance:     tsnCluster.Nodes[0],
		IndexerDirAsset: indexerDirectoryAsset,
	})

	// add read permission to the indexer instance role
	indexerDirectoryAsset.GrantRead(indexerInstance.Role)

	// Cloudfront for the TSN
	// We use cloudfront to handle TLS termination. The certificate is created in a separate stack in us-east-1.
	// We disable caching.
	cf := kwil_gateway.TSNCloudfrontInstance(stack, kwil_gateway.TSNCloudfrontConfig{
		DomainName:           domain,
		KgwPublicDnsName:     kgwInstance.Instance.InstancePublicDnsName(),
		Certificate:          props.cert,
		HostedZone:           hostedZone,
		IndexerPublicDnsName: indexerInstance.Instance.InstancePublicDnsName(),
	})

	// Deploy the system contract everytime the hash changes
	deployContract := system_contract.DeployContractResource(stack, system_contract.DeployContractResourceOptions{
		SystemContractPath: jsii.String("../../internal/contracts/system_contract.kf"),
		PrivateKey:         config.GetEnvironmentVariables().PrivateKey,
		ProviderUrl:        jsii.String(fmt.Sprintf("https://%s", *domain)),
		// so that every time the hash changes, the contract is deployed again
		Hash: tsnImageAsset.AssetHash(),
	})

	// contract must be the last thing done here. Otherwise it might try to deploy the contract before the instances are ready
	var contractDependencies []constructs.IDependable
	for _, node := range tsnCluster.Nodes {
		contractDependencies = append(contractDependencies, node.Instance)
	}
	contractDependencies = append(contractDependencies, kgwInstance.Instance)
	contractDependencies = append(contractDependencies, cf)

	deployContract.Node().AddDependency(contractDependencies...)

	// ## Output info
	// Public ip of each TSN node
	for _, node := range tsnCluster.Nodes {
		awscdk.NewCfnOutput(stack, jsii.String("public-address-"+*node.Instance.Node().Id()), &awscdk.CfnOutputProps{
			Value: node.Instance.InstancePublicIp(),
		})
	}

	// Number of TSN nodes
	awscdk.NewCfnOutput(stack, jsii.String("tsn-nodes-count"), &awscdk.CfnOutputProps{
		Value: jsii.String(strconv.Itoa(len(tsnCluster.Nodes))),
	})

	// Public ip of the gateway instance
	awscdk.NewCfnOutput(stack, jsii.String("gateway-public-address"), &awscdk.CfnOutputProps{
		Value: kgwInstance.Instance.InstancePublicIp(),
	})

	// Public ip of the indexer instance
	awscdk.NewCfnOutput(stack, jsii.String("indexer-public-address"), &awscdk.CfnOutputProps{
		Value: indexerInstance.Instance.InstancePublicIp(),
	})

	awscdk.NewCfnOutput(stack, jsii.String("region"), &awscdk.CfnOutputProps{
		Value: stack.Region(),
	})

	return stack
}

// CertStack creates a stack with an ACM certificate for the domain, fixed at us-east-1.
// This is necessary because CloudFront requires the certificate to be in us-east-1.
func CertStack(app constructs.Construct) awscertificatemanager.Certificate {
	env := env()
	env.Region = jsii.String("us-east-1")
	stackName := config.StackName(app) + "-Cert"
	stack := awscdk.NewStack(app, jsii.String(stackName), &awscdk.StackProps{
		Env:                   env,
		CrossRegionReferences: jsii.Bool(true),
	})
	domain := config.Domain(stack)
	hostedZone := domain_utils.GetTSNHostedZone(stack)
	return domain_utils.GetACMCertificate(stack, domain, &hostedZone)
}

func main() {
	app := awscdk.NewApp(nil)

	certificate := CertStack(app)

	TsnDBCdkStack(app, config.StackName(app), &CdkStackProps{
		awscdk.StackProps{
			Env:                   env(),
			CrossRegionReferences: jsii.Bool(true),
		},
		certificate,
	})

	app.Synth(nil)
}

// env determines the AWS environment (account+region) in which our stack is to
// be deployed. For more information see: https://docs.aws.amazon.com/cdk/latest/guide/environments.html
func env() *awscdk.Environment {
	account := os.Getenv("CDK_DEPLOY_ACCOUNT")
	region := os.Getenv("CDK_DEPLOY_REGION")

	if len(account) == 0 || len(region) == 0 {
		account = os.Getenv("CDK_DEFAULT_ACCOUNT")
		region = os.Getenv("CDK_DEFAULT_REGION")
	}

	return &awscdk.Environment{
		Account: jsii.String(account),
		Region:  jsii.String(region),
	}
}
