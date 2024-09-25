package stacks

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/domain_utils"
	kwil_gateway "github.com/truflation/tsn-db/infra/lib/kwil-gateway"
	kwil_indexer_instance "github.com/truflation/tsn-db/infra/lib/kwil-indexer"
	system_contract "github.com/truflation/tsn-db/infra/lib/system-contract"
	"github.com/truflation/tsn-db/infra/lib/tsn"
	"github.com/truflation/tsn-db/infra/lib/tsn/cluster"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type TsnStackProps struct {
	certStackExports CertStackExports
	clusterProvider  cluster.TSNClusterProvider
}

func TsnStack(stack awscdk.Stack, props *TsnStackProps) awscdk.Stack {
	cdkParams := config.NewCDKParams(stack)

	// if it's not being synthesized, return the stack
	if !config.IsStackInSynthesis(stack) {
		return stack
	}

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
		Key: jsii.String("gateway/kgw-v0.3.3.zip"),
	}

	// ## Instances & Permissions

	// ### TSN INSTANCE
	tsnCluster := props.clusterProvider.CreateCluster(stack, cluster.NewTSNClusterInput{
		TSNDockerComposeAsset: tsnComposeAsset,
		TSNDockerImageAsset:   tsnImageAsset,
		Vpc:                   defaultVPC,
		HostedZone:            hostedZone,
		TSNConfigImageAsset:   tsnConfigImageAsset,
	})

	tsnComposeAsset.GrantRead(tsnCluster.Role)
	tsnImageAsset.Repository().GrantPull(tsnCluster.Role)

	// ### GATEWAY INSTANCE

	kgwInstance := kwil_gateway.NewKGWInstance(stack, kwil_gateway.NewKGWInstanceInput{
		HostedZone:     hostedZone,
		Vpc:            defaultVPC,
		KGWBinaryAsset: kgwBinaryS3Object,
		KGWDirAsset:    kgwDirectoryAsset,
		Config: kwil_gateway.KGWConfig{
			Domain:           domain,
			CorsAllowOrigins: cdkParams.CorsAllowOrigins.ValueAsString(),
			SessionSecret:    cdkParams.SessionSecret.ValueAsString(),
			ChainId:          jsii.String(config.GetEnvironmentVariables[config.MainEnvironmentVariables](stack).ChainId),
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
		Domain:          domain,
		HostedZone:      hostedZone,
	})

	// add read permission to the indexer instance role
	indexerDirectoryAsset.GrantRead(indexerInstance.Role)

	// Cloudfront for the TSN
	// We use cloudfront to handle TLS termination. The certificate is created in a separate stack in us-east-1.
	// We disable caching.
	kwil_gateway.TSNCloudfrontInstance(
		stack,
		jsii.String("CloudFrontDistribution"),
		kwil_gateway.TSNCloudfrontConfig{
			DomainName:           domain,
			KgwPublicDnsName:     kgwInstance.InstanceDnsName,
			Certificate:          props.certStackExports.DomainCert,
			HostedZone:           hostedZone,
			IndexerPublicDnsName: indexerInstance.InstanceDnsName,
		},
	)

	// to make easier to deploy a contract, we create a lambda that can be manually triggered
	system_contract.SystemContractDeployer(stack, system_contract.DeployContractResourceOptions{
		SystemContractPath: jsii.String("../../internal/contracts/system_contract.kf"),
		PrivateKey:         config.GetEnvironmentVariables[config.MainEnvironmentVariables](stack).PrivateKey,
		ProviderUrl:        jsii.String(fmt.Sprintf("https://%s", *domain)),
	})

	// ## Output info
	// Public ip of each TSN node
	for _, node := range tsnCluster.Nodes {
		awscdk.NewCfnOutput(stack, jsii.String("public-address-node-"+strconv.Itoa(node.Index)), &awscdk.CfnOutputProps{
			Value: node.ElasticIp.AttrPublicIp(),
		})
	}

	// Number of TSN nodes
	awscdk.NewCfnOutput(stack, jsii.String("tsn-nodes-count"), &awscdk.CfnOutputProps{
		Value: jsii.String(strconv.Itoa(len(tsnCluster.Nodes))),
	})

	// Public ip of the gateway instance
	awscdk.NewCfnOutput(stack, jsii.String("gateway-public-address"), &awscdk.CfnOutputProps{
		Value: kgwInstance.InstanceDnsName,
	})

	// Public ip of the indexer instance
	awscdk.NewCfnOutput(stack, jsii.String("indexer-public-address"), &awscdk.CfnOutputProps{
		Value: indexerInstance.InstanceDnsName,
	})

	awscdk.NewCfnOutput(stack, jsii.String("region"), &awscdk.CfnOutputProps{
		Value: stack.Region(),
	})

	return stack
}
