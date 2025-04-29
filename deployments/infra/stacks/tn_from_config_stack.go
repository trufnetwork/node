package stacks

import (
	"fmt"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/config/domain"
	fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
	"github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
	"github.com/trufnetwork/node/infra/lib/constructs/observability_suite"
	"github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	kwil_network "github.com/trufnetwork/node/infra/lib/kwil-network"
	"github.com/trufnetwork/node/infra/lib/observer"
)

type TnFromConfigStackProps struct {
	awscdk.StackProps
	CertStackExports *CertStackExports `json:",omitempty"` // only for frontingType=cloudfront
}

func TnFromConfigStack(
	scope constructs.Construct,
	id string,
	props *TnFromConfigStackProps,
) awscdk.Stack {
	// Standard stack initialization
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)
	if !config.IsStackInSynthesis(stack) {
		return stack
	}

	// Read environment config for number of nodes
	cfg := config.GetEnvironmentVariables[config.ConfigStackEnvironmentVariables](stack)
	privateKeys := strings.Split(cfg.NodePrivateKeys, ",")

	// Always include observer init
	observerAsset := observer.GetObserverAsset(stack, jsii.String("observer"))
	initObserver := awsec2.InitFile_FromExistingAsset(
		jsii.String(observer.ObserverZipAssetDir), observerAsset,
		&awsec2.InitFileOptions{Owner: jsii.String("ec2-user")},
	)
	initElements := []awsec2.InitElement{initObserver}

	// VPC & domain setup
	vpc := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{IsDefault: jsii.Bool(true)})
	cdkParams := config.NewCDKParams(stack)
	hd := domain.NewHostedDomain(stack, "HostedDomain", &domain.HostedDomainProps{
		Spec: domain.Spec{
			Stage:     domain.StageType(*cdkParams.Stage.ValueAsString()),
			Sub:       "",
			DevPrefix: *cdkParams.DevPrefix.ValueAsString(),
		},
		EdgeCertificate: false,
	})

	// Generate network configs from number of private keys
	nodesConfig := kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(
		stack,
		kwil_network.KwilAutoNetworkConfigAssetInput{NumberOfNodes: len(privateKeys)},
	)

	// TN assets via helper
	tnAssets := validator_set.BuildTNAssets(stack, validator_set.TNAssetOptions{RootDir: "compose"})

	// Create ValidatorSet
	vs := validator_set.NewValidatorSet(stack, "ValidatorSet", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		NodesConfig:  nodesConfig,
		KeyPair:      nil,
		Assets:       tnAssets,
		InitElements: initElements,
	})

	// Kwil Cluster assets via helper
	kwilAssets := kwil_cluster.BuildKwilAssets(stack, kwil_cluster.KwilAssetOptions{
		RootDir:          ".", // Assuming stack run from infra root
		BinaryBucketName: "kwil-binaries",
		BinaryKeyPrefix:  "gateway",
	})

	// Create KwilCluster
	kc := kwil_cluster.NewKwilCluster(stack, "KwilCluster", &kwil_cluster.KwilClusterProps{
		Vpc:           vpc,
		HostedDomain:  hd,
		Cert:          props.CertStackExports.DomainCert,
		CorsOrigins:   cdkParams.CorsAllowOrigins.ValueAsString(),
		SessionSecret: cdkParams.SessionSecret.ValueAsString(),
		ChainId:       jsii.String(config.GetEnvironmentVariables[config.MainEnvironmentVariables](stack).ChainId),
		Validators:    vs.Nodes,
		InitElements:  initElements,
		Assets:        kwilAssets,
	})

	// Wire up API Gateway fronting
	ag := fronting.NewApiGatewayFronting()
	recordName := jsii.String("api." + *cdkParams.DevPrefix.ValueAsString())
	// Provision the front-end and retrieve its FQDN and certificate (plugin issues cert)
	frontRes := ag.AttachRoutes(stack, "APIGateway", &fronting.FrontingProps{
		HostedZone:          hd.Zone,
		ImportedCertificate: nil,
		AdditionalSANs:      nil,
		KGWEndpoint:         kc.Gateway.InstanceDnsName,
		IndexerEndpoint:     kc.Indexer.InstanceDnsName,
		RecordName:          recordName,
	})
	// Output the custom domain
	awscdk.NewCfnOutput(stack, jsii.String("ApiDomain"), &awscdk.CfnOutputProps{Value: frontRes.FQDN})
	// Output the certificate ARN
	awscdk.NewCfnOutput(stack, jsii.String("ApiCertArn"), &awscdk.CfnOutputProps{Value: frontRes.Certificate.CertificateArn()})

	// Observability
	_ = observability_suite.NewObservabilitySuite(stack, "ObservabilitySuite", &observability_suite.ObservabilitySuiteProps{
		Vpc:          vpc,
		ValidatorSg:  vs.SecurityGroup,
		GatewaySg:    kc.Gateway.SecurityGroup,
		ParamsPrefix: jsii.String(fmt.Sprintf("/%s/observer", *cdkParams.Stage.ValueAsString())),
	})

	return stack
}
