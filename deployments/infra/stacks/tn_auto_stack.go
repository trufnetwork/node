package stacks

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/config/domain"
	fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
	"github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
	"github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	kwil_network "github.com/trufnetwork/node/infra/lib/kwil-network"
	"github.com/trufnetwork/node/infra/lib/observer"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type TnAutoStackProps struct {
	awscdk.StackProps
	CertStackExports *CertStackExports `json:",omitempty"`
}

func TnAutoStack(scope constructs.Construct, id string, props *TnAutoStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)
	if !config.IsStackInSynthesis(stack) {
		return stack
	}

	// Define CDK params, stage, and prefix early
	cdkParams := config.NewCDKParams(stack)
	stage := *cdkParams.Stage.ValueAsString()
	devPrefix := *cdkParams.DevPrefix.ValueAsString()

	// Define Fronting Type parameter within stack scope
	_ = config.NewFrontingSelector(stack) // Result not explicitly needed here, but creates the parameter

	initElements := []awsec2.InitElement{} // Base elements only
	var observerAsset awss3assets.Asset    // Keep asset variable, needed for Attach call

	shouldIncludeObserver := config.GetEnvironmentVariables[config.AutoStackEnvironmentVariables](stack).IncludeObserver

	if shouldIncludeObserver {
		// Only get the asset here, don't generate InitElements
		observerAsset = observer.GetObserverAsset(stack, jsii.String("observer"))
	}

	// Get default VPC
	vpc := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{IsDefault: jsii.Bool(true)})
	hd := domain.NewHostedDomain(stack, "HostedDomain", &domain.HostedDomainProps{
		Spec: domain.Spec{
			Stage:     domain.StageType(stage),
			Sub:       "",
			DevPrefix: devPrefix,
		},
		EdgeCertificate: false,
	})

	peers, genesisAsset := kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(
		stack,
		kwil_network.KwilAutoNetworkConfigAssetInput{NumberOfNodes: config.NumOfNodes(stack)},
	)

	// TN assets via helper
	tnAssets := validator_set.BuildTNAssets(stack, validator_set.TNAssetOptions{RootDir: utils.GetProjectRootDir()})

	vs := validator_set.NewValidatorSet(stack, "ValidatorSet", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		Peers:        peers,
		GenesisAsset: genesisAsset,
		KeyPair:      nil,
		Assets:       tnAssets,
		InitElements: initElements,
	})

	// Kwil Cluster assets via helper
	kwilAssets := kwil_cluster.BuildKwilAssets(stack, kwil_cluster.KwilAssetOptions{
		RootDir:          utils.GetProjectRootDir(),
		BinaryBucketName: "kwil-binaries",
		BinaryKeyPrefix:  "gateway",
	})

	kc := kwil_cluster.NewKwilCluster(stack, "KwilCluster", &kwil_cluster.KwilClusterProps{
		Vpc:           vpc,
		HostedDomain:  hd,
		Cert:          props.CertStackExports.DomainCert,
		CorsOrigins:   cdkParams.CorsAllowOrigins.ValueAsString(),
		SessionSecret: cdkParams.SessionSecret.ValueAsString(),
		ChainId:       jsii.String(config.GetEnvironmentVariables[config.MainEnvironmentVariables](stack).ChainId),
		Validators:    vs.Nodes,
		InitElements:  initElements, // Only pass base elements
		Assets:        kwilAssets,
	})

	ag := fronting.NewApiGatewayFronting()
	recordName := jsii.String("api." + devPrefix) // Use var
	frontRes := ag.AttachRoutes(stack, "APIGateway", &fronting.FrontingProps{
		HostedZone:          hd.Zone,
		ImportedCertificate: nil,
		AdditionalSANs:      nil,
		KGWEndpoint:         kc.Gateway.InstanceDnsName,
		IndexerEndpoint:     kc.Indexer.InstanceDnsName,
		RecordName:          recordName,
	})
	awscdk.NewCfnOutput(stack, jsii.String("ApiDomain"), &awscdk.CfnOutputProps{Value: frontRes.FQDN})
	awscdk.NewCfnOutput(stack, jsii.String("ApiCertArn"), &awscdk.CfnOutputProps{Value: frontRes.Certificate.CertificateArn()})

	// AttachObserverPermissions call will be added here later
	if shouldIncludeObserver {
		if observerAsset == nil {
			panic("Observer asset is nil when observer should be included")
		}
		// observer.AttachObserverPermissions(...)
		observer.AttachObservability(observer.AttachObservabilityInput{
			Scope:         stack,
			ValidatorSet:  vs,
			KwilCluster:   kc,
			ObserverAsset: observerAsset,
		})
	}

	return stack
}
