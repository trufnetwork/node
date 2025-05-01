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

	// Define CDK params and stage early, and read dev prefix from context
	cdkParams := config.NewCDKParams(stack)
	mainEnvVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](stack)
	stage := config.GetStage(stack)
	devPrefix := config.GetDevPrefix(stack)

	// Define Fronting Type parameter within stack scope
	selectedKind := config.GetFrontingKind(stack) // Use context helper

	// Setup observer init elements
	initElements := []awsec2.InitElement{}                                     // Base elements
	observerAsset := observer.GetObserverAsset(stack, jsii.String("observer")) // Keep asset var

	// VPC & domain setup
	vpc := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{IsDefault: jsii.Bool(true)})
	hd := domain.NewHostedDomain(stack, "HostedDomain", &domain.HostedDomainProps{
		Spec: domain.Spec{
			Stage:     stage,
			Sub:       "",
			DevPrefix: devPrefix,
		},
		EdgeCertificate: false,
	})

	// Generate network configs from number of private keys
	peers, _, genesisAsset := kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(
		stack,
		kwil_network.KwilAutoNetworkConfigAssetInput{
			NumberOfNodes:   len(privateKeys),
			GenesisFilePath: cfg.GenesisPath,
		},
	)

	// TN assets via helper
	tnAssets := validator_set.BuildTNAssets(stack, validator_set.TNAssetOptions{RootDir: "compose"})

	// Create ValidatorSet
	vs := validator_set.NewValidatorSet(stack, "ValidatorSet", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		Peers:        peers,
		GenesisAsset: genesisAsset,
		KeyPair:      nil,
		Assets:       tnAssets,
		InitElements: initElements, // Only pass base elements
		CDKParams:    cdkParams,
	})

	// Kwil Cluster assets via helper
	kwilAssets := kwil_cluster.BuildKwilAssets(stack, kwil_cluster.KwilAssetOptions{
		RootDir:            ".", // Assuming stack run from infra root
		BinariesBucketName: "kwil-binaries",
		KGWBinaryKey:       "gateway/kgw-v0.4.1.zip",
	})

	// Create KwilCluster
	kc := kwil_cluster.NewKwilCluster(stack, "KwilCluster", &kwil_cluster.KwilClusterProps{
		Vpc:                  vpc,
		HostedDomain:         hd,
		CorsOrigins:          cdkParams.CorsAllowOrigins.ValueAsString(),
		SessionSecret:        jsii.String(mainEnvVars.SessionSecret),
		ChainId:              jsii.String(mainEnvVars.ChainId),
		Validators:           vs.Nodes,
		InitElements:         initElements, // Only pass base elements
		Assets:               kwilAssets,
		SelectedFrontingKind: selectedKind, // Pass selected kind
	})

	// --- Fronting Setup ---
	// Build Spec for domain subdomains
	spec := domain.Spec{
		Stage:     stage,
		Sub:       "",
		DevPrefix: devPrefix,
	}

	if selectedKind == fronting.KindAPI {
		// Dual API Gateway setup specific logic
		gatewayRecord := spec.Subdomain("gateway")
		indexerRecord := spec.Subdomain("indexer")

		// Get props for shared certificate setup
		gwProps, idxProps := fronting.GetSharedCertProps(hd.Zone, *gatewayRecord, *indexerRecord)

		// Set endpoints
		gwProps.Endpoint = kc.Gateway.GatewayFqdn
		idxProps.Endpoint = kc.Indexer.IndexerFqdn

		// 1. Gateway Fronting (issues cert)
		gApi := fronting.NewApiGatewayFronting() // Use concrete type here for API GW setup
		gatewayRes := gApi.AttachRoutes(stack, "GatewayFronting", &gwProps)

		// 2. Indexer Fronting (imports cert)
		idxProps.ImportedCertificate = gatewayRes.Certificate // Set imported cert
		iApi := fronting.NewApiGatewayFronting()              // Use concrete type here for API GW setup
		indexerRes := iApi.AttachRoutes(stack, "IndexerFronting", &idxProps)

		// --- Outputs ---
		awscdk.NewCfnOutput(stack, jsii.String("GatewayEndpoint"), &awscdk.CfnOutputProps{
			Value:       gatewayRes.FQDN,
			Description: jsii.String("Public FQDN for the Kwil Gateway API"),
		})
		awscdk.NewCfnOutput(stack, jsii.String("IndexerEndpoint"), &awscdk.CfnOutputProps{
			Value:       indexerRes.FQDN,
			Description: jsii.String("Public FQDN for the Kwil Indexer API"),
		})
		awscdk.NewCfnOutput(stack, jsii.String("ApiCertArn"), &awscdk.CfnOutputProps{
			Value:       gatewayRes.Certificate.CertificateArn(),
			Description: jsii.String("ARN of the regional ACM certificate used for API Gateway TLS"),
		})
	} else {
		// Handle other fronting types (ALB, CloudFront)
		// Currently, the dual endpoint setup is only implemented for API Gateway
		panic(fmt.Sprintf("Dual endpoint fronting setup not implemented for type: %s", selectedKind))
	}

	if observerAsset == nil {
		panic("Observer asset is nil in tn_from_config_stack") // Should not happen
	}
	observer.AttachObservability(observer.AttachObservabilityInput{
		Scope:         stack,
		ValidatorSet:  vs,
		KwilCluster:   kc,
		ObserverAsset: observerAsset,
		Params:        cdkParams,
	})

	return stack
}
