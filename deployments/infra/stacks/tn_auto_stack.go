package stacks

import (
	"fmt"

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

	// Define CDK params and stage early, and read dev prefix from context
	cdkParams := config.NewCDKParams(stack)
	stage := config.GetStage(stack)
	devPrefix := config.GetDevPrefix(stack)
	// Retrieve Main environment variables including DbOwner
	autoEnvVars := config.GetEnvironmentVariables[config.AutoStackEnvironmentVariables](stack)

	initElements := []awsec2.InitElement{} // Base elements only
	var observerAsset awss3assets.Asset    // Keep asset variable, needed for Attach call

	shouldIncludeObserver := autoEnvVars.IncludeObserver

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

	// Generate network config assets (peers, node keys, genesis)
	peers, nodeKeys, genesisAsset := kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(
		stack,
		kwil_network.KwilAutoNetworkConfigAssetInput{
			NumberOfNodes: config.NumOfNodes(stack),
			DbOwner:       autoEnvVars.DbOwner, // Pass DbOwner here
			Params:        cdkParams,
		},
	)

	// TN assets via helper
	tnAssets := validator_set.BuildTNAssets(stack, validator_set.TNAssetOptions{RootDir: utils.GetProjectRootDir()})

	vs := validator_set.NewValidatorSet(stack, "ValidatorSet", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		Peers:        peers,    // Pass public peer info
		NodeKeys:     nodeKeys, // Pass node keys (including private)
		GenesisAsset: genesisAsset,
		KeyPair:      nil,
		Assets:       tnAssets,
		InitElements: initElements,
		CDKParams:    cdkParams,
	})

	// --- Create EC2 Instances for TN Nodes ---
	for _, node := range vs.Nodes {
		instanceId := jsii.Sprintf("TNNodeInstance-%d", node.Index)
		utils.InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(
			stack,      // Scope
			instanceId, // Unique construct ID for the instance
			utils.InstanceFromLaunchTemplateOnPublicSubnetInput{
				LaunchTemplate: node.LaunchTemplate,
				ElasticIp:      node.ElasticIp,
				Vpc:            vpc,
			},
		)
	}

	// Kwil Cluster assets via helper
	kwilAssets := kwil_cluster.BuildKwilAssets(stack, kwil_cluster.KwilAssetOptions{
		RootDir:            utils.GetProjectRootDir(),
		BinariesBucketName: "kwil-binaries",
		KGWBinaryKey:       "gateway/kgw-v0.4.1.zip",
		IndexerBinaryKey:   "indexer/kwil-indexer_v0.3.0-dev_linux_amd64.zip",
	})

	// Create KwilCluster
	selectedKind := config.GetFrontingKind(stack)
	kc := kwil_cluster.NewKwilCluster(stack, "KwilCluster", &kwil_cluster.KwilClusterProps{
		Vpc:                  vpc,
		HostedDomain:         hd,
		Cert:                 props.CertStackExports.DomainCert,
		CorsOrigins:          cdkParams.CorsAllowOrigins.ValueAsString(),
		SessionSecret:        jsii.String(autoEnvVars.SessionSecret),
		ChainId:              jsii.String(autoEnvVars.ChainId),
		Validators:           vs.Nodes,
		InitElements:         initElements,
		Assets:               kwilAssets,
		SelectedFrontingKind: selectedKind,
	})

	// --- Create EC2 Instance for Kwil Gateway ---
	utils.InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(
		stack,                      // Scope
		jsii.String("KGWInstance"), // Unique construct ID
		utils.InstanceFromLaunchTemplateOnPublicSubnetInput{
			LaunchTemplate: kc.Gateway.LaunchTemplate,
			ElasticIp:      kc.Gateway.ElasticIp,
			Vpc:            vpc,
		},
	)

	// --- Create EC2 Instance for Kwil Indexer ---
	utils.InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(
		stack,                          // Scope
		jsii.String("IndexerInstance"), // Unique construct ID
		utils.InstanceFromLaunchTemplateOnPublicSubnetInput{
			LaunchTemplate: kc.Indexer.LaunchTemplate,
			ElasticIp:      kc.Indexer.ElasticIp,
			Vpc:            vpc,
		},
	)

	if selectedKind == fronting.KindAPI {
		// Dual API Gateway setup specific logic
		// Build Spec for domain subdomains - Moved inside the 'if' block as it's only used here
		spec := domain.Spec{
			Stage:     domain.StageType(stage),
			Sub:       "",
			DevPrefix: devPrefix,
		}
		gatewayRecord := spec.Subdomain("gateway")
		indexerRecord := spec.Subdomain("indexer")

		// Get props for shared certificate setup using full subdomains from spec
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
			Value:       gatewayRes.Certificate.CertificateArn(), // Output the ARN of the shared cert
			Description: jsii.String("ARN of the regional ACM certificate used for API Gateway TLS"),
		})
	} else {
		// Handle other fronting types (ALB, CloudFront)
		// Currently, the dual endpoint setup is only implemented for API Gateway
		panic(fmt.Sprintf("Dual endpoint fronting setup not implemented for type: %s", selectedKind))
	}

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
			Params:        cdkParams,
		})
	}

	return stack
}
