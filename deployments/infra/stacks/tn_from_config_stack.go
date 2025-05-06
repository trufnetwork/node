package stacks

import (
	"fmt"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/config/domain"
	altmgr "github.com/trufnetwork/node/infra/lib/constructs/alternativedomainmanager"
	fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
	"github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
	"github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	kwil_network "github.com/trufnetwork/node/infra/lib/kwil-network"
	"github.com/trufnetwork/node/infra/lib/observer"
	"github.com/trufnetwork/node/infra/lib/utils"
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
	shouldIncludeObserver := cfg.IncludeObserver // Read the new variable

	// Define CDK params and stage early, and read dev prefix from context
	cdkParams := config.NewCDKParams(stack)
	stage := config.GetStage(stack)
	devPrefix := config.GetDevPrefix(stack)

	// Define Fronting Type parameter within stack scope
	selectedKind := config.GetFrontingKind(stack) // Use context helper

	// --- Instantiate Alternative Domain Manager & SAN Builder ---
	sanBuilder := altmgr.NewSanListBuilder()
	// The manager handles loading config, registering targets, adding SANs, and creating records.
	altDomainManager := altmgr.NewAlternativeDomainManager(stack, "AltDomainManager", &altmgr.AlternativeDomainManagerProps{
		// ConfigFilePath and StackSuffix are read from context within the manager itself.
		CertSanBuilder: sanBuilder,
		// AlternativeHostedZoneDomainOverride: nil, // Optionally override config zone here.
	})

	// Setup observer init elements
	initElements := []awsec2.InitElement{} // Base elements
	var observerAsset awss3assets.Asset    // Keep asset var, initialize as nil
	if shouldIncludeObserver {             // Conditionally get the asset
		observerAsset = observer.GetObserverAsset(stack, jsii.String("observer"))
	}

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
	peers, nodeKeys, genesisAsset := kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(
		stack,
		kwil_network.KwilAutoNetworkConfigAssetInput{
			PrivateKeys:     privateKeys,
			GenesisFilePath: cfg.GenesisPath,
		},
	)

	// TN assets via helper
	tnAssets := validator_set.BuildTNAssets(stack, validator_set.TNAssetOptions{RootDir: utils.GetProjectRootDir()})

	// Create ValidatorSet
	vs := validator_set.NewValidatorSet(stack, "ValidatorSet", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		Peers:        peers,
		GenesisAsset: genesisAsset,
		KeyPair:      nil,
		Assets:       tnAssets,
		InitElements: initElements,
		CDKParams:    cdkParams,
		NodeKeys:     nodeKeys,
	})

	// --- Register Node Targets with Manager (After ValidatorSet creation) ---
	for _, node := range vs.Nodes {
		nodeTargetID := altmgr.NodeTargetID(node.Index) // Use helper for consistent ID.
		primaryFqdn := node.PeerConnection.Address
		// Ensure we have the necessary info before creating and registering the target.
		if primaryFqdn == nil || *primaryFqdn == "" {
			awscdk.Annotations_Of(stack).AddWarning(jsii.Sprintf("Node %d primary FQDN (PeerConnection.Address) is empty. Cannot register target %s.", node.Index+1, nodeTargetID))
			continue
		}

		// NOTE: Accessing node.ElasticIp.Ref() here assumes that the ValidatorSet construct
		// internally creates and associates an Elastic IP with each NodeInfo, even though
		// the EC2 instance itself might be created elsewhere (unlike tn_auto_stack).
		// This works if ValidatorSet consistently populates NodeInfo.ElasticIp.
		// A more robust solution might involve ValidatorSet explicitly returning EIP Refs.
		if node.ElasticIp == nil {
			awscdk.Annotations_Of(stack).AddWarning(jsii.Sprintf("Node %d ElasticIp is nil in NodeInfo. Cannot register target %s.", node.Index+1, nodeTargetID))
		} else {
			// Create a NodeTarget DnsTarget implementation using the EIP's Ref attribute.
			nodeTarget := &validator_set.NodeTarget{
				IpAddress:   node.ElasticIp.Ref(), // Ref() resolves to the allocated IP address.
				PrimaryAddr: primaryFqdn,
			}
			altDomainManager.RegisterTarget(nodeTargetID, nodeTarget)
		}
	}

	// Kwil Cluster assets via helper
	kwilAssets := kwil_cluster.BuildKwilAssets(stack, kwil_cluster.KwilAssetOptions{
		RootDir:            utils.GetProjectRootDir(), // Assuming stack run from infra root
		BinariesBucketName: "kwil-binaries",
		KGWBinaryKey:       "gateway/kgw-v0.4.1.zip",
		IndexerBinaryKey:   "indexer/kwil-indexer_v0.3.0-dev_linux_amd64.zip",
	})

	// Create KwilCluster
	kc := kwil_cluster.NewKwilCluster(stack, "KwilCluster", &kwil_cluster.KwilClusterProps{
		Vpc:                  vpc,
		HostedDomain:         hd,
		CorsOrigins:          cdkParams.CorsAllowOrigins.ValueAsString(),
		SessionSecret:        jsii.String(cfg.SessionSecret),
		ChainId:              jsii.String(cfg.ChainId),
		Validators:           vs.Nodes,
		InitElements:         initElements,
		Assets:               kwilAssets,
		SelectedFrontingKind: selectedKind,
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

		// Call the AlternativeDomainManager to populate the sanBuilder with SANs
		// from its configuration *before* adding local primary/sibling FQDNs.
		altDomainManager.CollectAndAddConfiguredSansToBuilder()

		// Add the primary FQDNs for Gateway and Indexer to the SAN list builder.
		// These will be combined with any SANs already added by the AlternativeDomainManager.
		sanBuilder.Add(gatewayRecord, indexerRecord)

		// Get props for shared certificate setup.
		gwProps, idxProps := fronting.GetSharedCertProps(hd.Zone, *gatewayRecord, *indexerRecord)

		// Set backend endpoints for the fronting constructs.
		gwProps.Endpoint = kc.Gateway.GatewayFqdn
		idxProps.Endpoint = kc.Indexer.IndexerFqdn

		// Assign the final consolidated SAN list from the builder to the Gateway props
		// (which is responsible for creating the shared certificate).
		gwProps.SubjectAlternativeNames = sanBuilder.List()

		// 1. Gateway Fronting (issues cert)
		gApi := fronting.NewApiGatewayFronting()
		gatewayRes := gApi.AttachRoutes(stack, "GatewayFronting", &gwProps)

		// --- Register Gateway Target with Manager ---
		if gatewayRes.FQDN != nil && *gatewayRes.FQDN != "" {
			// Pass the whole FrontingResult; it implements DnsTarget.
			altDomainManager.RegisterTarget(altmgr.TargetGateway, &gatewayRes)
		} else {
			awscdk.Annotations_Of(stack).AddWarning(jsii.Sprintf("Gateway primary FQDN is empty. Cannot register target %s.", altmgr.TargetGateway))
		}

		// 2. Indexer Fronting (imports cert)
		idxProps.ImportedCertificate = gatewayRes.Certificate
		iApi := fronting.NewApiGatewayFronting()
		indexerRes := iApi.AttachRoutes(stack, "IndexerFronting", &idxProps)

		// --- Register Indexer Target with Manager ---
		if indexerRes.FQDN != nil && *indexerRes.FQDN != "" {
			altDomainManager.RegisterTarget(altmgr.TargetIndexer, &indexerRes)
		} else {
			awscdk.Annotations_Of(stack).AddWarning(jsii.Sprintf("Indexer primary FQDN is empty. Cannot register target %s.", altmgr.TargetIndexer))
		}

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

	// --- Bind the Alternative Domain Manager ---
	// This call creates the necessary alternative A records in Route 53 based on the
	// registered targets and loaded configuration. SANs should have been collected separately
	// by calling CollectAndAddConfiguredSansToBuilder earlier.
	altDomainManager.Bind()

	// Conditionally attach observability
	if shouldIncludeObserver {
		if observerAsset == nil {
			panic("Observer asset is nil when observer should be included") // Should not happen
		}
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
