package stacks

import (
	"fmt"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
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

	// --- Instantiate Alternative Domain Manager ---
	altDomainManager := altmgr.NewAlternativeDomainManager(stack, "AltDomainManager", &altmgr.AlternativeDomainManagerProps{})

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
			BaseDomainFqdn:  hd.DomainName,
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
			cdklogger.LogWarning(stack, "", "Node %d primary FQDN (PeerConnection.Address) is empty. Cannot register target %s.", node.Index+1, nodeTargetID)
			continue
		}

		// NOTE: Accessing node.ElasticIp.Ref() here assumes that the ValidatorSet construct
		// internally creates and associates an Elastic IP with each NodeInfo, even though
		// the EC2 instance itself might be created elsewhere (unlike tn_auto_stack).
		// This works if ValidatorSet consistently populates NodeInfo.ElasticIp.
		// A more robust solution might involve ValidatorSet explicitly returning EIP Refs.
		if node.ElasticIp == nil {
			cdklogger.LogWarning(stack, "", "Node %d ElasticIp is nil in NodeInfo. Cannot register target %s.", node.Index+1, nodeTargetID)
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

	// Declare sharedCert here to be accessible for ProvisionAlternativeDomains if moved outside the 'if' block later.
	var sharedCert awscertificatemanager.ICertificate

	if selectedKind == fronting.KindAPI {
		// Dual API Gateway setup specific logic
		gatewayPrimaryFqdn := spec.Subdomain("gateway")
		indexerPrimaryFqdn := spec.Subdomain("indexer")

		// Build SAN list and DNS validation method
		certPrimaryDomain, certSans, certValidation, err := altDomainManager.GetCertificateRequirements(
			hd.Zone, // Stack's primary hosted zone
			gatewayPrimaryFqdn,
			indexerPrimaryFqdn,
		)
		if err != nil {
			panic(fmt.Sprintf("Failed to get certificate requirements from ADM: %v", err))
		}

		// Create the single shared certificate based on ADM's requirements
		sharedCert = awscertificatemanager.NewCertificate(stack, jsii.String("SharedDomainsCert"), &awscertificatemanager.CertificateProps{
			DomainName:              certPrimaryDomain,
			SubjectAlternativeNames: &certSans,
			Validation:              certValidation,
		})

		// Prepare shared fronting props
		gwProps, idxProps := fronting.GetSharedCertProps(hd.Zone, *gatewayPrimaryFqdn, *indexerPrimaryFqdn)
		gwProps.ImportedCertificate = sharedCert
		gwProps.PrimaryDomainName = gatewayPrimaryFqdn
		gwProps.Endpoint = kc.Gateway.GatewayFqdn

		idxProps.ImportedCertificate = sharedCert
		idxProps.PrimaryDomainName = indexerPrimaryFqdn
		idxProps.Endpoint = kc.Indexer.IndexerFqdn

		// 1. Gateway Fronting
		gApi := fronting.NewApiGatewayFronting()
		gatewayFrontingResult := gApi.AttachRoutes(stack, "GatewayFronting", &gwProps)

		// 2. Indexer Fronting
		iApi := fronting.NewApiGatewayFronting()
		indexerFrontingResult := iApi.AttachRoutes(stack, "IndexerFronting", &idxProps)

		// --- Register Gateway/Indexer FrontingResults with ADM ---
		altDomainManager.RegisterTarget(altmgr.TargetGateway, &gatewayFrontingResult)
		altDomainManager.RegisterTarget(altmgr.TargetIndexer, &indexerFrontingResult)

		// --- Provision all alternative domains using ADM ---
		err = altDomainManager.ProvisionAlternativeDomains(sharedCert)
		if err != nil {
			panic(fmt.Sprintf("Failed to provision alternative domains: %v", err))
		}

		// --- Outputs ---
		awscdk.NewCfnOutput(stack, jsii.String("GatewayEndpoint"), &awscdk.CfnOutputProps{
			Value:       gatewayFrontingResult.FQDN,
			Description: jsii.String("Public FQDN for the Kwil Gateway API"),
		})
		awscdk.NewCfnOutput(stack, jsii.String("IndexerEndpoint"), &awscdk.CfnOutputProps{
			Value:       indexerFrontingResult.FQDN,
			Description: jsii.String("Public FQDN for the Kwil Indexer API"),
		})
		awscdk.NewCfnOutput(stack, jsii.String("ApiCertArn"), &awscdk.CfnOutputProps{
			Value:       sharedCert.CertificateArn(),
			Description: jsii.String("ARN of the regional ACM certificate used for API Gateway TLS"),
		})
	} else {
		// Handle other fronting types (ALB, CloudFront)
		// Currently, the dual endpoint setup is only implemented for API Gateway
		// As in tn_auto_stack, if alternative domains (especially for Nodes) are needed for other fronting types,
		// this logic would need adjustment. For now, keeping ProvisionAlternativeDomains within this block.
		panic(fmt.Sprintf("Dual endpoint fronting setup not implemented for type: %s. Alternative domain provisioning for this type also needs review.", selectedKind))
	}

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
