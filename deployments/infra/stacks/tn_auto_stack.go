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
	altmgr "github.com/trufnetwork/node/infra/lib/constructs/alternativedomainmanager"
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

	// --- Instantiate Alternative Domain Manager & SAN Builder ---
	sanBuilder := altmgr.NewSanListBuilder()
	// The manager handles loading config, registering targets, adding SANs, and creating records.
	altDomainManager := altmgr.NewAlternativeDomainManager(stack, "AltDomainManager", &altmgr.AlternativeDomainManagerProps{
		// ConfigFilePath and StackSuffix are read from context within the manager itself.
		CertSanBuilder: sanBuilder,
		// AlternativeHostedZoneDomainOverride: nil, // Optionally override config zone here.
	})

	// --- Create EC2 Instances for TN Nodes and Register Targets with Manager ---
	for _, node := range vs.Nodes {
		instanceId := jsii.Sprintf("TNNodeInstance-%d", node.Index)
		// Create the instance (we don't need the returned object here anymore).
		utils.InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(
			stack,      // Scope
			instanceId, // Unique construct ID for the instance
			utils.InstanceFromLaunchTemplateOnPublicSubnetInput{
				LaunchTemplate: node.LaunchTemplate,
				ElasticIp:      node.ElasticIp,
				Vpc:            vpc,
			},
		)

		// --- Register Node Target with Manager ---
		nodeTargetID := altmgr.NodeTargetID(node.Index) // Use helper for consistent ID.
		primaryFqdn := node.PeerConnection.Address
		// Ensure we have the necessary info before creating and registering the target.
		if primaryFqdn == nil || *primaryFqdn == "" {
			awscdk.Annotations_Of(stack).AddWarning(jsii.Sprintf("Node %d primary FQDN (PeerConnection.Address) is empty. Cannot register target %s.", node.Index+1, nodeTargetID))
		} else if node.ElasticIp == nil {
			awscdk.Annotations_Of(stack).AddWarning(jsii.Sprintf("Node %d ElasticIp is nil. Cannot register target %s.", node.Index+1, nodeTargetID))
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
			Value:       gatewayRes.Certificate.CertificateArn(), // Output the ARN of the shared cert
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

	// AttachObserverPermissions call will be added here later
	if shouldIncludeObserver {
		if observerAsset == nil {
			panic("Observer asset is nil when observer should be included")
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
