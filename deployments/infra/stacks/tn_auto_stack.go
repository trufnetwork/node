package stacks

import (
	"fmt"

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
	cdklogger.LogInfo(stack, "", "Loaded environment variables for TnAutoStack. Key vars: DB_OWNER=%s, CHAIN_ID=%s, KWILD_CLI_PATH=%s, CDK_DOCKER=%s, IncludeObserver=%t. SessionSecret is loaded (value masked).",
		autoEnvVars.DbOwner, autoEnvVars.ChainId, autoEnvVars.KwildCliPath, autoEnvVars.CdkDocker, autoEnvVars.IncludeObserver)

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

	// --- Instantiate Alternative Domain Manager ---
	// The manager handles loading config, registering targets, adding SANs, and creating records.
	altDomainManager := altmgr.NewAlternativeDomainManager(stack, "AltDomainManager", &altmgr.AlternativeDomainManagerProps{
		// ConfigFilePath and StackSuffix are read from context within the manager itself.
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
			cdklogger.LogWarning(stack, "", "Node %d primary FQDN (PeerConnection.Address) is empty. Cannot register target %s.", node.Index+1, nodeTargetID)
		} else if node.ElasticIp == nil {
			cdklogger.LogWarning(stack, "", "Node %d ElasticIp is nil. Cannot register target %s.", node.Index+1, nodeTargetID)
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
		spec := domain.Spec{
			Stage:     domain.StageType(stage),
			Sub:       "",
			DevPrefix: devPrefix,
		}
		gatewayPrimaryFqdn := spec.Subdomain("gateway") // RENAMED from gatewayRecord for clarity
		indexerPrimaryFqdn := spec.Subdomain("indexer") // RENAMED from indexerRecord for clarity

		// Build certificate validation using manager helper

		certPrimaryDomain, certSans, certValidation, err := altDomainManager.GetCertificateRequirements(
			hd.Zone, // Stack's primary hosted zone
			gatewayPrimaryFqdn,
			indexerPrimaryFqdn,
		)
		if err != nil {
			panic(fmt.Sprintf("Failed to get certificate requirements from ADM: %v", err))
		}

		// Create the single shared certificate based on ADM's requirements
		sharedCert := awscertificatemanager.NewCertificate(stack, jsii.String("SharedDomainsCert"), &awscertificatemanager.CertificateProps{
			DomainName:              certPrimaryDomain,
			SubjectAlternativeNames: &certSans, // Note: Ensure GetCertificateRequirements returns &[]*string or handle accordingly
			Validation:              certValidation,
		})

		// Prepare shared fronting props
		gwProps, idxProps := fronting.GetSharedCertProps(hd.Zone, *gatewayPrimaryFqdn, *indexerPrimaryFqdn)
		// The certificate itself is now created *before* these props are fully built.

		gwProps.ImportedCertificate = sharedCert // Use the cert we just made for the *primary* API GW domain
		gwProps.PrimaryDomainName = gatewayPrimaryFqdn
		gwProps.Endpoint = kc.Gateway.GatewayFqdn

		idxProps.ImportedCertificate = sharedCert // Indexer uses the same cert for its *primary* API GW domain
		idxProps.PrimaryDomainName = indexerPrimaryFqdn
		idxProps.Endpoint = kc.Indexer.IndexerFqdn

		// 1. Gateway Fronting (Attaches primary custom domain & A-record)
		gApi := fronting.NewApiGatewayFronting()
		gatewayFrontingResult := gApi.AttachRoutes(stack, "GatewayFronting", &gwProps)

		// 2. Indexer Fronting (Attaches primary custom domain & A-record)
		// idxProps.ImportedCertificate = gatewayRes.Certificate //This was old logic if cert created by gwFronting
		iApi := fronting.NewApiGatewayFronting()
		indexerFrontingResult := iApi.AttachRoutes(stack, "IndexerFronting", &idxProps)

		// --- Register Gateway/Indexer FrontingResults with ADM ---
		altDomainManager.RegisterTarget(altmgr.TargetGateway, &gatewayFrontingResult)
		altDomainManager.RegisterTarget(altmgr.TargetIndexer, &indexerFrontingResult)

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
			Value:       sharedCert.CertificateArn(), // Output the ARN of the shared cert
			Description: jsii.String("ARN of the regional ACM certificate used for API Gateway TLS"),
		})

		// --- Provision all alternative domains using ADM ---
		err = altDomainManager.ProvisionAlternativeDomains(sharedCert)
		if err != nil {
			panic(fmt.Sprintf("Failed to provision alternative domains: %v", err))
		}
	} else {
		// Handle other fronting types (ALB, CloudFront)
		// Currently, the dual endpoint setup is only implemented for API Gateway
		// If alternative domains are needed for non-API Gateway fronting, ProvisionAlternativeDomains might need adjustments
		// or to be called with a nil certificate if it can handle that for non-API GW targets.
		// For now, assuming non-API GW types don't use this specific alternative domain shared cert flow.
		// If Node alternatives are always desired, ProvisionAlternativeDomains(nil) could be called outside.
		// However, the user prompt focused on consolidating the existing APIGW alternative logic.

		// If there are non-APIGW targets that need alternative domains (e.g. nodes) and these
		// should be provisioned regardless of fronting type, this call might need to be
		// outside this if block, and ProvisionAlternativeDomains would need to handle a nil certificate
		// gracefully for targets that don't require one (like Nodes).
		// For now, keeping it tied to the API Gateway fronting path as that was the focus of the refactor.
		panic(fmt.Sprintf("Dual endpoint fronting setup not implemented for type: %s. Alternative domain provisioning for this type also needs review.", selectedKind))
	}

	// altDomainManager.Bind() // REMOVED: Old method, replaced by ProvisionAlternativeDomains

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
