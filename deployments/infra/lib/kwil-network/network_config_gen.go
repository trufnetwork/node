package kwil_network

import (
	"fmt"
	"strconv"

	awscdk "github.com/aws/aws-cdk-go/awscdk/v2"
	awss3assets "github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	constructs "github.com/aws/constructs-go/constructs/v10"
	jsii "github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
)

type NetworkConfigInput struct {
	KwilAutoNetworkConfigAssetInput
	ConfigPath string
}

type NetworkConfigOutput struct {
	NodeConfigPaths []string
}

type KwilAutoNetworkConfigAssetInput struct {
	NumberOfNodes int
}

type KwilNetworkConfig struct {
	Asset      awss3assets.Asset
	Connection peer.TSNPeer
}

// KwilNetworkConfigAssetsFromNumberOfNodes generates configuration S3 asset for a network kwil node
// It may be used as a init file mounted into EC2 instances
func KwilNetworkConfigAssetsFromNumberOfNodes(scope constructs.Construct, input KwilAutoNetworkConfigAssetInput) []KwilNetworkConfig {
	// Initialize CDK parameters and DomainConfig
	cdkParams := config.NewCDKParams(scope)
	stageToken := cdkParams.Stage.ValueAsString()
	devPrefix := cdkParams.DevPrefix.ValueAsString()
	// scope should be a Stack for DomainConfig
	stack, ok := scope.(awscdk.Stack)
	if !ok {
		panic(fmt.Sprintf("KwilNetworkConfigAssetsFromNumberOfNodes: expected scope to be awscdk.Stack, got %T", scope))
	}
	// Create HostedDomain to centralize domain logic
	hd := domaincfg.NewHostedDomain(stack, "NetworkDomain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     domaincfg.StageType(*stageToken),
			Sub:       "",         // no leaf subdomain here
			DevPrefix: *devPrefix, // prepend prefix in dev
		},
	})
	// Base domain: use HostedDomain's DomainName
	baseDomain := *hd.DomainName

	// Retrieve environment variables for chain ID
	env := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	nodeKeys := make([]NodeKeys, input.NumberOfNodes)
	peers := make([]peer.TSNPeer, input.NumberOfNodes)
	for i := 0; i < input.NumberOfNodes; i++ {
		nodeKeys[i] = GenerateNodeKeys(scope)
		// Construct full peer FQDN using HostedDomain
		peers[i] = peer.TSNPeer{
			NodeCometEncodedAddress: nodeKeys[i].NodeId,
			Address:                 jsii.String(fmt.Sprintf("node-%d.%s", i+1, baseDomain)),
			NodeHexAddress:          nodeKeys[i].PublicKeyPlainHex,
		}
	}

	genesisFilePath := GenerateGenesisFile(scope, GenerateGenesisFileInput{
		ChainId:         env.ChainId,
		PeerConnections: peers,
	})

	assets := make([]KwilNetworkConfig, input.NumberOfNodes)
	for i := 0; i < input.NumberOfNodes; i++ {
		cfg := GeneratePeerConfig(scope, GeneratePeerConfigInput{
			PrivateKey:      jsii.String(nodeKeys[i].PrivateKeyHex),
			GenesisFilePath: genesisFilePath,
			Peers:           peers,
			CurrentPeer:     peers[i],
		})

		assets[i].Asset = awss3assets.NewAsset(scope, jsii.String("KwilNetworkConfigAsset-"+strconv.Itoa(i)), &awss3assets.AssetProps{
			Path: jsii.String(cfg),
		})
		assets[i].Connection = peers[i]
	}

	return assets
}
