package kwil_network

import (
	"fmt"

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
	NumberOfNodes   int
	DbOwner         string
	GenesisFilePath string
	Params          config.CDKParams
}

type KwilNetworkConfig struct {
	Asset      awss3assets.Asset
	Connection peer.TNPeer
}

// KwilNetworkConfigAssetsFromNumberOfNodes generates peer information and the genesis file asset.
// It no longer generates individual node config files, as that's handled by templating.
func KwilNetworkConfigAssetsFromNumberOfNodes(scope constructs.Construct, input KwilAutoNetworkConfigAssetInput) ([]peer.TNPeer, []NodeKeys, awss3assets.Asset) {
	// Initialize CDK parameters and DomainConfig
	stage := config.GetStage(scope)
	devPrefix := config.GetDevPrefix(scope)
	stack, ok := scope.(awscdk.Stack)
	if !ok {
		panic(fmt.Sprintf("KwilNetworkConfigAssetsFromNumberOfNodes: expected scope to be awscdk.Stack, got %T", scope))
	}
	hd := domaincfg.NewHostedDomain(stack, "NetworkDomain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     stage,
			Sub:       "",
			DevPrefix: devPrefix,
		},
	})
	baseDomain := *hd.DomainName

	env := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	// Generate Node Keys and Peer Info
	nodeKeys := make([]NodeKeys, input.NumberOfNodes)
	peers := make([]peer.TNPeer, input.NumberOfNodes)
	for i := 0; i < input.NumberOfNodes; i++ {
		nodeKeys[i] = GenerateNodeKeys(scope)
		peers[i] = peer.TNPeer{
			NodeId:         nodeKeys[i].NodeId,
			Address:        jsii.String(fmt.Sprintf("node-%d.%s", i+1, baseDomain)),
			NodeHexAddress: nodeKeys[i].PublicKeyHex,
		}
	}

	var genesisAsset awss3assets.Asset

	// Either generate a genesis file or use the provided one
	if input.GenesisFilePath != "" {
		genesisAsset = awss3assets.NewAsset(scope, jsii.String("GenesisFileAsset"), &awss3assets.AssetProps{
			Path: jsii.String(input.GenesisFilePath), // Path to the provided genesis.json
		})
	} else if input.DbOwner != "" {
		genesisFilePath := GenerateGenesisFile(scope, GenerateGenesisFileInput{
			ChainId:         env.ChainId,
			PeerConnections: peers, // Pass peers to include validators in genesis
			DbOwner:         input.DbOwner,
		})

		// Create Genesis Asset
		genesisAsset = awss3assets.NewAsset(scope, jsii.String("GenesisFileAsset"), &awss3assets.AssetProps{
			Path: jsii.String(genesisFilePath), // Path to the generated genesis.json
		})
	} else {
		panic("DbOwner or GenesisFilePath must be provided")
	}

	// Return the list of peers, the corresponding node keys, and the single genesis asset
	return peers, nodeKeys, genesisAsset
}
