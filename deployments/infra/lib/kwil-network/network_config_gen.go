package kwil_network

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"strconv"
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
	env := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)
	nodeKeys := make([]NodeKeys, input.NumberOfNodes)
	peers := make([]peer.TSNPeer, input.NumberOfNodes)
	for i := 0; i < input.NumberOfNodes; i++ {
		nodeKeys[i] = GenerateNodeKeys(scope)
		peers[i] = peer.TSNPeer{
			NodeCometEncodedAddress: nodeKeys[i].NodeId,
			// e.g. staging.node-1.tsn.truflation.com
			Address:        config.Domain(scope, "node-"+strconv.Itoa(i+1)),
			NodeHexAddress: nodeKeys[i].PublicKeyPlainHex,
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
