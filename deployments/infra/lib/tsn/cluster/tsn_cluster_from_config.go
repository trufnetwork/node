package cluster

import (
	"fmt"
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/kwil-network"
	"github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
)

type TsnClusterFromConfigInput struct {
	GenesisFilePath string
	PrivateKeys     []string
}

var _ TSNClusterProvider = (*TsnClusterFromConfigInput)(nil)

func (t TsnClusterFromConfigInput) CreateCluster(scope awscdk.Stack, input NewTSNClusterInput) TSNCluster {
	numOfNodes := len(t.PrivateKeys)

	// Generate TSNPeer for each node
	peerConnections := make([]peer.TSNPeer, numOfNodes)
	for i := 0; i < numOfNodes; i++ {
		keys := kwil_network.ExtractKeys(scope, t.PrivateKeys[i])
		peerConnections[i] = peer.TSNPeer{
			NodeCometEncodedAddress: keys.NodeId,
			Address:                 config.Domain(scope, fmt.Sprintf("node-%d", i+1)),
			NodeHexAddress:          keys.PublicKeyPlainHex,
		}
	}

	// Generate KwilNetworkConfig for each node
	nodesConfig := make([]kwil_network.KwilNetworkConfig, numOfNodes)
	for i := 0; i < numOfNodes; i++ {
		configDir := kwil_network.GeneratePeerConfig(scope, kwil_network.GeneratePeerConfigInput{
			CurrentPeer:     peerConnections[i],
			Peers:           peerConnections,
			PrivateKey:      jsii.String(t.PrivateKeys[i]),
			GenesisFilePath: t.GenesisFilePath,
		})

		nodesConfig[i] = kwil_network.KwilNetworkConfig{
			Asset: awss3assets.NewAsset(scope, jsii.String(fmt.Sprintf("TSNConfigAsset-%d", i)), &awss3assets.AssetProps{
				Path: jsii.String(configDir),
			}),
			Connection: peerConnections[i],
		}
	}

	input.NodesConfig = nodesConfig

	// Call NewTSNCluster
	return NewTSNCluster(scope, input)
}
