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
	NewTSNClusterInput
	GenesisFilePath string
	PrivateKeys     []*string
}

func TsnClusterFromConfig(scope awscdk.Stack, input TsnClusterFromConfigInput) TSNCluster {
	numOfNodes := len(input.PrivateKeys)

	// Generate TSNPeer for each node
	peerConnections := make([]peer.TSNPeer, numOfNodes)
	for i := 0; i < numOfNodes; i++ {
		keys := kwil_network.ExtractKeys(*input.PrivateKeys[i])
		peerConnections[i] = peer.TSNPeer{
			NodeCometEncodedAddress: keys.NodeId,
			Address:                 config.Domain(scope, fmt.Sprintf("node-%d", i+1)),
		}
	}

	// Generate KwilNetworkConfig for each node
	nodesConfig := make([]kwil_network.KwilNetworkConfig, numOfNodes)
	for i := 0; i < numOfNodes; i++ {
		configDir := kwil_network.GeneratePeerConfig(scope, kwil_network.GeneratePeerConfigInput{
			CurrentPeer:     peerConnections[i],
			Peers:           peerConnections,
			PrivateKey:      input.PrivateKeys[i],
			GenesisFilePath: input.GenesisFilePath,
		})

		nodesConfig[i] = kwil_network.KwilNetworkConfig{
			Asset: awss3assets.NewAsset(scope, jsii.String(fmt.Sprintf("TSNConfigAsset-%d", i)), &awss3assets.AssetProps{
				Path: jsii.String(configDir),
			}),
			Connection: peerConnections[i],
		}
	}

	// Create NewTSNClusterInput
	newTSNClusterInput := NewTSNClusterInput{
		NodesConfig:           nodesConfig,
		TSNDockerComposeAsset: input.TSNDockerComposeAsset,
		TSNDockerImageAsset:   input.TSNDockerImageAsset,
		TSNConfigImageAsset:   input.TSNConfigImageAsset,
		Vpc:                   input.Vpc,
	}

	// Call NewTSNCluster
	return NewTSNCluster(scope, newTSNClusterInput)
}
