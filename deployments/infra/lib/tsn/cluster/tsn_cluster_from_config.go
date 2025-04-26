package cluster

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	kwil_network "github.com/trufnetwork/node/infra/lib/kwil-network"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
)

type TsnClusterFromConfigInput struct {
	GenesisFilePath string
	PrivateKeys     []string
}

var _ TSNClusterProvider = (*TsnClusterFromConfigInput)(nil)

func (t TsnClusterFromConfigInput) CreateCluster(scope awscdk.Stack, input NewTSNClusterInput) TSNCluster {
	// Initialize CDK parameters and HostedDomain for peer FQDNs
	cdkParams := config.NewCDKParams(scope)
	stageToken := cdkParams.Stage.ValueAsString()
	devPrefix := cdkParams.DevPrefix.ValueAsString()
	hd := domaincfg.NewHostedDomain(scope, "Domain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     domaincfg.StageType(*stageToken),
			Sub:       "",
			DevPrefix: *devPrefix,
		},
	})
	baseDomain := *hd.DomainName

	numOfNodes := len(t.PrivateKeys)

	// Generate TSNPeer for each node
	peerConnections := make([]peer.TSNPeer, numOfNodes)
	for i := 0; i < numOfNodes; i++ {
		keys := kwil_network.ExtractKeys(scope, t.PrivateKeys[i])
		// Construct full peer FQDN using HostedDomain
		peerConnections[i] = peer.TSNPeer{
			NodeCometEncodedAddress: keys.NodeId,
			Address:                 jsii.String(fmt.Sprintf("node-%d.%s", i+1, baseDomain)),
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
