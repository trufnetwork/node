package cluster

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/truflation/tsn-db/infra/lib/kwil-network"
)

type AutoTsnClusterProvider struct {
	NumberOfNodes int
	// Controls the restart of the instance when the hash changes.
	IdHash string
}

var _ TSNClusterProvider = (*AutoTsnClusterProvider)(nil)

func (t AutoTsnClusterProvider) CreateCluster(scope awscdk.Stack, input NewTSNClusterInput) TSNCluster {
	input.NodesConfig = kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(scope, kwil_network.KwilAutoNetworkConfigAssetInput{
		NumberOfNodes: t.NumberOfNodes,
	})

	input.IdHash = t.IdHash

	return NewTSNCluster(scope, input)
}
