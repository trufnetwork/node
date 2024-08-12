package cluster

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/truflation/tsn-db/infra/lib/kwil-network"
	"github.com/truflation/tsn-db/infra/lib/tsn"
)

type NewAutoTSNClusterInput struct {
	NewTSNClusterInput
	NumberOfNodes int
}

type TSNCluster struct {
	Nodes         []tsn.TSNInstance
	Role          awsiam.IRole
	SecurityGroup awsec2.SecurityGroup
}

func NewAutoTSNCluster(scope awscdk.Stack, input NewAutoTSNClusterInput) TSNCluster {
	// to be safe, let's create a reasonable ceiling for the number of nodes
	if input.NumberOfNodes > 5 {
		panic("Number of nodes limited to 5 to prevent typos")
	}

	input.NewTSNClusterInput.NodesConfig = kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(scope, kwil_network.KwilAutoNetworkConfigAssetInput{
		NumberOfNodes: input.NumberOfNodes,
	})

	return NewTSNCluster(scope, input.NewTSNClusterInput)
}

type NewConfigTSNClusterInput struct {
	NewAutoTSNClusterInput
	NodePrivateKeys []*string
}
