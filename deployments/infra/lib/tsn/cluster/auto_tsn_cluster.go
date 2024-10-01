package cluster

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	kwil_network "github.com/truflation/tsn-db/infra/lib/kwil-network"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type AutoTsnClusterProvider struct {
	NumberOfNodes int
}

var _ TSNClusterProvider = (*AutoTsnClusterProvider)(nil)

func (t AutoTsnClusterProvider) CreateCluster(scope awscdk.Stack, input NewTSNClusterInput) TSNCluster {
	input.NodesConfig = kwil_network.KwilNetworkConfigAssetsFromNumberOfNodes(scope, kwil_network.KwilAutoNetworkConfigAssetInput{
		NumberOfNodes: t.NumberOfNodes,
	})

	cluster := NewTSNCluster(scope, input)

	// auto tsn cluster also creates the instance itself
	for idx, node := range cluster.Nodes {
		utils.InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(
			scope,
			jsii.Sprintf(
				"node-%d",
				idx,
			), utils.InstanceFromLaunchTemplateOnPublicSubnetInput{
				LaunchTemplate: node.LaunchTemplate,
				ElasticIp:      node.ElasticIp,
				Vpc:            input.Vpc,
			})
	}

	return cluster
}
