package cluster

import (
	"strconv"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/jsii-runtime-go"
	kwil_network "github.com/truflation/tsn-db/infra/lib/kwil-network"
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

	tsnSubnetId := *input.Vpc.SelectSubnets(&awsec2.SubnetSelection{
		SubnetType: awsec2.SubnetType_PUBLIC,
	}).SubnetIds

	// auto tsn cluster also creates the instance itself
	for _, node := range cluster.Nodes {
		instance := awsec2.NewCfnInstance(scope, jsii.String("Auto-TSN-Instance-"+strconv.Itoa(node.Index)), &awsec2.CfnInstanceProps{
			LaunchTemplate: &awsec2.CfnInstance_LaunchTemplateSpecificationProperty{
				LaunchTemplateId: node.LaunchTemplate.LaunchTemplateId(),
				Version:          node.LaunchTemplate.LatestVersionNumber(),
			},
			SubnetId: tsnSubnetId[0],
		})

		instance.AddDependsOn(node.LaunchTemplate.Node().DefaultChild().(awscdk.CfnResource))

		awsec2.NewCfnEIPAssociation(scope, jsii.String("Auto-TSN-EIP-Association-"+strconv.Itoa(node.Index)), &awsec2.CfnEIPAssociationProps{
			InstanceId:   instance.AttrInstanceId(),
			AllocationId: node.ElasticIp.AttrAllocationId(),
		})
	}

	return cluster
}
