package stacks

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/tsn/cluster"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type TsnAutoStackProps struct {
	awscdk.StackProps
	CertStackExports CertStackExports
}

func TsnAutoStack(scope constructs.Construct, id string, props *TsnAutoStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)

	tsnStack := TsnStack(stack, &TsnStackProps{
		certStackExports: props.CertStackExports,
		clusterProvider: cluster.AutoTsnClusterProvider{
			NumberOfNodes: config.NumOfNodes(stack),
		},
	})

	// create kgw and indexer from launch templates
	// since the intention of tsn_auto_stack is quick development
	utils.InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(utils.InstanceFromLaunchTemplateOnPublicSubnetInput{
		Scope:          stack,
		LaunchTemplate: tsnStack.IndexerInstance.LaunchTemplate,
		ElasticIp:      tsnStack.IndexerInstance.ElasticIp,
		Vpc:            tsnStack.Vpc,
	})

	return tsnStack.Stack
}
