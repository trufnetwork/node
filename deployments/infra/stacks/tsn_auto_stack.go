package stacks

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/tsn/cluster"
)

type TsnAutoStackProps struct {
	awscdk.StackProps
	Cert awscertificatemanager.Certificate
}

func TsnAutoStack(scope constructs.Construct, id string, props *TsnAutoStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)

	return TsnStack(stack, &TsnStackProps{
		cert: props.Cert,
		clusterProvider: cluster.AutoTsnClusterProvider{
			NumberOfNodes: config.NumOfNodes(stack),
			IdHash:        config.GetEnvironmentVariables[config.AutoStackEnvironmentVariables](stack).RestartHash,
		},
	})
}
