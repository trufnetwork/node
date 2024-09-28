package stacks

import (
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/tsn/cluster"
)

type TsnFromConfigStackProps struct {
	awscdk.StackProps
	CertStackExports CertStackExports
}

func TsnFromConfigStack(
	scope constructs.Construct,
	id string,
	props *TsnFromConfigStackProps,
) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)

	cfg := config.GetEnvironmentVariables[config.ConfigStackEnvironmentVariables](stack)
	genesisFilePath := cfg.GenesisPath

	// from comma separated string to slice
	privateKeys := strings.Split(cfg.NodePrivateKeys, ",")

	return TsnStack(stack, &TsnStackProps{
		certStackExports: props.CertStackExports,
		clusterProvider: cluster.TsnClusterFromConfigInput{
			GenesisFilePath: genesisFilePath,
			PrivateKeys:     privateKeys,
		},
	}).Stack
}
