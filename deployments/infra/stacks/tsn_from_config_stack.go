package stacks

import (
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/observer"
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
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops) // if it's not being synthesized, return the stack
	if !config.IsStackInSynthesis(stack) {
		return stack
	}

	cfg := config.GetEnvironmentVariables[config.ConfigStackEnvironmentVariables](stack)
	genesisFilePath := cfg.GenesisPath

	// from comma separated string to slice
	privateKeys := strings.Split(cfg.NodePrivateKeys, ",")

	// from config always include observer
	observerAsset := observer.GetObserverAsset(stack, jsii.String("observer"))

	initObserver := awsec2.InitFile_FromExistingAsset(jsii.String(observer.ObserverZipAssetDir), observerAsset, &awsec2.InitFileOptions{
		Owner: jsii.String("ec2-user"),
	})

	initElements := []awsec2.InitElement{initObserver}

	tsnStack := TsnStack(stack, &TsnStackProps{
		certStackExports: props.CertStackExports,
		clusterProvider: cluster.TsnClusterFromConfigInput{
			GenesisFilePath: genesisFilePath,
			PrivateKeys:     privateKeys,
		},
		InitElements: initElements,
	})

	observer.AttachObservability(stack, &observer.AttachObservabilityInput{
		TSNCluster:      tsnStack.TSNCluster,
		KGWInstance:     tsnStack.KGWInstance,
		IndexerInstance: tsnStack.IndexerInstance,
	})

	return tsnStack.Stack
}
