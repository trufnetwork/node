package stacks

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/observer"
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
	if !config.IsStackInSynthesis(stack) {
		return stack
	}

	initElements := []awsec2.InitElement{}
	shouldIncludeObserver := config.GetEnvironmentVariables[config.AutoStackEnvironmentVariables](stack).IncludeObserver

	if shouldIncludeObserver {
		observerAsset := observer.GetObserverAsset(stack, jsii.String("observer"))

		initObserver := awsec2.InitFile_FromExistingAsset(jsii.String(observer.ObserverZipAssetDir), observerAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		})

		initElements = append(initElements, initObserver)
	}

	tsnStack := TsnStack(stack, &TsnStackProps{
		certStackExports: props.CertStackExports,
		clusterProvider: cluster.AutoTsnClusterProvider{
			NumberOfNodes: config.NumOfNodes(stack),
		},
		InitElements: initElements,
	})

	if shouldIncludeObserver {
		observer.AttachObservability(stack, &observer.AttachObservabilityInput{
			TSNCluster:      tsnStack.TSNCluster,
			KGWInstance:     tsnStack.KGWInstance,
			IndexerInstance: tsnStack.IndexerInstance,
		})
	}

	// create kgw and indexer from launch templates
	// since the intention of tsn_auto_stack is quick development
	utils.InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(
		stack,
		jsii.String("indexer"),
		utils.InstanceFromLaunchTemplateOnPublicSubnetInput{
			LaunchTemplate: tsnStack.IndexerInstance.LaunchTemplate,
			ElasticIp:      tsnStack.IndexerInstance.ElasticIp,
			Vpc:            tsnStack.Vpc,
		},
	)

	utils.InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(
		stack,
		jsii.String("kgw"),
		utils.InstanceFromLaunchTemplateOnPublicSubnetInput{
			LaunchTemplate: tsnStack.KGWInstance.LaunchTemplate,
			ElasticIp:      tsnStack.KGWInstance.ElasticIp,
			Vpc:            tsnStack.Vpc,
		},
	)

	return tsnStack.Stack
}
