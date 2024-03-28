package config

import (
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/domain_utils"
)

// DO NOT modify this function, change stack name by 'cdk.json/context/stackName'.
func StackName(scope constructs.Construct) string {
	stackName := "MyEKSClusterStack"

	ctxValue := scope.Node().TryGetContext(jsii.String("stackName"))
	if v, ok := ctxValue.(string); ok {
		stackName = v
	}

	return stackName
}

// DO NOT modify this function, change EC2 key pair name by 'cdk.json/context/keyPairName'.
func KeyPairName(scope constructs.Construct) string {
	keyPairName := "MyKeyPair"

	ctxValue := scope.Node().TryGetContext(jsii.String("keyPairName"))
	if v, ok := ctxValue.(string); ok {
		keyPairName = v
	}

	return keyPairName
}

// DO NOT modify this function, change ECR repository name by 'cdk.json/context/imageRepoName'.
func EcrRepoName(scope constructs.Construct) string {
	ecrRepoName := "MyRepository"

	ctxValue := scope.Node().TryGetContext(jsii.String("imageRepoName"))
	if v, ok := ctxValue.(string); ok {
		ecrRepoName = v
	}

	return ecrRepoName
}

// Deployment stage config
type DeploymentStageType string

const (
	DeploymentStage_DEV     DeploymentStageType = "DEV"
	DeploymentStage_STAGING DeploymentStageType = "STAGING"
	DeploymentStage_PROD    DeploymentStageType = "PROD"
)

// DO NOT modify this function, change EKS cluster name by 'cdk-cli-wrapper-dev.sh/--context deploymentStage='.
func DeploymentStage(scope constructs.Construct) DeploymentStageType {
	deploymentStage := DeploymentStage_PROD

	ctxValue := scope.Node().TryGetContext(jsii.String("deploymentStage"))
	if v, ok := ctxValue.(string); ok {
		deploymentStage = DeploymentStageType(v)
	}

	return deploymentStage
}

func Domain(scope constructs.Construct) *string {
	domainEnvMap := map[DeploymentStageType]string{
		DeploymentStage_DEV:     "dev." + domain_utils.MainDomain,
		DeploymentStage_STAGING: "staging." + domain_utils.MainDomain,
		DeploymentStage_PROD:    domain_utils.MainDomain,
	}

	return jsii.String(domainEnvMap[DeploymentStage(scope)])
}
