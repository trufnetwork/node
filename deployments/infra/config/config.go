package config

import (
	"fmt"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/domain_utils"
	"log"
	"strconv"
)

// Stack suffix is intended to be used after the stack name to differentiate between different stages.
func WithStackSuffix(scope constructs.Construct, stackName string) string {
	stage := GetDomainStage(scope)

	suffix := "-Stack"
	if stage != "" {
		suffix = "-" + string(stage) + suffix
	}

	return stackName + suffix
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

func GetDomainStage(scope constructs.Construct) string {
	stageEnvMap := map[DeploymentStageType]string{
		DeploymentStage_DEV:     "dev",
		DeploymentStage_STAGING: "staging",
		DeploymentStage_PROD:    "",
	}

	// special domain names only work for DEV
	ctxValue := scope.Node().TryGetContext(jsii.String("specialDomain"))

	if v, ok := ctxValue.(string); ok {
		if DeploymentStage(scope) != DeploymentStage_DEV {
			log.Printf("Special domain %s is used in non-DEV stage", v)
		}
		stageEnvMap[DeploymentStage_DEV] = v
	}

	return stageEnvMap[DeploymentStage(scope)]
}

func Domain(scope constructs.Construct, subdomains ...string) *string {
	// goes like this: <stage>.<subdomain#...>.<main_domain>
	domain := GetDomainStage(scope)

	for _, subdomain := range subdomains {
		domain += "." + subdomain
	}

	domain += "." + domain_utils.MainDomain

	return jsii.String(domain)
}

func NumOfNodes(scope constructs.Construct) int {
	numOfNodes := 1

	ctxValue := scope.Node().TryGetContext(jsii.String("numOfNodes"))
	if ctxValue != nil {
		// ctxValue may be a float64 or a string
		switch v := ctxValue.(type) {
		case float64:
			numOfNodes = int(v)
		case string:
			var err error
			numOfNodes, err = strconv.Atoi(v)
			if err != nil {
				panic(fmt.Sprintf("numOfNodes context value is not a number: %s", v))
			}
		}
	}

	return numOfNodes
}
