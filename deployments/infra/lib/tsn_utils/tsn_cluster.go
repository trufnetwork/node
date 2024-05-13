package tsn_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"strconv"
)

type NewTSNClusterInput struct {
	NumberOfNodes         int
	TSNDockerComposeAsset awss3assets.Asset
	TSNDockerImageAsset   awsecrassets.DockerImageAsset
	Vpc                   awsec2.IVpc
}

type TSNCluster struct {
	Nodes         []TSNInstance
	Role          awsiam.IRole
	SecurityGroup awsec2.SecurityGroup
}

func NewTSNCluster(scope constructs.Construct, input NewTSNClusterInput) TSNCluster {
	// to be safe, let's create a reasonable ceiling for the number of nodes
	if input.NumberOfNodes > 5 {
		panic("Number of nodes limited to 5 to prevent typos")
	}

	securityGroup := NewTSNSecurityGroup(scope, NewTSNSecurityGroupInput{
		vpc: input.Vpc,
	})

	role := awsiam.NewRole(scope, jsii.String("TSN-Cluster-Role"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	instances := make([]TSNInstance, input.NumberOfNodes)
	for i := 0; i < input.NumberOfNodes; i++ {
		instance := NewTSNInstance(scope, newTSNInstanceInput{
			Id:                    strconv.Itoa(i),
			Role:                  role,
			Vpc:                   input.Vpc,
			SecurityGroup:         securityGroup,
			TSNDockerComposeAsset: input.TSNDockerComposeAsset,
			TSNDockerImageAsset:   input.TSNDockerImageAsset,
		})
		instances[i] = instance
	}

	return TSNCluster{
		Nodes:         instances,
		Role:          role,
		SecurityGroup: securityGroup,
	}
}
