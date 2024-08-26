package benchmark

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/utils/asset"
)

// Main stack function
func BenchmarkStack(scope constructs.Construct, id string, props *awscdk.StackProps) {
	stack := awscdk.NewStack(scope, jsii.String(id), props)

	// Create S3 buckets for storing binaries and results
	binaryS3Asset := asset.BuildGoBinaryIntoS3Asset(
		stack,
		jsii.String("benchmark-binary"),
		asset.BuildGoBinaryIntoS3AssetInput{
			BinaryPath: jsii.String("../../../cmd/benchmark/main.go"),
			BinaryName: jsii.String("benchmark"),
		},
	)
	resultsBucket := createBucket(stack, "benchmark-results-"+*stack.StackName())

	// Define the EC2 instance types to be tested
	testedInstances := []awsec2.InstanceType{
		awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_MICRO),
		awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_SMALL),
		awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_MEDIUM),
		awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_LARGE),
	}

	// default vpc
	defaultVPC := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{
		IsDefault: jsii.Bool(true),
	})

	securityGroup := awsec2.NewSecurityGroup(stack, jsii.String("benchmark-security-group"), &awsec2.SecurityGroupProps{
		Vpc: defaultVPC,
	})

	// permit 22 port for ssh
	securityGroup.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(22)),
		jsii.String("Allow SSH access"),
		jsii.Bool(true),
	)

	ec2InstanceRole := awsiam.NewRole(stack, jsii.String("EC2InstanceRole"), &awsiam.RoleProps{})

	// permit write access to the results bucket
	resultsBucket.GrantReadWrite(ec2InstanceRole, "*")

	// Use default key pair
	keyPairName := config.KeyPairName(scope)
	if len(keyPairName) == 0 {
		panic("KeyPairName is empty")
	}

	keyPair := awsec2.KeyPair_FromKeyPairName(stack, jsii.String(keyPairName), jsii.String("benchmark-key-pair"))

	// Create EC2 launch templates for each instance type
	launchTemplatesMap := make(map[awsec2.InstanceType]CreateLaunchTemplateOutput)
	for _, instanceType := range testedInstances {
		launchTemplatesMap[instanceType] = createLaunchTemplate(
			stack,
			CreateLaunchTemplateInput{
				ID:            fmt.Sprintf("benchmark-%s", *instanceType.ToString()),
				InstanceType:  instanceType,
				BinaryS3Asset: binaryS3Asset,
				SecurityGroup: securityGroup,
				IAMRole:       ec2InstanceRole,
				KeyPair:       keyPair,
			},
		)
	}

	// Create the main state machine to orchestrate the benchmark process
	createStateMachine(stack, CreateStateMachineInput{
		LaunchTemplatesMap: launchTemplatesMap,
		BinaryS3Asset:      binaryS3Asset,
		ResultsBucket:      resultsBucket,
	})
}
