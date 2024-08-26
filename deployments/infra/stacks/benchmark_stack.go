package stacks

import (
	"fmt"

	// AWS CDK imports
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsstepfunctions"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	// Local imports
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/utils/asset"
)

// Type definitions
type (
	CreateStateMachineInput struct {
		LaunchTemplatesMap map[awsec2.InstanceType]CreateLaunchTemplateOutput
		BinaryS3Asset      awscdk.IAsset
		ResultsBucket      awss3.IBucket
	}

	CreateLaunchTemplateInput struct {
		ID            string
		InstanceType  awsec2.InstanceType
		BinaryS3Asset awss3assets.Asset
		SecurityGroup awsec2.ISecurityGroup
		IAMRole       awsiam.IRole
		KeyPair       awsec2.IKeyPair
	}

	CreateLaunchTemplateOutput struct {
		LaunchTemplate      awsec2.LaunchTemplate
		InstanceType        awsec2.InstanceType
		BenchmarkBinaryPath string
	}

	CreateWorkflowInput struct {
		LaunchTemplate  awsec2.LaunchTemplate
		BinaryS3Asset   awscdk.IAsset
		ResultsBucket   awss3.IBucket
		CurrentTimeTask awsstepfunctions.IChainable
	}
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

// S3 related functions
func createBucket(scope constructs.Construct, name string) awss3.IBucket {
	return awss3.NewBucket(scope, jsii.String(name), &awss3.BucketProps{
		// private
		PublicReadAccess: jsii.Bool(false),
		BucketName:       jsii.String(name),
	})
}

// EC2 related functions
func createLaunchTemplate(scope constructs.Construct, input CreateLaunchTemplateInput) CreateLaunchTemplateOutput {
	launchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String(input.ID), &awsec2.LaunchTemplateProps{
		InstanceType:  input.InstanceType,
		SecurityGroup: input.SecurityGroup,
		KeyPair:       input.KeyPair,
	})

	instanceType := input.InstanceType.ToString()

	launchTemplate.UserData().AddCommands(
		*jsii.Strings(
			// we need to tell what type of instance this is
			fmt.Sprintf("INSTANCE_TYPE=%s", *instanceType),
			"echo INSTANCE_TYPE=$INSTANCE_TYPE >> /etc/environment",
		)...,
	)

	benchmarkBinaryPath := "/home/ec2-user/benchmark"

	// copy the binary from S3
	launchTemplate.UserData().AddCommands(
		*jsii.Strings(
			fmt.Sprintf("aws s3 cp s3://%s/%s %s",
				*input.BinaryS3Asset.S3BucketName(),
				*input.BinaryS3Asset.S3ObjectKey(),
				benchmarkBinaryPath,
			),
			fmt.Sprintf("chmod +x %s", benchmarkBinaryPath),
		)...,
	)

	return CreateLaunchTemplateOutput{
		LaunchTemplate:      launchTemplate,
		InstanceType:        input.InstanceType,
		BenchmarkBinaryPath: benchmarkBinaryPath,
	}
}

// Step Functions related functions
func createStateMachine(scope constructs.Construct, input CreateStateMachineInput) awsstepfunctions.StateMachine {
	var workflows []awsstepfunctions.IChainable

	// get timestamp should be the first step in the workflow
	// i.e.
	// getCurrentTime -> parallel(workflows)

	// todo: getCurrentTimeTask should be a task that gets the current time
	var getCurrentTimeTask awsstepfunctions.IChainable

	for _, launchTemplate := range input.LaunchTemplatesMap {
		workflow := createWorkflow(scope, CreateWorkflowInput{
			LaunchTemplate:  launchTemplate.LaunchTemplate,
			BinaryS3Asset:   input.BinaryS3Asset,
			ResultsBucket:   input.ResultsBucket,
			CurrentTimeTask: getCurrentTimeTask,
		})
		workflows = append(workflows, workflow)
	}

	mainWorkflow := parallelizeWorkflows(scope, workflows)

	// Create the state machine with a timeout to prevent long-running or stuck executions
	stateMachine := awsstepfunctions.NewStateMachine(scope, jsii.String("BenchmarkStateMachine"), &awsstepfunctions.StateMachineProps{
		Definition: mainWorkflow,
		Timeout:    awscdk.Duration_Minutes(jsii.Number(30)),
	})

	return stateMachine
}

func createWorkflow(scope constructs.Construct, input CreateWorkflowInput) awsstepfunctions.IChainable {
	// Implement the complete workflow
	// Steps to consider:
	// 1. Create EC2 instance, using timestamp as input
	// 2. Wait for instance to be ready
	// 3. Copy benchmark binary from S3
	// 4. Run benchmark tests
	// 5. Export results to S3
	// 6. Terminate EC2 instance
	// 7. Handle errors and retries

	return nil
}

func parallelizeWorkflows(scope constructs.Construct, workflows []awsstepfunctions.IChainable) awsstepfunctions.IChainable {
	return awsstepfunctions.NewParallel(scope, jsii.String("ParallelWorkflow"), &awsstepfunctions.ParallelProps{
		// Configure parallel execution
		// Consider:
		// - Error handling strategy
		// - Result aggregation
	})
}

// Lambda related functions
func createTimestampLambda() awslambda.IFunction {
	// Implement Lambda function creation
	// Consider:
	// - Runtime selection (e.g., Go, Python)
	// - Minimal IAM permissions
	// - Function timeout
	return nil
}
