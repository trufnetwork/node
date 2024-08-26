package stacks

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsstepfunctions"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// BenchmarkStack creates the main infrastructure for running benchmarks
// across multiple EC2 instance types.
func BenchmarkStack(scope constructs.Construct, id string, props *awscdk.StackProps) {
	stack := awscdk.NewStack(scope, jsii.String(id), props)

	// Create S3 buckets for storing binaries and results
	binaryS3Asset := buildGoBinaryIntoS3Asset(
		stack,
		jsii.String("benchmark-binary"),
		buildGoBinaryIntoS3AssetInput{
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

	// Create EC2 launch templates for each instance type
	launchTemplatesMap := make(map[awsec2.InstanceType]awsec2.LaunchTemplate)
	for _, instanceType := range testedInstances {
		launchTemplatesMap[instanceType] = createLaunchTemplate(
			stack,
			CreateLaunchTemplateInput{
				InstanceType:  instanceType,
				binaryS3Asset: binaryS3Asset,
			},
		)
	}

	// Create the main state machine to orchestrate the benchmark process
	createStateMachine(stack, CreateStateMachineInput{
		launchTemplatesMap: launchTemplatesMap,
		binaryS3Asset:      binaryS3Asset,
		resultsBucket:      resultsBucket,
	})
}

type CreateStateMachineInput struct {
	launchTemplatesMap map[awsec2.InstanceType]awsec2.LaunchTemplate
	binaryS3Asset      awscdk.IAsset
	resultsBucket      awss3.IBucket
}

// createStateMachine sets up the Step Functions state machine that coordinates
// the benchmark workflows for each instance type.
func createStateMachine(
	scope constructs.Construct,
	input CreateStateMachineInput,
) awsstepfunctions.StateMachine {
	var workflows []awsstepfunctions.IChainable

	// get timestamp should be the first step in the workflow
	// i.e.
	// getCurrentTime -> parallel(workflows)

	// todo: getCurrentTimeTask should be a task that gets the current time
	var getCurrentTimeTask awsstepfunctions.IChainable

	for _, launchTemplate := range input.launchTemplatesMap {
		workflow := createWorkflow(scope, CreateWorkflowInput{
			LaunchTemplate:  launchTemplate,
			BinaryS3Asset:   input.binaryS3Asset,
			ResultsBucket:   input.resultsBucket,
			currentTimeTask: getCurrentTimeTask,
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

type CreateLaunchTemplateInput struct {
	InstanceType  awsec2.InstanceType
	binaryS3Asset awscdk.IAsset
}

// createLaunchTemplate generates an EC2 launch template for a given instance type.
// TODO: Implement this function to set up the EC2 instance configuration.
func createLaunchTemplate(scope constructs.Construct, input CreateLaunchTemplateInput) awsec2.LaunchTemplate {
	// Implement EC2 launch template creation
	// Consider:
	// - Appropriate AMI selection
	// - IAM role for EC2 instances with minimal permissions
	// - User data script for instance initialization
	// - Save instance type somewhere to be referenced in the workflow
	return nil
}

// parallelizeWorkflows combines individual instance workflows into a parallel execution.
// TODO: Implement this function to run workflows concurrently.
func parallelizeWorkflows(scope constructs.Construct, workflows []awsstepfunctions.IChainable) awsstepfunctions.IChainable {
	return awsstepfunctions.NewParallel(scope, jsii.String("ParallelWorkflow"), &awsstepfunctions.ParallelProps{
		// Configure parallel execution
		// Consider:
		// - Error handling strategy
		// - Result aggregation
	})
}

// createBucket creates an S3 bucket with appropriate settings.
// TODO: Implement this function with proper S3 bucket configuration.
func createBucket(scope constructs.Construct, name string) awss3.IBucket {
	return awss3.NewBucket(scope, jsii.String(name), &awss3.BucketProps{
		// private
		PublicReadAccess: jsii.Bool(false),
		BucketName:       jsii.String(name),
	})
}

// CreateWorkflowInput defines the input parameters for creating a benchmark workflow.
type CreateWorkflowInput struct {
	LaunchTemplate  awsec2.LaunchTemplate
	BinaryS3Asset   awscdk.IAsset
	ResultsBucket   awss3.IBucket
	currentTimeTask awsstepfunctions.IChainable
}

// createWorkflow builds the Step Functions workflow for a single instance type.
// TODO: Implement the complete workflow with all necessary steps.
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

// createTimestampLambda creates a Lambda function to generate timestamps.
// TODO: Implement this function to create a simple Lambda for timestamp generation.
func createTimestampLambda() awslambda.IFunction {
	// Implement Lambda function creation
	// Consider:
	// - Runtime selection (e.g., Go, Python)
	// - Minimal IAM permissions
	// - Function timeout
	return nil
}
