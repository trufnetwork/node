package benchmark

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsstepfunctions"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type (
	CreateStateMachineInput struct {
		LaunchTemplatesMap map[awsec2.InstanceType]CreateLaunchTemplateOutput
		BinaryS3Asset      awscdk.IAsset
		ResultsBucket      awss3.IBucket
	}
	CreateWorkflowInput struct {
		LaunchTemplate  awsec2.LaunchTemplate
		BinaryS3Asset   awscdk.IAsset
		ResultsBucket   awss3.IBucket
		CurrentTimeTask awsstepfunctions.IChainable
	}
)

// Step Functions related functions
func createStateMachine(scope constructs.Construct, input CreateStateMachineInput) awsstepfunctions.StateMachine {
	var workflows []awsstepfunctions.IChainable

	// get timestamp should be the first step in the workflow
	// i.e.
	// getCurrentTime -> parallel(workflows)

	// todo: getCurrentTimeTask should be a task that gets the current time
	var getCurrentTimeTask awsstepfunctions.IChainable

	// create workflows for each launch template
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
		DefinitionBody: awsstepfunctions.DefinitionBody_FromChainable(mainWorkflow),
		Timeout:        awscdk.Duration_Minutes(jsii.Number(30)),
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
