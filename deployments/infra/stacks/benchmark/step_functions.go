package benchmark

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsstepfunctions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsstepfunctionstasks"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type (
	CreateStateMachineInput struct {
		LaunchTemplatesMap  map[awsec2.InstanceType]CreateLaunchTemplateOutput
		BinaryS3Asset       awss3assets.Asset
		ResultsBucket       awss3.IBucket
		ExportResultsLambda awslambda.IFunction
	}
	CreateWorkflowInput struct {
		Id                   string
		LaunchTemplateOutput CreateLaunchTemplateOutput
		BinaryS3Asset        awss3assets.Asset
		ResultsBucket        awss3.IBucket
	}
)

// Step Functions related functions
func createStateMachine(scope constructs.Construct, input CreateStateMachineInput) awsstepfunctions.StateMachine {
	var benchmarkWorkflows []awsstepfunctions.IChainable

	// get timestamp should be the first step in the workflow
	// i.e.
	// getCurrentTime -> parallel(workflows)

	// output:
	// - timestamp: <timestamp>
	getCurrentTimeTask := awsstepfunctionstasks.NewLambdaInvoke(scope, jsii.String("GetCurrentTimeTask"), &awsstepfunctionstasks.LambdaInvokeProps{
		LambdaFunction: awslambda.NewFunction(scope, jsii.String("GetCurrentTimeLambda"), &awslambda.FunctionProps{
			Code:    awslambda.Code_FromInline(jsii.String("exports.handler = async () => { return new Date().toISOString(); }")),
			Handler: jsii.String("index.handler"),
			Runtime: awslambda.Runtime_NODEJS_20_X(),
		}),
		ResultSelector: &map[string]interface{}{
			"timestamp": awsstepfunctions.JsonPath_StringAt(jsii.String("$.Payload")),
		},
	})

	// create workflows for each launch template
	for idx, launchTemplate := range input.LaunchTemplatesMap {
		workflow := createBenchmarkWorkflow(scope, CreateWorkflowInput{
			Id:                   fmt.Sprintf("%s-%d", *launchTemplate.LaunchTemplate.LaunchTemplateId(), idx),
			LaunchTemplateOutput: launchTemplate,
			BinaryS3Asset:        input.BinaryS3Asset,
			ResultsBucket:        input.ResultsBucket,
		})
		benchmarkWorkflows = append(benchmarkWorkflows, workflow)
	}

	parallelBenchmarks := parallelizeWorkflows(scope, benchmarkWorkflows)

	// export results to s3 using our custom lambda
	exportResultsTask := awsstepfunctionstasks.NewLambdaInvoke(scope, jsii.String("ExportResultsTask"), &awsstepfunctionstasks.LambdaInvokeProps{
		LambdaFunction: input.ExportResultsLambda,
		// pass the bucket and key to the lambda function as a json object
		Payload: awsstepfunctions.TaskInput_FromObject(
			&map[string]interface{}{
				"bucket": input.ResultsBucket.BucketName(),
				"key": awsstepfunctions.JsonPath_Format(
					jsii.String("{}/{}.csv"),
					input.ResultsBucket.BucketName(),
					awsstepfunctions.JsonPath_StringAt(jsii.String("$.timestamp")),
				),
			},
		),
	})

	benchmarkWorkflowChain := awsstepfunctions.Chain_Start(getCurrentTimeTask).
		Next(parallelBenchmarks).
		Next(exportResultsTask)

	// Create a task to terminate EC2 instances
	terminateAllEc2Instances := awsstepfunctionstasks.NewCallAwsService(scope, jsii.String("TerminateAllEC2Instances"), &awsstepfunctionstasks.CallAwsServiceProps{
		Service: jsii.String("ec2"),
		Action:  jsii.String("terminateInstances"),
		IamResources: jsii.Strings(
			"arn:aws:ec2:*:*:instance/*",
		),
		Parameters: &map[string]interface{}{
			"InstanceIds": awsstepfunctions.JsonPath_ListAt(jsii.String("$.parallelResults[?(@.error)].error.ec2Instance.InstanceId")),
		},
	})

	// Create an error handler that terminates instances and then fails
	errorHandler := awsstepfunctions.Chain_Start(terminateAllEc2Instances).
		Next(awsstepfunctions.NewFail(scope, jsii.String("StateMachineErrorHandler"), &awsstepfunctions.FailProps{
			Cause: jsii.String("State Machine execution failed"),
			Error: jsii.String("StateMachineFailed"),
		}))

	parallelBenchmarks.AddCatch(errorHandler, &awsstepfunctions.CatchProps{
		ResultPath: jsii.String("$.error"),
	})

	stateMachine := awsstepfunctions.NewStateMachine(scope, jsii.String("BenchmarkStateMachine"), &awsstepfunctions.StateMachineProps{
		DefinitionBody: awsstepfunctions.DefinitionBody_FromChainable(benchmarkWorkflowChain),
		Timeout:        awscdk.Duration_Minutes(jsii.Number(30)),
	})

	return stateMachine
}

func createBenchmarkWorkflow(scope constructs.Construct, input CreateWorkflowInput) awsstepfunctions.IChainable {
	// Create EC2 instance using AWS SDK Service Integrations
	// see https://repost.aws/questions/QUzl5DGCU0Reazk8ov8oyY5Q/how-to-run-ec2-instance-with-step-function
	// Output:
	// - ec2Instance: {
	//   - InstanceId
	//   - InstanceType
	// }
	// - timestamp: <timestamp>
	createEC2Task := awsstepfunctionstasks.NewCallAwsService(
		scope,
		jsii.String("CreateEC2Instance"+input.Id),
		&awsstepfunctionstasks.CallAwsServiceProps{
			Service: jsii.String("ec2"),
			Action:  jsii.String("runInstances"),
			Parameters: &map[string]interface{}{
				"LaunchTemplate": map[string]interface{}{
					"LaunchTemplateId": input.LaunchTemplateOutput.LaunchTemplate.LaunchTemplateId(),
					"Version":          "$Latest",
				},
				"MinCount": 1,
				"MaxCount": 1,
			},
			IamResources: jsii.Strings(
				// just enough to run instances
				fmt.Sprintf("arn:aws:ec2:*:*:launch-template/%s", *input.LaunchTemplateOutput.LaunchTemplate.LaunchTemplateId()),
				"arn:aws:ec2:*:*:instance/*",
			),
			ResultPath: jsii.String("$.ec2Instance"),
			ResultSelector: &map[string]interface{}{
				"InstanceId":   awsstepfunctions.JsonPath_StringAt(jsii.String("$.Instances[0].InstanceId")),
				"InstanceType": awsstepfunctions.JsonPath_StringAt(jsii.String("$.Instances[0].InstanceType")),
			},
		},
	)

	// Wait for instance to be ready
	waitForInstanceTask := WaitForInstanceToBeReady(scope, WaitForInstanceToBeReadyInput{
		Task:   createEC2Task,
		IdPath: jsii.String("$.ec2Instance.InstanceId"),
	})

	// Copy binary to EC2 instance, run benchmark tests and export results to S3
	runBenchmarkTask := awsstepfunctionstasks.NewCallAwsService(scope, jsii.String("RunBenchmark"+input.Id), &awsstepfunctionstasks.CallAwsServiceProps{
		Service: jsii.String("ssm"),
		Action:  jsii.String("sendCommand"),
		IamResources: jsii.Strings(
			"arn:aws:ssm:*:*:document/AWS-RunShellScript",
		),
		// include the command output in the result
		// see shape at https://docs.aws.amazon.com/systems-manager/latest/APIReference/API_SendCommand.html#API_SendCommand_ResponseSyntax
		ResultPath:  jsii.String("$.commandOutput"),
		TaskTimeout: awsstepfunctions.Timeout_Duration(awscdk.Duration_Minutes(jsii.Number(15))),
		Parameters: &map[string]interface{}{
			"InstanceIds": awsstepfunctions.JsonPath_Array(
				awsstepfunctions.JsonPath_StringAt(jsii.String("$.ec2Instance.InstanceId")),
			),
			"DocumentName": jsii.String("AWS-RunShellScript"),
			"Parameters": map[string]interface{}{
				"commands": awsstepfunctions.JsonPath_Array(
					awsstepfunctions.JsonPath_Format(
						jsii.String("aws s3 cp s3://{}/{} {}"),
						input.BinaryS3Asset.S3BucketName(),
						input.BinaryS3Asset.S3ObjectKey(),
						jsii.String(input.LaunchTemplateOutput.BenchmarkBinaryZipPath),
					),
					// unzip the binary
					jsii.String("unzip -o "+input.LaunchTemplateOutput.BenchmarkBinaryZipPath+" -d /home/ec2-user/benchmark"),
					jsii.String("chmod +x /home/ec2-user/benchmark/benchmark"),
					awsstepfunctions.JsonPath_Format(
						jsii.String("RESULTS_PATH=/tmp/results.csv /home/ec2-user/benchmark/benchmark && aws s3 cp /tmp/results.csv s3://{}/{}_{}.csv"),
						input.ResultsBucket.BucketName(),
						awsstepfunctions.JsonPath_StringAt(jsii.String("$.timestamp")),
						awsstepfunctions.JsonPath_StringAt(jsii.String("$.ec2Instance.InstanceType")),
					),
				),
			},
		},
	})

	waitForRunBenchmarkTask := WaitForSendCommandSuccess(scope, WaitForSendCommandSuccessInput{
		Command:    runBenchmarkTask,
		InstanceId: awsstepfunctions.JsonPath_StringAt(jsii.String("$.ec2Instance.InstanceId")),
		CommandId:  awsstepfunctions.JsonPath_StringAt(jsii.String("$.commandOutput.Command.CommandId")),
	})

	// Terminate EC2 instance
	terminateEc2Instance := awsstepfunctionstasks.NewCallAwsService(scope, jsii.String("TerminateEC2Instance"+input.Id), &awsstepfunctionstasks.CallAwsServiceProps{
		Service: jsii.String("ec2"),
		Action:  jsii.String("terminateInstances"),
		IamResources: jsii.Strings(
			"arn:aws:ec2:*:*:instance/*",
		),
		Parameters: &map[string]interface{}{
			"InstanceIds": awsstepfunctions.JsonPath_Array(awsstepfunctions.JsonPath_StringAt(jsii.String("$.ec2Instance.InstanceId"))),
		},
	})

	// Chain the tasks together
	mainWorkflow := awsstepfunctions.Chain_Start(createEC2Task).
		Next(waitForInstanceTask).
		Next(runBenchmarkTask).
		Next(waitForRunBenchmarkTask).
		Next(terminateEc2Instance)

	benchmarkWorkflowState := mainWorkflow.ToSingleState(jsii.String("BenchmarkWorkflow"+input.Id), &awsstepfunctions.ParallelProps{
		OutputPath: jsii.String("$[0]"),
	})

	errorHandler := awsstepfunctions.NewFail(scope, jsii.String("ErrorHandler"+input.Id), &awsstepfunctions.FailProps{
		// we want to pass the error and the ec2 instance to the error handler, so we can handle upstream
		Cause: awsstepfunctions.JsonPath_JsonToString(awsstepfunctions.JsonPath_JsonMerge(
			awsstepfunctions.JsonPath_ObjectAt(jsii.String("$.error")),
			awsstepfunctions.JsonPath_ObjectAt(jsii.String("$.ec2Instance")),
		)),
		Error: jsii.String("WorkflowFailed"),
	})

	benchmarkWorkflowState.AddCatch(errorHandler, &awsstepfunctions.CatchProps{
		ResultPath: jsii.String("$.error"),
	})

	return benchmarkWorkflowState
}

func parallelizeWorkflows(scope constructs.Construct, workflows []awsstepfunctions.IChainable) awsstepfunctions.Parallel {
	parallel := awsstepfunctions.NewParallel(scope, jsii.String("ParallelWorkflow"), &awsstepfunctions.ParallelProps{
		ResultPath: jsii.String("$.parallelResults"),
	})

	for _, workflow := range workflows {
		parallel.Branch(workflow)
	}

	return parallel
}

type WaitForSendCommandSuccessInput struct {
	Command    awsstepfunctionstasks.CallAwsService
	InstanceId *string
	CommandId  *string
}

type WaitForInstanceToBeReadyInput struct {
	Task   awsstepfunctionstasks.CallAwsService
	IdPath *string
}

func WaitForInstanceToBeReady(scope constructs.Construct, input WaitForInstanceToBeReadyInput) awsstepfunctions.IChainable {
	// Create a wait state that will pause execution for a specified time
	wait := awsstepfunctions.NewWait(scope, jsii.String("Wait"+*input.Task.Id()), &awsstepfunctions.WaitProps{
		Time: awsstepfunctions.WaitTime_Duration(awscdk.Duration_Seconds(jsii.Number(30))),
	})

	// Create a task to check the instance status and system checks
	checkStatus := awsstepfunctionstasks.NewCallAwsService(scope, jsii.String("CheckStatus"+*input.Task.Id()), &awsstepfunctionstasks.CallAwsServiceProps{
		Service: jsii.String("ec2"),
		Action:  jsii.String("describeInstanceStatus"),
		Parameters: &map[string]interface{}{
			"InstanceIds": awsstepfunctions.JsonPath_Array(awsstepfunctions.JsonPath_StringAt(input.IdPath)),
		},
		IamResources: jsii.Strings(
			"arn:aws:ec2:*:*:instance/*",
		),
		ResultPath: jsii.String("$.instanceStatus"),
	})

	// Create a choice state to determine if the instance is ready
	isReady := awsstepfunctions.NewChoice(scope, jsii.String("IsReady"+*input.Task.Id()), &awsstepfunctions.ChoiceProps{})

	// If the instance is running and all status checks have passed, move to the next state
	readyCondition := awsstepfunctions.Condition_And(
		awsstepfunctions.Condition_StringEquals(jsii.String("$.instanceStatus.InstanceStatuses[0].InstanceState.Name"), jsii.String("running")),
		awsstepfunctions.Condition_StringEquals(jsii.String("$.instanceStatus.InstanceStatuses[0].InstanceStatus.Status"), jsii.String("ok")),
		awsstepfunctions.Condition_StringEquals(jsii.String("$.instanceStatus.InstanceStatuses[0].SystemStatus.Status"), jsii.String("ok")),
	)

	// If the instance is still in progress or checks are not complete, wait and check again
	inProgressCondition := awsstepfunctions.Condition_Or(
		awsstepfunctions.Condition_StringEquals(jsii.String("$.instanceStatus.InstanceStatuses[0].InstanceState.Name"), jsii.String("pending")),
		awsstepfunctions.Condition_StringEquals(jsii.String("$.instanceStatus.InstanceStatuses[0].InstanceStatus.Status"), jsii.String("initializing")),
		awsstepfunctions.Condition_StringEquals(jsii.String("$.instanceStatus.InstanceStatuses[0].SystemStatus.Status"), jsii.String("initializing")),
	)

	// If the instance failed or terminated, throw an error
	failureCondition := awsstepfunctions.Condition_Or(
		awsstepfunctions.Condition_StringEquals(jsii.String("$.instanceStatus.InstanceStatuses[0].InstanceState.Name"), jsii.String("terminated")),
		awsstepfunctions.Condition_StringEquals(jsii.String("$.instanceStatus.InstanceStatuses[0].InstanceState.Name"), jsii.String("stopped")),
	)

	// Create an error state
	failState := awsstepfunctions.NewFail(scope, jsii.String("Fail"+*input.Task.Id()), &awsstepfunctions.FailProps{
		Cause: jsii.String("Instance failed to be ready or system checks failed"),
		Error: jsii.String("InstanceFailed"),
	})

	// Chain the states together
	definition := awsstepfunctions.Chain_Start(wait).
		Next(checkStatus).
		Next(isReady.
			When(readyCondition, awsstepfunctions.NewSucceed(scope, jsii.String("Success"+*input.Task.Id()), &awsstepfunctions.SucceedProps{
				Comment: jsii.String("Instance is ready and all checks passed"),
			}), nil).
			When(inProgressCondition, wait, nil).
			When(failureCondition, failState, nil).
			Otherwise(failState))

	// make the definition a single parallel state
	return definition.ToSingleState(jsii.String("WaitForInstanceToBeReady"+*input.Task.Id()), &awsstepfunctions.ParallelProps{
		ResultPath: awsstepfunctions.JsonPath_DISCARD(),
	})
}

// WaitForSendCommandSuccess waits for the command to be successful
// it uses the command output to run a polling pattern to check if the command was successful
func WaitForSendCommandSuccess(scope constructs.Construct, input WaitForSendCommandSuccessInput) awsstepfunctions.IChainable {
	// Create a wait state that will pause execution for a specified time
	wait := awsstepfunctions.NewWait(scope, jsii.String("Wait"+*input.Command.Id()), &awsstepfunctions.WaitProps{
		Time: awsstepfunctions.WaitTime_Duration(awscdk.Duration_Seconds(jsii.Number(30))),
	})

	// Create a task to check the command status
	checkStatus := awsstepfunctionstasks.NewCallAwsService(scope, jsii.String("CheckStatus"+*input.Command.Id()), &awsstepfunctionstasks.CallAwsServiceProps{
		Service: jsii.String("ssm"),
		Action:  jsii.String("getCommandInvocation"),
		Parameters: &map[string]interface{}{
			"CommandId":  input.CommandId,
			"InstanceId": input.InstanceId,
		},
		IamResources: jsii.Strings(
			"arn:aws:ssm:*:*:document/AWS-RunShellScript",
		),
		ResultPath: jsii.String("$.checkCommandInvocation"),
	})

	// Create a choice state to determine if the command is complete
	isComplete := awsstepfunctions.NewChoice(scope, jsii.String("IsComplete"+*input.Command.Id()), &awsstepfunctions.ChoiceProps{})

	// If the command is successful, move to the next state
	successCondition := awsstepfunctions.Condition_StringEquals(jsii.String("$.checkCommandInvocation.Status"), jsii.String("Success"))

	// If the command is still in progress, wait and check again
	inProgressCondition := awsstepfunctions.Condition_StringEquals(jsii.String("$.checkCommandInvocation.Status"), jsii.String("InProgress"))

	// If the command failed, throw an error
	failureCondition := awsstepfunctions.Condition_StringEquals(jsii.String("$.checkCommandInvocation.Status"), jsii.String("Failed"))

	formatErrorState := awsstepfunctions.NewPass(scope, jsii.String("FormatError"+*input.Command.Id()), &awsstepfunctions.PassProps{
		Parameters: &map[string]interface{}{
			"Error": awsstepfunctions.JsonPath_ObjectAt(jsii.String("$.checkCommandInvocation")),
			"Cause": awsstepfunctions.JsonPath_Format(
				jsii.String("Command execution failed: {}"),
				awsstepfunctions.JsonPath_StringAt(jsii.String("$.checkCommandInvocation.StandardErrorContent")),
			),
		},
		ResultPath: jsii.String("$.error"),
	})

	// Create an error state
	failState := awsstepfunctions.NewFail(scope, jsii.String("Fail"+*input.Command.Id()), &awsstepfunctions.FailProps{
		CausePath: jsii.String("$.error.Cause"),
		ErrorPath: jsii.String("$.error.Error"),
	})

	// Chain the states together
	definition := awsstepfunctions.Chain_Start(wait).
		Next(checkStatus).
		Next(isComplete.
			When(successCondition, awsstepfunctions.NewSucceed(scope, jsii.String("Success"+*input.Command.Id()), &awsstepfunctions.SucceedProps{
				Comment: awsstepfunctions.JsonPath_Format(
					jsii.String("Command execution successful: {}"),
					awsstepfunctions.JsonPath_StringAt(jsii.String("$.checkCommandInvocation.StandardOutputContent")),
				),
			}), nil).
			When(inProgressCondition, wait, nil).
			When(failureCondition, failState, nil).
			Otherwise(formatErrorState.Next(failState)))

	// make the definition a single parallel state
	return definition.ToSingleState(jsii.String("WaitForSendCommandSuccess"+*input.Command.Id()), &awsstepfunctions.ParallelProps{
		ResultPath: awsstepfunctions.JsonPath_DISCARD(),
	})
}
