package main

import (
	"fmt"
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/stacks"
	"go.uber.org/zap"
)

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}

func main() {
	app := awscdk.NewApp(nil)

	// Test only AMI Pipeline Stack with dynamic environment
	// This will use the current AWS credentials to get account/region
	testEnv := &awscdk.Environment{
		Account: nil, // CDK will auto-detect from AWS credentials
		Region:  nil, // CDK will auto-detect from AWS CLI config
	}

	// Use stage-specific stack name for proper environment isolation
	stage := app.Node().TryGetContext(jsii.String("stage"))
	if stage == nil {
		stage = "default"
	}
	stackName := fmt.Sprintf("AMI-Pipeline-%s-Stack", stage)
	_, amiExports := stacks.AmiPipelineStack(
		app,
		stackName,
		&stacks.AmiPipelineStackProps{
			StackProps: awscdk.StackProps{Env: testEnv},
		},
	)
	_ = amiExports // Use exports if needed by other stacks

	app.Synth(nil)
}
