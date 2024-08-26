package stacks

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
)

func TestBuildGoBinaryIntoS3Asset(t *testing.T) {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("test-stack"), nil)

	buildGoBinaryIntoS3Asset(stack, jsii.String("test-asset"), buildGoBinaryIntoS3AssetInput{
		BinaryPath: jsii.String("../../../cmd/benchmark/main.go"),
	})

	app.Synth(nil)
}
