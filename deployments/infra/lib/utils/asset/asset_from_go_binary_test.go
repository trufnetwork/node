package asset

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/stretchr/testify/assert"
)

func TestBuildGoBinaryIntoS3Asset(t *testing.T) {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("test-stack"), nil)

	asset := BuildGoBinaryIntoS3Asset(stack, jsii.String("test-asset"), BuildGoBinaryIntoS3AssetInput{
		BinaryPath: jsii.String("../../../tests/hello/main.go"),
		BinaryName: jsii.String("hello"),
	})

	// Assert that an asset path was generated (basic check)
	assert.NotEmpty(t, asset.AssetPath(), "Asset path should not be empty")

	if err := app.Synth(nil); err != nil {
		t.Fatalf("Failed to synth app: %s", err)
	}
}
