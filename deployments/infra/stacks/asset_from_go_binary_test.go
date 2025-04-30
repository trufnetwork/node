package stacks_test

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/node/infra/lib/utils/asset"
)

func TestBuildGoBinaryIntoS3Asset(t *testing.T) {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("test-stack"), nil)

	assetObj := asset.BuildGoBinaryIntoS3Asset(stack, jsii.String("test-asset"), asset.BuildGoBinaryIntoS3AssetInput{
		BinaryPath: jsii.String("../../tests/hello/main.go"),
		BinaryName: jsii.String("hello"),
	})

	// Assert that an asset path was generated
	assert.NotEmpty(t, assetObj.AssetPath(), "Asset path should not be empty")

	if err := app.Synth(nil); err != nil {
		t.Fatalf("Failed to synth app: %s", err)
	}
}
