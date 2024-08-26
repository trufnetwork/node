package stacks

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
)

func TestBuildGoBinaryIntoS3Asset(t *testing.T) {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("test-stack"), nil)

	asset := buildGoBinaryIntoS3Asset(stack, jsii.String("test-asset"), buildGoBinaryIntoS3AssetInput{
		BinaryPath: jsii.String("../tests/hello/main.go"),
		BinaryName: jsii.String("hello"),
	})

	assetPath := asset.AssetPath()

	fullPath := fmt.Sprintf("%s/%s/%s", *app.Outdir(), *assetPath, "hello")

	// run and expect
	os.Chmod(fullPath, 0755)
	cmd := exec.Command(fullPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to run binary: %s", err)
	}

	if string(output) != "Hello, World!\n" {
		t.Fatalf("Expected 'Hello, World!', got %s", output)
	}

	app.Synth(nil)
}
