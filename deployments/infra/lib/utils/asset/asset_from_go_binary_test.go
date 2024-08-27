package asset

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

	asset := BuildGoBinaryIntoS3Asset(stack, jsii.String("test-asset"), BuildGoBinaryIntoS3AssetInput{
		BinaryPath: jsii.String("../../../tests/hello/main.go"),
		BinaryName: jsii.String("hello"),
	})

	assetPath := asset.AssetPath()

	fullPath := fmt.Sprintf("%s/%s/%s", *app.Outdir(), *assetPath, "hello")

	// run and expect
	err := os.Chmod(fullPath, 0755)
	if err != nil {
		t.Fatalf("Failed to chmod binary: %s", err)
	}

	cmd := exec.Command(fullPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to run binary: %s", err)
	}

	if string(output) != "Hello, World!\n" {
		t.Fatalf("Expected 'Hello, World!', got %s", output)
	}

	if err := app.Synth(nil); err != nil {
		t.Fatalf("Failed to synth app: %s", err)
	}
}
