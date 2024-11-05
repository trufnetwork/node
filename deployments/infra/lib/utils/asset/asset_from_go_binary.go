package asset

import (
	"bytes"
	"fmt"
	"go.uber.org/zap"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/go-playground/validator/v10"
)

type BuildGoBinaryIntoS3AssetInput struct {
	BinaryPath *string `validate:"required"`
	BinaryName *string `validate:"required"`
	// If true, the binary will be built as a test binary
	IsTest bool
}

func BuildGoBinaryIntoS3Asset(scope constructs.Construct, id *string, input BuildGoBinaryIntoS3AssetInput) awss3assets.Asset {
	binaryDir := filepath.Dir(*input.BinaryPath)

	// validate input
	if err := validator.New().Struct(input); err != nil {
		panic(err)
	}

	// Create an S3 asset from the Go binary
	asset := awss3assets.NewAsset(scope, id, &awss3assets.AssetProps{
		Path: jsii.String(binaryDir),
		// Use a custom bundling option to build the Go binary
		Bundling: &awscdk.BundlingOptions{
			Image: awscdk.DockerImage_FromRegistry(jsii.String("should-never-run-this-image")),
			Local: NewLocalGoBundling(*input.BinaryPath, *input.BinaryName, input.IsTest),
		},
	})

	return asset
}

// BaseLocalGoBundling is the common structure for both test and regular bundling
type BaseLocalGoBundling struct {
	binaryPath string
	binaryName string
	isTest     bool
}

var _ awscdk.ILocalBundling = &BaseLocalGoBundling{}

func (b *BaseLocalGoBundling) TryBundle(outputDir *string, options *awscdk.BundlingOptions) *bool {
	goCmd := "go"
	var buildArgs []string
	if b.isTest {
		buildArgs = []string{"test", "-v", "-c", "-timeout", "0", b.binaryPath}
	} else {
		buildArgs = []string{"build"}
	}
	buildArgs = append(buildArgs, "-o", filepath.Join(*outputDir, b.binaryName))

	env := []string{
		"GOOS=linux",
		"GOARCH=amd64",
		"CGO_ENABLED=0",
	}

	env = append(env, os.Environ()...)

	if options.Environment != nil {
		for k, v := range *options.Environment {
			env = append(env, fmt.Sprintf("%s=%s", k, *v))
		}
	}

	cmd := exec.Command(goCmd, buildArgs...)
	cmd.Env = env

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil || cmd.ProcessState.ExitCode() != 0 {
		zap.L().Error("Error building Go binary", zap.Error(err), zap.String("stdout", stdout.String()), zap.String("stderr", stderr.String()))
		return jsii.Bool(false)
	}

	zap.L().Info("Go binary built successfully", zap.String("stdout", stdout.String()))

	return jsii.Bool(true)
}

func NewLocalGoBundling(binaryPath string, binaryName string, isTest bool) *BaseLocalGoBundling {
	return &BaseLocalGoBundling{
		binaryPath: binaryPath,
		binaryName: binaryName,
		isTest:     isTest,
	}
}
