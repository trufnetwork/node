package asset

import (
	"bytes"
	"fmt"
	"log"
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
			Local: NewLocalGoBundling(*input.BinaryPath, *input.BinaryName),
		},
	})

	return asset
}

type LocalGoBundling struct {
	binaryPath string
	binaryName string
}

var _ awscdk.ILocalBundling = &LocalGoBundling{}

func (l *LocalGoBundling) TryBundle(outputDir *string, options *awscdk.BundlingOptions) *bool {
	goCmd := "go"
	// get args
	buildArgs := []string{"build", "-o", filepath.Join(*outputDir, l.binaryName)}

	// Add the source file path from the BundlingOptions
	buildArgs = append(buildArgs, l.binaryPath)

	// Set up environment variables
	env := []string{
		"GOOS=linux",
		"GOARCH=amd64",
		"CGO_ENABLED=0",
	}

	// include host go env vars
	env = append(env, os.Environ()...)

	// Merge with any environment variables from BundlingOptions
	if options.Environment != nil {
		for k, v := range *options.Environment {
			env = append(env, fmt.Sprintf("%s=%s", k, *v))
		}
	}

	// Execute the Go build command as a shell command
	cmd := exec.Command(goCmd, buildArgs...)
	cmd.Env = env

	// Capture stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run the command
	err := cmd.Run()
	if err != nil {
		log.Printf("Error building Go binary: %v\n", err)
		log.Printf("Stdout: %s\n", stdout.String())
		log.Printf("Stderr: %s\n", stderr.String())
		return jsii.Bool(false)
	}

	// check if got exit status 0
	if cmd.ProcessState.ExitCode() != 0 {
		log.Printf("Go binary build failed\n")
		log.Printf("Stdout: %s\n", stdout.String())
		log.Printf("Stderr: %s\n", stderr.String())
		return jsii.Bool(false)
	}

	log.Printf("Go binary built successfully\n")
	log.Printf("Stdout: %s\n", stdout.String())

	return jsii.Bool(true)
}

func NewLocalGoBundling(binaryPath string, binaryName string) *LocalGoBundling {
	return &LocalGoBundling{
		binaryPath: binaryPath,
		binaryName: binaryName,
	}
}
