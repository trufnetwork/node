package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/lib/utils"
	"github.com/trufnetwork/node/infra/stacks"
	"github.com/trufnetwork/node/infra/stacks/benchmark"
	"go.uber.org/zap"
)

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}

func main() {
	app := awscdk.NewApp(nil)

	// Determine desired fronting type: api or cloudfront
	frontingType := *app.Node().TryGetContext(jsii.String("frontingType")).(*string)
	if frontingType == "" {
		frontingType = "api"
	}

	// Only deploy the legacy CertStack if using CloudFront
	var certExports *stacks.CertStackExports
	if frontingType == "cloudfront" {
		exports := stacks.CertStack(app)
		certExports = &exports
	}

	// TN-Auto Stack
	stacks.TnAutoStack(
		app,
		config.WithStackSuffix(app, "TN-DB-Auto"),
		&stacks.TnAutoStackProps{
			StackProps:       awscdk.StackProps{Env: utils.CdkEnv()},
			CertStackExports: certExports,
		},
	)

	// TN-From-Config Stack
	stacks.TnFromConfigStack(
		app,
		config.WithStackSuffix(app, "TN-From-Config"),
		&stacks.TnFromConfigStackProps{
			StackProps:       awscdk.StackProps{Env: utils.CdkEnv()},
			CertStackExports: certExports,
		},
	)

	benchmark.BenchmarkStack(
		app,
		config.WithStackSuffix(app, "Benchmark"),
		&awscdk.StackProps{
			Env: utils.CdkEnv(),
		},
	)

	app.Synth(nil)
}
