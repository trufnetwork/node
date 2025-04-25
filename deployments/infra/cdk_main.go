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

	// CertStack needs the app scope to create the stack
	certStackExports := stacks.CertStack(app)

	// TSN stacks will initialize their own parameters within their scope
	stacks.TsnAutoStack(
		app,
		config.WithStackSuffix(app, "TSN-DB-Auto"),
		&stacks.TsnAutoStackProps{
			StackProps: awscdk.StackProps{
				Env:                   utils.CdkEnv(),
				CrossRegionReferences: jsii.Bool(true),
				Description:           jsii.String("TSN-DB Auto is a stack that uses on-fly generated config files for the TSN nodes"),
			},
			CertStackExports: certStackExports,
		},
	)

	stacks.TsnFromConfigStack(
		app,
		config.WithStackSuffix(app, "TSN-From-Config"),
		&stacks.TsnFromConfigStackProps{
			StackProps: awscdk.StackProps{
				Env:                   utils.CdkEnv(),
				CrossRegionReferences: jsii.Bool(true),
				Description:           jsii.String("TSN-From-Config is a stack that uses a pre-existing config file for the TSN nodes"),
			},
			CertStackExports: certStackExports,
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
