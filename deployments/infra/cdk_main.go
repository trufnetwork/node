package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/utils"
	"github.com/truflation/tsn-db/infra/stacks"
	"github.com/truflation/tsn-db/infra/stacks/benchmark"
)

func main() {
	app := awscdk.NewApp(nil)

	certStackExports := stacks.CertStack(app)

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
