package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	domain_utils "github.com/truflation/tsn-db/infra/lib/domain_utils"
	"github.com/truflation/tsn-db/infra/stacks"
	"os"
)

type CdkStackProps struct {
	awscdk.StackProps
	cert awscertificatemanager.Certificate
}

// CertStack creates a stack with an ACM certificate for the domain, fixed at us-east-1.
// This is necessary because CloudFront requires the certificate to be in us-east-1.
func CertStack(app constructs.Construct) awscertificatemanager.Certificate {
	env := env()
	env.Region = jsii.String("us-east-1")
	stackName := config.WithStackSuffix(app, "TSN-Cert")
	stack := awscdk.NewStack(app, jsii.String(stackName), &awscdk.StackProps{
		Env:                   env,
		CrossRegionReferences: jsii.Bool(true),
	})
	domain := config.Domain(stack)
	hostedZone := domain_utils.GetTSNHostedZone(stack)
	return domain_utils.GetACMCertificate(stack, domain, &hostedZone)
}

func main() {
	app := awscdk.NewApp(nil)

	certificate := CertStack(app)

	stacks.TsnAutoStack(
		app,
		config.WithStackSuffix(app, "TSN-DB-Auto"),
		&stacks.TsnAutoStackProps{
			StackProps: awscdk.StackProps{
				Env:                   env(),
				CrossRegionReferences: jsii.Bool(true),
				Description:           jsii.String("TSN-DB Auto is a stack that uses on-fly generated config files for the TSN nodes"),
			},
			Cert: certificate,
		},
	)

	stacks.TsnFromConfigStack(
		app,
		config.WithStackSuffix(app, "TSN-From-Config"),
		&stacks.TsnFromConfigStackProps{
			StackProps: awscdk.StackProps{
				Env:                   env(),
				CrossRegionReferences: jsii.Bool(true),
				Description:           jsii.String("TSN-From-Config is a stack that uses a pre-existing config file for the TSN nodes"),
			},
			Cert: certificate,
		},
	)

	app.Synth(nil)
}

// env determines the AWS environment (account+region) in which our stack is to
// be deployed. For more information see: https://docs.aws.amazon.com/cdk/latest/guide/environments.html
func env() *awscdk.Environment {
	account := os.Getenv("CDK_DEPLOY_ACCOUNT")
	region := os.Getenv("CDK_DEPLOY_REGION")

	if len(account) == 0 || len(region) == 0 {
		account = os.Getenv("CDK_DEFAULT_ACCOUNT")
		region = os.Getenv("CDK_DEFAULT_REGION")
	}

	return &awscdk.Environment{
		Account: jsii.String(account),
		Region:  jsii.String(region),
	}
}
