package stacks

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type CertStackExports struct {
	DomainCert awscertificatemanager.Certificate
}

// CertStack creates a stack with an ACM certificate for the domain, fixed at us-east-1.
// This is necessary because CloudFront requires the certificate to be in us-east-1.
func CertStack(app awscdk.App) CertStackExports {
	env := utils.CdkEnv()
	env.Region = jsii.String("us-east-1")
	stackName := config.WithStackSuffix(app, "TN-Cert")
	stack := awscdk.NewStack(app, jsii.String(stackName), &awscdk.StackProps{
		Env:                   env,
		CrossRegionReferences: jsii.Bool(true),
	})

	// Read dev prefix from CDK context
	stage := config.GetStage(stack)
	devPrefix := config.GetDevPrefix(stack)
	// Build a Domain construct: no leaf Sub, DevPrefix comes from context
	hd := domaincfg.NewHostedDomain(stack, "Domain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     stage,
			Sub:       "",
			DevPrefix: devPrefix,
		},
	})
	domainCert := hd.Cert

	return CertStackExports{
		DomainCert: domainCert,
	}
}
