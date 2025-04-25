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
	stackName := config.WithStackSuffix(app, "TSN-Cert")
	stack := awscdk.NewStack(app, jsii.String(stackName), &awscdk.StackProps{
		Env:                   env,
		CrossRegionReferences: jsii.Bool(true),
	})

	// Initialize HostedDomain using centralized CDK parameters
	params := config.NewCDKParams(stack)
	stageToken := params.Stage.ValueAsString()
	devPrefix := params.DevPrefix.ValueAsString()
	// Build a Domain construct: no leaf Sub, DevPrefix comes from CFN parameter
	hd := domaincfg.NewHostedDomain(stack, "Domain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     domaincfg.StageType(*stageToken),
			Sub:       "",
			DevPrefix: *devPrefix,
		},
	})
	domainCert := hd.Cert

	return CertStackExports{
		DomainCert: domainCert,
	}
}
