package stacks

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/lib/domain_utils"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type CertStackExports struct {
	DomainCert awscertificatemanager.Certificate
}

// CertStack creates a stack with an ACM certificate for the domain, fixed at us-east-1.
// This is necessary because CloudFront requires the certificate to be in us-east-1.
func CertStack(app constructs.Construct) CertStackExports {
	env := utils.CdkEnv()
	env.Region = jsii.String("us-east-1")
	stackName := config.WithStackSuffix(app, "TSN-Cert")
	stack := awscdk.NewStack(app, jsii.String(stackName), &awscdk.StackProps{
		Env:                   env,
		CrossRegionReferences: jsii.Bool(true),
	})
	domain := config.Domain(stack)
	hostedZone := domain_utils.GetTSNHostedZone(stack)

	domainCert := domain_utils.GetACMCertificate(stack, domain, &hostedZone)

	return CertStackExports{
		DomainCert: domainCert,
	}
}
