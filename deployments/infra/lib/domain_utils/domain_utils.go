package domain_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"strings"
)

func GetACMCertificate(stack constructs.Construct, domain *string, hostedZone *awsroute53.IHostedZone) awscertificatemanager.Certificate {
	id := strings.Join([]string{*domain, "ACM-Certificate"}, "-")
	// Create ACM certificate.
	return awscertificatemanager.NewCertificate(stack, &id, &awscertificatemanager.CertificateProps{
		DomainName: domain,
		Validation: awscertificatemanager.CertificateValidation_FromDns(*hostedZone),
	})
}

const MainDomain = "tsn.truflation.com"
const TestDomain = "tsn.test.truflation.com"

func GetTSNHostedZone(stack awscdk.Stack) awsroute53.IHostedZone {
	return awsroute53.HostedZone_FromLookup(stack, jsii.String("HostedZone"), &awsroute53.HostedZoneProviderProps{
		DomainName: jsii.String(MainDomain),
	})
}

func GetTSNTestHostedZone(stack awscdk.Stack) awsroute53.IHostedZone {
	return awsroute53.HostedZone_FromLookup(stack, jsii.String("TestHostedZone"), &awsroute53.HostedZoneProviderProps{
		DomainName: jsii.String(TestDomain),
	})
}
