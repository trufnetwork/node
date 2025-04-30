package provider

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// defaultProvider is the standard implementation of CertProvider.
type defaultProvider struct{}

// New returns a CertProvider that issues certificates for edge or regional scopes.
func New() CertProvider {
	return &defaultProvider{}
}

// Get returns an ACM certificate for the given fqdn in the hosted zone under the specified scope.
func (p *defaultProvider) Get(
	scope constructs.Construct,
	id string,
	zone awsroute53.IHostedZone,
	fqdn string,
	sScope CertScope,
	additionalSANs []*string,
) awscertificatemanager.ICertificate {
	// Determine the issuance scope: edge cert in us-east-1 or regional
	var certScope constructs.Construct = scope
	if sScope == ScopeEdge {
		// Create a nested stack in us-east-1 for the edge certificate
		edgeStack := awscdk.NewStack(scope, jsii.String(id+"EdgeCertStack"), &awscdk.StackProps{
			Env: &awscdk.Environment{Region: jsii.String("us-east-1")},
		})
		certScope = edgeStack
	}
	// Define certificate properties
	certProps := &awscertificatemanager.CertificateProps{
		DomainName: jsii.String(fqdn),
		Validation: awscertificatemanager.CertificateValidation_FromDns(zone),
	}
	// Add SANs if provided
	if len(additionalSANs) > 0 {
		certProps.SubjectAlternativeNames = &additionalSANs // Pass directly
	}

	// Issue the certificate
	return awscertificatemanager.NewCertificate(certScope, jsii.String(id), certProps)
}
