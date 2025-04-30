package provider

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
)

// CertScope indicates certificate issuance scope: edge or region.
type CertScope string

const (
	// ScopeEdge issues a certificate in us-east-1 for edge services (e.g. CloudFront).
	ScopeEdge CertScope = "edge"
	// ScopeRegion issues a certificate in the same region as the calling stack (e.g. HTTP API).
	ScopeRegion CertScope = "region"
)

// CertProvider defines how to obtain an ACM certificate for a domain.
type CertProvider interface {
	// Get returns an ACM certificate for the given fqdn in the hosted zone under the specified scope.
	// additionalSANs allows specifying extra SubjectAlternativeNames.
	Get(scope constructs.Construct, id string, zone awsroute53.IHostedZone, fqdn string, s CertScope, additionalSANs []*string) awscertificatemanager.ICertificate
}
