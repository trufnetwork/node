package fronting

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
)

// FrontingResult bundles the public FQDN and the ACM certificate used.
// This allows stacks to export both the domain name and the certificate ARN.
// FQDN: full custom domain (e.g., "api.dev.example.com").
// Certificate: the ACM certificate resource for TLS termination.
type FrontingResult struct {
	FQDN        *string
	Certificate awscertificatemanager.ICertificate
}

// FrontingProps holds the inputs needed to wire up an edge proxy.
type FrontingProps struct {
	// HostedZone is the Route53 hosted zone for record creation.
	HostedZone awsroute53.IHostedZone
	// Optional imported ACM certificate; if nil, a new cert is issued.
	ImportedCertificate awscertificatemanager.ICertificate
	// AdditionalSANs allows passing extra SubjectAlternativeNames when creating a new certificate.
	AdditionalSANs []*string
	// Endpoint is the public DNS name of the backend service.
	Endpoint *string
	// RecordName is the subdomain prefix under HostedZone (e.g. "gateway.dev").
	RecordName *string
}

// Fronting abstracts TLS termination + path routing.
// AttachRoutes now returns a FrontingResult, giving both domain and certificate.
// IngressRules defines the security-group openings required for this fronting.
type Fronting interface {
	// AttachRoutes provisions the fronting (e.g. API Gateway) and returns the public domain FQDN.
	AttachRoutes(scope constructs.Construct, id string, props *FrontingProps) FrontingResult
	// IngressRules returns the set of security-group ingress rules required by this fronting implementation.
	IngressRules() []IngressSpec
}

// NewApiGatewayFronting returns a Fronting implemented via HTTP API.
func NewApiGatewayFronting() Fronting {
	return &apiGateway{}
}
