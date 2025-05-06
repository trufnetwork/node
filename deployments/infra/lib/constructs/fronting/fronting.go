package fronting

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
)

// DnsTarget defines an interface for CDK constructs or resources that can serve
// as a target for Route 53 records within the alternative domain system.
// This allows different types of resources (like API Gateways, EC2 Instances via IP,
// ALBs, etc.) to be registered with the AlternativeDomainManager.
type DnsTarget interface {
	// RecordTarget returns the specific awsroute53.RecordTarget configuration
	// (e.g., IP addresses or an Alias target definition) suitable for use in
	// awsroute53.ARecordProps.Target.
	RecordTarget() awsroute53.RecordTarget
	// PrimaryFQDN returns the original, primary FQDN associated with this target resource
	// (e.g., gateway.dev.infra.truf.network). This is used mainly for informational annotations.
	PrimaryFQDN() *string
}

// FrontingResult bundles the outputs of a Fronting implementation (like API Gateway).
// It includes the primary public FQDN created, the ACM certificate used, and
// the specific Route53 alias target information.
// It implements the DnsTarget interface, allowing it to be directly registered
// with the AlternativeDomainManager.
type FrontingResult struct {
	FQDN        *string                            // The primary FQDN created by the fronting construct.
	Certificate awscertificatemanager.ICertificate // The ACM certificate used for TLS.
	AliasTarget awsroute53.IAliasRecordTarget      // The specific alias target properties (e.g., for API GW domain).
	Api         awsapigatewayv2.IHttpApi           // The underlying HTTP API construct.
}

// RecordTarget implements the DnsTarget interface.
// It returns an awsroute53.RecordTarget configured for an Alias record
// using the AliasTarget information stored in the FrontingResult.
func (fr *FrontingResult) RecordTarget() awsroute53.RecordTarget {
	if fr.AliasTarget == nil {
		// This indicates an internal error - AttachRoutes should always populate AliasTarget for alias-based fronting.
		panic("FrontingResult AliasTarget is nil; cannot create RecordTarget for alias.")
	}
	return awsroute53.RecordTarget_FromAlias(fr.AliasTarget)
}

// PrimaryFQDN implements the DnsTarget interface.
// It returns the primary FQDN stored in the FrontingResult.
func (fr *FrontingResult) PrimaryFQDN() *string {
	return fr.FQDN
}

// FrontingProps holds the inputs needed to wire up an edge proxy.
type FrontingProps struct {
	// HostedZone is the Route53 hosted zone for record creation.
	HostedZone awsroute53.IHostedZone
	// Optional imported ACM certificate; if nil, a new cert is issued.
	ImportedCertificate awscertificatemanager.ICertificate
	// SubjectAlternativeNames allows passing additional SANs when creating a new certificate.
	SubjectAlternativeNames []*string
	// ValidationMethod defines how the certificate (if created) should be validated via DNS (single- or multi-zone).
	ValidationMethod awscertificatemanager.CertificateValidation
	// Endpoint is the public DNS name of the backend service.
	Endpoint *string
	// RecordName is the subdomain prefix under HostedZone (e.g. "gateway.dev").
	RecordName *string
	// PrimaryDomainName is the main domain name for the certificate request (CN).
	PrimaryDomainName *string
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
