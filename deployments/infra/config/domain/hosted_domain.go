package domain

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
	jsii "github.com/aws/jsii-runtime-go"
)

// HostedDomainProps holds inputs for creating a HostedDomain construct.
// Spec includes Stage, Sub (optional leaf subdomain), and DevPrefix (mandatory when Stage is dev).
type HostedDomainProps struct {
	Spec            Spec
	EdgeCertificate bool     // if true, issues the certificate in us-east-1
	AdditionalNames []string // extra SANs for the certificate
}

// HostedDomain is a construct that looks up a Route53 hosted zone and provisions an ACM certificate for a given FQDN.
type HostedDomain struct {
	constructs.Construct
	Zone       awsroute53.IHostedZone
	Cert       awscertificatemanager.Certificate
	FQDN       string  // fully-qualified domain name
	DomainName *string // DomainName token for callsites needing a *string
}

// NewHostedDomain creates or reuses a HostedDomain in the CDK tree, ensuring idempotence per (scope, id).
func NewHostedDomain(scope constructs.Construct, id string, props *HostedDomainProps) *HostedDomain {
	// Create the underlying construct node
	node := constructs.NewConstruct(scope, jsii.String(id))
	hd := &HostedDomain{Construct: node}

	// Determine the full FQDN from Spec
	hd.FQDN = *props.Spec.FQDN()
	// Expose a *string for backward compatibility
	hd.DomainName = jsii.String(hd.FQDN)

	// Lookup the existing hosted zone for the fixed root domain
	hd.Zone = awsroute53.HostedZone_FromLookup(node, jsii.String("Zone"), &awsroute53.HostedZoneProviderProps{
		DomainName: jsii.String(MainDomain),
	})

	// Decide certificate scope: same node or a us-east-1 Stack for edge certs
	certScope := node
	if props.EdgeCertificate {
		edgeStack := awscdk.NewStack(scope, jsii.String(id+"-EdgeCert"), &awscdk.StackProps{
			Env: &awscdk.Environment{Region: jsii.String("us-east-1")},
		})
		certScope = edgeStack
	}

	// Prepare SubjectAlternativeNames tokens
	var altNames []*string
	for _, name := range props.AdditionalNames {
		altNames = append(altNames, jsii.String(name))
	}

	// Define certificate properties
	certProps := &awscertificatemanager.CertificateProps{
		DomainName: jsii.String(hd.FQDN),
		Validation: awscertificatemanager.CertificateValidation_FromDns(hd.Zone),
	}
	if len(altNames) > 0 {
		certProps.SubjectAlternativeNames = &altNames
	}

	// Create the certificate
	hd.Cert = awscertificatemanager.NewCertificate(certScope, jsii.String("Cert"), certProps)

	// Emit CfnOutputs inside this construct for domain, hosted zone ID, and certificate ARN
	awscdk.NewCfnOutput(node, jsii.String("Domain"), &awscdk.CfnOutputProps{Value: jsii.String(hd.FQDN)})
	awscdk.NewCfnOutput(node, jsii.String("HostedZoneId"), &awscdk.CfnOutputProps{Value: hd.Zone.HostedZoneId()})
	awscdk.NewCfnOutput(node, jsii.String("CertificateArn"), &awscdk.CfnOutputProps{Value: hd.Cert.CertificateArn()})

	return hd
}

// AddARecord creates an A record in this hosted zone for the given subdomain (empty sub for apex).
func (h *HostedDomain) AddARecord(id string, sub string, target awsroute53.RecordTarget) awsroute53.ARecord {
	var recordName *string
	if sub != "" {
		recordName = jsii.String(fmt.Sprintf("%s.%s", sub, h.FQDN))
	}
	return awsroute53.NewARecord(h, jsii.String(id), &awsroute53.ARecordProps{
		Zone:       h.Zone,
		RecordName: recordName,
		Target:     target,
	})
}
