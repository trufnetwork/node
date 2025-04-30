package fronting

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	provider "github.com/trufnetwork/node/infra/lib/cert/provider"
)

// certManager centralizes certificate issuance for fronting plugins.
type certManager struct {
	provider provider.CertProvider
}

// NewCertManager creates a new certManager using the default provider.
func NewCertManager() *certManager {
	return &certManager{provider: provider.New()}
}

// --- Shared Certificate Helper ---

// SharedCertResult holds the primary certificate and the SAN list needed for it.
type SharedCertResult struct {
	PrimaryCertProps   FrontingProps // Props to use for the primary service (will issue cert)
	SecondaryCertProps FrontingProps // Props to use for the secondary service (will import cert)
}

// GetSharedCertProps prepares FrontingProps for two services sharing a single certificate.
// It determines which props will issue the certificate (with the other's FQDN as a SAN)
// and which props will import the resulting certificate.
// 'primaryRecordName' and 'secondaryRecordName' should be the simple record names (e.g., "gateway.dev").
func GetSharedCertProps(hostedZone awsroute53.IHostedZone, primaryRecordName, secondaryRecordName string) (primaryProps FrontingProps, secondaryProps FrontingProps) {
	zoneName := *hostedZone.ZoneName()
	secondaryFqdn := fmt.Sprintf("%s.%s", secondaryRecordName, zoneName)

	// Primary issues the cert, including secondary as SAN
	primaryProps = FrontingProps{
		HostedZone:     hostedZone,
		RecordName:     jsii.String(primaryRecordName),
		AdditionalSANs: []*string{jsii.String(secondaryFqdn)},
		// Endpoint needs to be set by caller
		// ImportedCertificate: nil (default)
	}

	// Secondary will import the certificate issued by primary
	secondaryProps = FrontingProps{
		HostedZone: hostedZone,
		RecordName: jsii.String(secondaryRecordName),
		// Endpoint needs to be set by caller
		// ImportedCertificate needs to be set by caller using primary's result
		// AdditionalSANs: nil (not needed when importing)
	}

	return primaryProps, secondaryProps
}

// --- Original Methods ---

// GetRegional issues or returns a regional ACM certificate for the given domain in this hosted zone.
// 'additionalSANs' can be provided to include extra SubjectAlternativeNames.
func (c *certManager) GetRegional(
	scope constructs.Construct,
	id string,
	zone awsroute53.IHostedZone,
	fqdn string,
	additionalSANs []*string,
) awscertificatemanager.ICertificate {
	// Pass additionalSANs to the provider
	return c.provider.Get(scope, id, zone, fqdn, provider.ScopeRegion, additionalSANs)
}

// GetEdge issues or returns an edge (us-east-1) ACM certificate for the given domain in this hosted zone.
func (c *certManager) GetEdge(
	scope constructs.Construct,
	id string,
	zone awsroute53.IHostedZone,
	fqdn string,
	additionalSANs []*string,
) awscertificatemanager.ICertificate {
	// Pass additionalSANs to the provider
	return c.provider.Get(scope, id, zone, fqdn, provider.ScopeEdge, additionalSANs)
}
