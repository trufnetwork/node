package fronting

import (
	"fmt"
	"strings"

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
// 'primaryFqdn' and 'secondaryFqdn' should be the fully qualified domain names (e.g., "gateway.dev.infra.truf.network").
func GetSharedCertProps(hostedZone awsroute53.IHostedZone, primaryFqdn, secondaryFqdn string) (primaryProps FrontingProps, secondaryProps FrontingProps) {
	zoneName := *hostedZone.ZoneName()
	zoneSuffix := "." + zoneName

	// Extract the simple label part (e.g., "gateway.dev") by trimming the zone suffix.
	primaryLabel := strings.TrimSuffix(primaryFqdn, zoneSuffix)
	secondaryLabel := strings.TrimSuffix(secondaryFqdn, zoneSuffix)

	if primaryLabel == primaryFqdn || secondaryLabel == secondaryFqdn {
		// This indicates the zone suffix wasn't found, meaning the input FQDN was likely incorrect.
		panic(fmt.Sprintf("GetSharedCertProps: Input FQDN(s) '%s', '%s' did not end with expected zone suffix '%s'", primaryFqdn, secondaryFqdn, zoneSuffix))
	}

	// Primary issues the cert, using the primary simple label for its RecordName.
	primaryProps = FrontingProps{
		HostedZone: hostedZone,
		RecordName: jsii.String(primaryLabel), // Use the extracted simple label for Route53 RecordName
		// Endpoint needs to be set by caller
		// ImportedCertificate: nil (default)
	}

	// Secondary will import the certificate issued by primary.
	// Its RecordName should be the secondary simple label.
	secondaryProps = FrontingProps{
		HostedZone: hostedZone,
		RecordName: jsii.String(secondaryLabel), // Use the extracted simple label for Route53 RecordName
		// Endpoint needs to be set by caller
		// ImportedCertificate needs to be set by caller using primary's result
		// AdditionalSANs: nil (not needed when importing)
	}

	return primaryProps, secondaryProps
}

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
