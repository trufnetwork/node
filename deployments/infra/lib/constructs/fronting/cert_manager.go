package fronting

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
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

// GetRegional issues or returns a regional ACM certificate for the given domain in this hosted zone.
// 'additionalSANs' can be provided to include extra SubjectAlternativeNames.
func (c *certManager) GetRegional(
	scope constructs.Construct,
	id string,
	zone awsroute53.IHostedZone,
	fqdn string,
	additionalSANs []*string,
) awscertificatemanager.ICertificate {
	// TODO: pass 'additionalSANs' into provider if supported
	return c.provider.Get(scope, id, zone, fqdn, provider.ScopeRegion)
}

// GetEdge issues or returns an edge (us-east-1) ACM certificate for the given domain in this hosted zone.
func (c *certManager) GetEdge(
	scope constructs.Construct,
	id string,
	zone awsroute53.IHostedZone,
	fqdn string,
	additionalSANs []*string,
) awscertificatemanager.ICertificate {
	// TODO: pass 'additionalSANs' into provider if supported
	return c.provider.Get(scope, id, zone, fqdn, provider.ScopeEdge)
}
