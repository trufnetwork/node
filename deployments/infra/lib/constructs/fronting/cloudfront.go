package fronting

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
)

// cloudFront is a stub for a CloudFront-based fronting plugin.
// TODO: implement AttachRoutes to configure a CloudFront Distribution and Route53 alias.
type cloudFront struct{}

// NewCloudFrontFronting returns a Fronting stub for CloudFront.
func NewCloudFrontFronting() Fronting {
	return &cloudFront{}
}

// AttachRoutes is not yet implemented for CloudFront.
func (c *cloudFront) AttachRoutes(scope constructs.Construct, id string, props *FrontingProps) *string {
	// TODO: create awscloudfront.Distribution, ApiMapping, and awsroute53.ARecord
	panic("CloudFront fronting not implemented yet")
}

// IngressRules declares the security-group ingress rules for CloudFront origin access.
func (c *cloudFront) IngressRules() []IngressSpec {
	// AWS-managed prefix list ID for CloudFront origins
	const cfPrefixList = "pl-68a54001"
	return []IngressSpec{
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    443,
			ToPort:      443,
			Source:      cfPrefixList,
			Description: "TLS from CloudFront",
		},
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    1337,
			ToPort:      1337,
			Source:      cfPrefixList,
			Description: "Indexer TLS traffic from CloudFront",
		},
	}
}
