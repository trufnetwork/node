package fronting

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
)

// albFronting is a stub for an Application Load Balancer fronting plugin.
// TODO: implement AttachRoutes to provision an ALB, listeners, target groups, and Route53 record.
type albFronting struct{}

// NewAlbFronting returns a Fronting stub for ALB.
func NewAlbFronting() Fronting {
	return &albFronting{}
}

// AttachRoutes is not yet implemented for ALB fronting.
// It panics to indicate ALB fronting is unimplemented, conforming to the Fronting interface.
func (a *albFronting) AttachRoutes(scope constructs.Construct, id string, props *FrontingProps) FrontingResult {
	// TODO: create awselasticloadbalancingv2.ApplicationLoadBalancer,
	//       add HTTP/HTTPS listeners, configure target groups for KGW and Indexer,
	//       map custom domain with props.RecordName + ZoneName(), and
	//       return the DNS name of the ALB.
	panic("ALB fronting not implemented yet")
}

// IngressRules declares the security-group ingress rules for ALB origin access.
func (a *albFronting) IngressRules() []IngressSpec {
	// ALB exposes standard HTTP and HTTPS ports publicly.
	return []IngressSpec{
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    80,
			ToPort:      80,
			Source:      "0.0.0.0/0",
			Description: "HTTP from clients via ALB",
		},
		{
			Protocol:    awsec2.Protocol_TCP,
			FromPort:    443,
			ToPort:      443,
			Source:      "0.0.0.0/0",
			Description: "HTTPS from clients via ALB",
		},
	}
}
