package fronting

import "github.com/aws/aws-cdk-go/awscdk/v2/awsec2"

// IngressSpec defines one security group rule that a fronting plugin requires.
type IngressSpec struct {
	// Protocol (e.g. awsec2.Protocol_TCP)
	Protocol awsec2.Protocol
	// FromPort is the starting port number for the rule.
	FromPort float64
	// ToPort is the ending port number for the rule.
	ToPort float64
	// Source is either an IPv4 CIDR (e.g., "0.0.0.0/0"), IPv6 CIDR, or a PrefixList ID ("pl-...").
	Source string
	// Description annotates the rule (e.g. "HTTP from API Gateway").
	Description string
}
