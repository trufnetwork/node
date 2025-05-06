package utils

import (
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/jsii-runtime-go"

	fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
)

// ApplyIngressRules adds a list of ingress rules to a security group.
// It now uses the fronting.IngressSpec type.
func ApplyIngressRules(sg awsec2.SecurityGroup, rules []fronting.IngressSpec) {
	for _, spec := range rules {
		var peer awsec2.IPeer
		if strings.HasPrefix(spec.Source, "pl-") {
			peer = awsec2.Peer_PrefixList(jsii.String(spec.Source))
		} else if strings.Contains(spec.Source, ":") {
			peer = awsec2.Peer_Ipv6(jsii.String(spec.Source))
		} else {
			peer = awsec2.Peer_Ipv4(jsii.String(spec.Source))
		}

		sg.AddIngressRule(
			peer,
			awsec2.Port_Tcp(jsii.Number(spec.FromPort)), // Assumes TCP
			jsii.String(spec.Description),
			jsii.Bool(false),
		)
	}
}
