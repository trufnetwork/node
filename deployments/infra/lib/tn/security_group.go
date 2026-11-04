package tn

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
)

type NewTNSecurityGroupInput struct {
	Vpc awsec2.IVpc
}

func NewTNSecurityGroup(scope constructs.Construct, input NewTNSecurityGroupInput) awsec2.SecurityGroup {
	id := "TN-DB-SG"
	vpc := input.Vpc

	sg := awsec2.NewSecurityGroup(scope, jsii.String(id), &awsec2.SecurityGroupProps{
		Vpc:              vpc,
		AllowAllOutbound: jsii.Bool(true),
		Description:      jsii.String("TN-DB Instance security group."),
	})

	// The RPC port is open only to the sources listed for this stack under the
	// 'rpcSources' context key, or to any address when the stack has no entry.
	if sources, ok := config.RPCSources(scope); ok {
		for _, source := range sources {
			sg.AddIngressRule(
				awsec2.Peer_Ipv4(jsii.String(source.CIDR)),
				awsec2.Port_Tcp(jsii.Number(peer.TnRPCPort)),
				jsii.String(fmt.Sprintf("Allow requests to the TN RPC port from the %s.", source.Name)),
				jsii.Bool(false))
		}
	} else {
		sg.AddIngressRule(
			awsec2.Peer_AnyIpv4(),
			awsec2.Port_Tcp(jsii.Number(peer.TnRPCPort)),
			jsii.String("Allow requests to the TN RPC port."),
			jsii.Bool(false))
	}

	// These ports are open to the public
	publicPorts := []struct {
		port int
		name string
	}{
		{peer.TnIndexerPort, "TN Indexer port"},
		{peer.TnP2pPort, "TN P2P port"},
		{peer.TnCometBFTRPCPort, "TN Comet BFT RPC port"},
		{22, "SSH port"},
	}

	for _, p := range publicPorts {
		sg.AddIngressRule(
			// TODO security could be hardened by allowing only specific IPs
			//   relative to cloudfront distribution IPs
			awsec2.Peer_AnyIpv4(),
			awsec2.Port_Tcp(jsii.Number(p.port)),
			jsii.String(fmt.Sprintf("Allow requests to the %s.", p.name)),
			jsii.Bool(false))
	}

	return sg
}
