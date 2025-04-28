package tn

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
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

	// These ports are open to the public
	publicPorts := []struct {
		port int
		name string
	}{
		{peer.TnRPCPort, "TN RPC port"},
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
