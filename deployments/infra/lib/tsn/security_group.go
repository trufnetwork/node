package tsn

import (
	"fmt"
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
)

type NewTSNSecurityGroupInput struct {
	vpc   awsec2.IVpc
	peers []peer.PeerConnection
}

func NewTSNSecurityGroup(scope constructs.Construct, input NewTSNSecurityGroupInput) awsec2.SecurityGroup {
	id := "TSN-DB-SG"
	vpc := input.vpc

	sg := awsec2.NewSecurityGroup(scope, jsii.String(id), &awsec2.SecurityGroupProps{
		Vpc:              vpc,
		AllowAllOutbound: jsii.Bool(true),
		Description:      jsii.String("TSN-DB Instance security group."),
	})

	// These ports are open to the public
	publicPorts := []struct {
		port int
		name string
	}{
		{peer.TsnRPCPort, "TSN RPC port"},
		{peer.TsnIndexerPort, "TSN Indexer port"},
		{22, "SSH port"},
	}

	// Only other TSN nodes should be able to communicate through these ports
	interNodePorts := []struct {
		port int
		name string
	}{
		{peer.TsnP2pPort, "TSN P2P port"},
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

	// allow communication between nodes
	for _, p := range input.peers {
		for _, port := range interNodePorts {
			sg.AddIngressRule(
				// We need to provide the public IP of the peer node
				// We can't use the SG allowance directly because this rule references the private IP,
				// but we need the public IP reference instead
				awsec2.Peer_Ipv4(awscdk.Fn_Join(jsii.String(""), &[]*string{p.ElasticIp.AttrPublicIp(), jsii.String("/32")})),
				awsec2.Port_Tcp(jsii.Number(port.port)),
				jsii.String(fmt.Sprintf("Allow communication between nodes by the %s.", port.name)),
				jsii.Bool(false))
		}
	}

	return sg
}
