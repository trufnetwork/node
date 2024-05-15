package tsn

import (
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

	// TODO security could be hardened by allowing only specific IPs
	//   relative to cloudfront distribution IPs
	sg.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(peer.TsnRPCPort)),
		jsii.String("Allow requests to the TSN RPC port."),
		jsii.Bool(false))

	// ssh
	sg.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(22)),
		jsii.String("Allow ssh."),
		jsii.Bool(false))

	// allow communication between nodes by the P2P port
	for _, p := range input.peers {
		sg.AddIngressRule(
			// We need to provide the public IP of the peer node
			// We can't use the SG allowance directly because this rule references the private IP,
			// but we need the public IP reference instead
			awsec2.Peer_Ipv4(awscdk.Fn_Join(jsii.String(""), &[]*string{p.ElasticIp.AttrPublicIp(), jsii.String("/32")})),
			awsec2.Port_Tcp(jsii.Number(p.P2PPort)),
			jsii.String("Allow communication between nodes."),
			jsii.Bool(false))
	}

	return sg
}
