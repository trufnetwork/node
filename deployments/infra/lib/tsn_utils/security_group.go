package tsn_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/network_utils/peer"
)

type NewTSNSecurityGroupInput struct {
	vpc awsec2.IVpc
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
		awsec2.Port_Tcp(jsii.Number(peer.TsnHttpPort)),
		jsii.String("Allow requests to http."),
		jsii.Bool(false))

	// ssh
	sg.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(22)),
		jsii.String("Allow ssh."),
		jsii.Bool(false))

	// allow communication between nodes by the P2P port, by the same security group
	sg.AddIngressRule(
		sg,
		awsec2.Port_Tcp(jsii.Number(peer.TsnP2pPort)),
		jsii.String("Allow communication between nodes."),
		jsii.Bool(false))

	return sg
}
