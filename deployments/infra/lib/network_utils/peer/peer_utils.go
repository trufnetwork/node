package peer

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/jsii-runtime-go"
	"strconv"
)

// TsnP2pPort is the port used for P2P communication
// this is hardcoded at the Dockerfile that generates TSN nodes
const TsnP2pPort = 26656
const TsnHttpPort = 80

type PeerConnection struct {
	ElasticIp awsec2.CfnEIP
	P2PPort   int
	HttpPort  int
}

func (p PeerConnection) GetP2PAddress() *string {
	ipAndPort := []*string{p.ElasticIp.AttrPublicIp(), jsii.String(strconv.Itoa(p.P2PPort))}
	return awscdk.Fn_Join(jsii.String(":"), &ipAndPort)
}

func (p PeerConnection) GetHttpAddress() *string {
	ipAndPort := []*string{p.ElasticIp.AttrPublicIp(), jsii.String(strconv.Itoa(p.HttpPort))}
	return awscdk.Fn_Join(jsii.String(":"), &ipAndPort)
}

func NewPeerConnection(ip awsec2.CfnEIP) PeerConnection {
	return PeerConnection{
		ElasticIp: ip,
		P2PPort:   TsnP2pPort,
		HttpPort:  TsnHttpPort,
	}
}
