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
const TsnRPCPort = 8080

type PeerConnection struct {
	ElasticIp               awsec2.CfnEIP
	P2PPort                 int
	RPCPort                 int
	NodeCometEncodedAddress string
}

func (p PeerConnection) GetP2PAddress(withId bool) *string {
	// full p2p address = <comet_address>@<public_ip>:<p2p_port>
	// partial p2p address = <public_ip>:<p2p_port>

	// we need to create a partial address first
	partialAddress := []*string{
		p.ElasticIp.AttrPublicIp(),
		jsii.String(":"),
		jsii.String(strconv.Itoa(p.P2PPort)),
	}

	var result []*string
	if withId {
		cometAddressParts := []*string{
			jsii.String(p.NodeCometEncodedAddress),
			jsii.String("@"),
		}

		result = append(cometAddressParts, partialAddress...)
	} else {
		result = partialAddress
	}

	return awscdk.Fn_Join(jsii.String(""), &result)
}

func (p PeerConnection) GetHttpAddress() *string {
	ipAndPort := []*string{p.ElasticIp.AttrPublicIp(), jsii.String(strconv.Itoa(p.RPCPort))}
	return awscdk.Fn_Join(jsii.String(":"), &ipAndPort)
}

func NewPeerConnection(ip awsec2.CfnEIP, nodeCometEncodedAddress string) PeerConnection {
	return PeerConnection{
		ElasticIp:               ip,
		P2PPort:                 TsnP2pPort,
		RPCPort:                 TsnRPCPort,
		NodeCometEncodedAddress: nodeCometEncodedAddress,
	}
}
