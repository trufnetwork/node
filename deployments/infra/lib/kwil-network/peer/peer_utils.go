package peer

import (
	"strconv"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
)

// TnP2pPort is the port used for P2P communication
// this is hardcoded at the Dockerfile that generates TN nodes
const TnP2pPort = 26656
const TnRPCPort = 8484
const TnIndexerPort = 1337
const TnCometBFTRPCPort = 26657
const TnPostgresPort = 5432

type TNPeer struct {
	Address        *string
	NodeId         string
	NodeHexAddress string
}

func (p TNPeer) GetExternalP2PAddress(withId bool) *string {
	// full p2p address = <comet_address>@<public_ip>:<p2p_port>
	// partial p2p address = <public_ip>:<p2p_port>

	// we need to create a partial address first
	p2pHost := []*string{
		p.Address,
		jsii.String(":"),
		jsii.String(strconv.Itoa(TnP2pPort)),
	}

	var result []*string
	if withId {
		cometAddressParts := []*string{
			jsii.String(p.NodeId),
			jsii.String("@"),
		}

		result = append(cometAddressParts, p2pHost...)
	} else {
		result = p2pHost
	}

	return awscdk.Fn_Join(jsii.String(""), &result)
}

func (p TNPeer) GetRpcHost() *string {
	return awscdk.Fn_Join(
		jsii.String(":"),
		&[]*string{
			p.Address,
			jsii.String(strconv.Itoa(TnRPCPort)),
		},
	)
}
