package peer

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"strconv"
)

// TsnP2pPort is the port used for P2P communication
// this is hardcoded at the Dockerfile that generates TSN nodes
const TsnP2pPort = 26656
const TsnRPCPort = 8484
const TsnIndexerPort = 1337
const TsnCometBFTRPCPort = 26657
const TSNPostgresPort = 5432

type TSNPeer struct {
	Address                 *string
	NodeCometEncodedAddress string
	NodeHexAddress          string
}

func (p TSNPeer) GetExternalP2PAddress(withId bool) *string {
	// full p2p address = <comet_address>@<public_ip>:<p2p_port>
	// partial p2p address = <public_ip>:<p2p_port>

	// we need to create a partial address first
	p2pHost := []*string{
		p.Address,
		jsii.String(":"),
		jsii.String(strconv.Itoa(TsnP2pPort)),
	}

	var result []*string
	if withId {
		cometAddressParts := []*string{
			jsii.String(p.NodeCometEncodedAddress),
			jsii.String("@"),
		}

		result = append(cometAddressParts, p2pHost...)
	} else {
		result = p2pHost
	}

	return awscdk.Fn_Join(jsii.String(""), &result)
}

func (p TSNPeer) GetRpcHost() *string {
	return awscdk.Fn_Join(
		jsii.String(":"),
		&[]*string{
			p.Address,
			jsii.String(strconv.Itoa(TsnRPCPort)),
		},
	)
}
