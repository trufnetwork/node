package validator_set

import (
	"strconv"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/constructs-go/constructs/v10"

	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/tn"
)

// newNode builds a single TNInstance using the shared role and security group
func newNode(
	scope constructs.Construct,
	index int,
	role awsiam.IRole,
	sg awsec2.SecurityGroup,
	props *ValidatorSetProps,
	connection peer2.TNPeer,
	allPeers []peer2.TNPeer,
) tn.TNInstance {
	return tn.NewTNInstance(scope, tn.NewTNInstanceInput{
		Index:                index,
		Id:                   strconv.Itoa(index),
		Role:                 role,
		Vpc:                  props.Vpc,
		SecurityGroup:        sg,
		TNDockerComposeAsset: props.Assets.DockerCompose,
		TNDockerImageAsset:   props.Assets.DockerImage,
		TNConfigAsset:        props.NodesConfig[index].Asset,
		TNConfigImageAsset:   props.Assets.ConfigImage,
		InitElements:         props.InitElements,
		PeerConnection:       connection,
		AllPeerConnections:   allPeers,
		KeyPair:              props.KeyPair,
	})
}
