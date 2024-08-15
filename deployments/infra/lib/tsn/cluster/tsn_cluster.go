package cluster

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/kwil-network"
	"github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
	"github.com/truflation/tsn-db/infra/lib/tsn"
	"strconv"
)

type TSNCluster struct {
	Nodes         []tsn.TSNInstance
	Role          awsiam.IRole
	SecurityGroup awsec2.SecurityGroup
	IdHash        string
}

// TSNClusterProvider is an interface that provides a way to create a TSNCluster
// this way, we can have different implementations of TSNCluster creation
type TSNClusterProvider interface {
	CreateCluster(stack awscdk.Stack, input NewTSNClusterInput) TSNCluster
}

type NewTSNClusterInput struct {
	NodesConfig           []kwil_network.KwilNetworkConfig
	TSNDockerComposeAsset awss3assets.Asset
	TSNDockerImageAsset   awsecrassets.DockerImageAsset
	TSNConfigImageAsset   awss3assets.Asset
	HostedZone            awsroute53.IHostedZone
	Vpc                   awsec2.IVpc
	// Controls the restart of the instance when the hash changes.
	IdHash string
}

func NewTSNCluster(scope awscdk.Stack, input NewTSNClusterInput) TSNCluster {
	// create new key pair
	keyPairName := config.KeyPairName(scope)
	if len(keyPairName) == 0 {
		panic("KeyPairName is empty")
	}

	keyPair := awsec2.KeyPair_FromKeyPairName(scope, jsii.String("DefaultKeyPair"), jsii.String(keyPairName))

	role := awsiam.NewRole(scope, jsii.String("TSN-Cluster-Role"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	numOfNodes := len(input.NodesConfig)

	// let's limit it here, so we prevent typos
	if numOfNodes > 5 {
		panic("safety measure: number of nodes should be less than 5")
	}

	allPeerConnections := make([]peer.TSNPeer, numOfNodes)
	for i := 0; i < numOfNodes; i++ {
		allPeerConnections[i] = input.NodesConfig[i].Connection
	}

	securityGroup := tsn.NewTSNSecurityGroup(scope, tsn.NewTSNSecurityGroupInput{
		Vpc: input.Vpc,
	})

	instances := make([]tsn.TSNInstance, numOfNodes)
	for i := 0; i < numOfNodes; i++ {
		instance := tsn.NewTSNInstance(scope, tsn.NewTSNInstanceInput{
			Id:                    strconv.Itoa(i) + "-" + input.IdHash,
			Role:                  role,
			Vpc:                   input.Vpc,
			SecurityGroup:         securityGroup,
			TSNDockerComposeAsset: input.TSNDockerComposeAsset,
			TSNDockerImageAsset:   input.TSNDockerImageAsset,
			TSNConfigImageAsset:   input.TSNConfigImageAsset,
			TSNConfigAsset:        input.NodesConfig[i].Asset,
			PeerConnection:        input.NodesConfig[i].Connection,
			AllPeerConnections:    allPeerConnections,
			KeyPair:               keyPair,
		})
		instances[i] = instance
	}

	// for each connection, create:
	// - elastic ip so it doesn't change throught restarts;
	// - A Record in Route53, so we can use the domain name to connect to the peer
	for i := 0; i < numOfNodes; i++ {
		peerConnection := input.NodesConfig[i].Connection
		instance := instances[i]

		// Create Elastic IP
		eip := awsec2.NewCfnEIP(scope, jsii.String("Peer-EIP-"+strconv.Itoa(i)), &awsec2.CfnEIPProps{})
		// associations make sure not to couple both creation and association
		awsec2.NewCfnEIPAssociation(scope, jsii.String("Peer-EIP-Association-"+strconv.Itoa(i)), &awsec2.CfnEIPAssociationProps{
			AllocationId: eip.AttrAllocationId(),
			InstanceId:   instance.Instance.InstanceId(),
		})

		aRecord := awsroute53.NewARecord(scope, jsii.String("Peer-ARecord-"+strconv.Itoa(i)), &awsroute53.ARecordProps{
			Zone:       input.HostedZone,
			Target:     awsroute53.RecordTarget_FromIpAddresses(eip.AttrPublicIp()),
			RecordName: peerConnection.Address,
		})
		_ = aRecord
	}

	return TSNCluster{
		Nodes:         instances,
		Role:          role,
		SecurityGroup: securityGroup,
		IdHash:        input.IdHash,
	}
}