package tsn

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/kwil-network"
	peer2 "github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
	"strconv"
)

type NewTSNClusterInput struct {
	NumberOfNodes         int
	TSNDockerComposeAsset awss3assets.Asset
	TSNDockerImageAsset   awsecrassets.DockerImageAsset
	TSNConfigImageAsset   awss3assets.Asset
	Vpc                   awsec2.IVpc
}

type TSNCluster struct {
	Nodes         []TSNInstance
	Role          awsiam.IRole
	SecurityGroup awsec2.SecurityGroup
}

func NewTSNCluster(scope awscdk.Stack, input NewTSNClusterInput) TSNCluster {
	// to be safe, let's create a reasonable ceiling for the number of nodes
	if input.NumberOfNodes > 5 {
		panic("Number of nodes limited to 5 to prevent typos")
	}

	// create new key pair
	keyPairName := config.KeyPairName(scope)
	if len(keyPairName) == 0 {
		panic("KeyPairName is empty")
	}

	keyPair := awsec2.KeyPair_FromKeyPairName(scope, jsii.String("DefaultKeyPair"), jsii.String(keyPairName))

	role := awsiam.NewRole(scope, jsii.String("TSN-Cluster-Role"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	configAssets := kwil_network.NewKwilNetworkConfigAssets(scope, kwil_network.KwilNetworkConfigAssetInput{
		NumberOfNodes: input.NumberOfNodes,
	})

	// we create a peer connection for each node before even creating the instances
	// that's required because Peer Connection info is used at startup scripts
	peerConnections := make([]peer2.PeerConnection, input.NumberOfNodes)
	for i := 0; i < input.NumberOfNodes; i++ {
		elasticIp := awsec2.NewCfnEIP(scope, jsii.String("TSN-Instance-ElasticIp-"+strconv.Itoa(i)), &awsec2.CfnEIPProps{
			Domain: jsii.String("vpc"),
		})
		peerConnections[i] = peer2.NewPeerConnection(elasticIp, configAssets[i].Id)
	}

	securityGroup := NewTSNSecurityGroup(scope, NewTSNSecurityGroupInput{
		vpc:   input.Vpc,
		peers: peerConnections,
	})

	instances := make([]TSNInstance, input.NumberOfNodes)
	for i := 0; i < input.NumberOfNodes; i++ {
		instance := NewTSNInstance(scope, newTSNInstanceInput{
			Id:                    strconv.Itoa(i),
			Role:                  role,
			Vpc:                   input.Vpc,
			SecurityGroup:         securityGroup,
			TSNDockerComposeAsset: input.TSNDockerComposeAsset,
			TSNDockerImageAsset:   input.TSNDockerImageAsset,
			TSNConfigImageAsset:   input.TSNConfigImageAsset,
			TSNConfigAsset:        configAssets[i].Asset,
			PeerConnection:        peerConnections[i],
			AllPeerConnections:    peerConnections,
			KeyPair:               keyPair,
		})
		instances[i] = instance
	}

	return TSNCluster{
		Nodes:         instances,
		Role:          role,
		SecurityGroup: securityGroup,
	}
}
