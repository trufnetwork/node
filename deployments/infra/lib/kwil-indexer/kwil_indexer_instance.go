package kwil_indexer_instance

import (
	"fmt"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/kwilteam/kwil-db/core/utils/random"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
	"github.com/truflation/tsn-db/infra/lib/tsn"
)

type KGWConfig struct {
}

type NewIndexerInstanceInput struct {
	Vpc             awsec2.IVpc
	TSNInstance     tsn.TSNInstance
	IndexerDirAsset awss3assets.Asset
}

type IndexerInstance struct {
	Instance      awsec2.Instance
	SecurityGroup awsec2.SecurityGroup
	Role          awsiam.IRole
}

const IndexerVolumeSize = 50
const indexerInstanceSize = awsec2.InstanceSize_SMALL

func NewIndexerInstance(scope constructs.Construct, input NewIndexerInstanceInput) IndexerInstance {
	role := awsiam.NewRole(scope, jsii.String("IndexerInstanceRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	// Create security group
	instanceSG := awsec2.NewSecurityGroup(scope, jsii.String("IndexerSG"), &awsec2.SecurityGroupProps{
		Vpc:              input.Vpc,
		AllowAllOutbound: jsii.Bool(true),
		Description:      jsii.String("Kwil Indexer Security Group."),
	})

	// These ports will be used by the indexer to communicate with the TSN node
	indexerToTsnPorts := []struct {
		port int
		name string
	}{
		{peer.TSNPostgresPort, "TSN Postgres port"},
		{peer.TsnCometBFTRPCPort, "TSN Comet BFT RPC port"},
		{peer.TsnP2pPort, "TSN P2P port"},
	}

	// allow communication from indexer to TSN node
	for _, p := range indexerToTsnPorts {
		input.TSNInstance.SecurityGroup.AddIngressRule(
			// for this to work, it's necessary to use the private IP of the tsn node
			instanceSG,
			awsec2.Port_Tcp(jsii.Number(p.port)),
			jsii.String(fmt.Sprintf("Allow requests to the %s from the indexer.", p.name)),
			jsii.Bool(false))
	}

	// TODO security could be hardened by allowing only specific IPs
	//   relative to cloudfront distribution IPs
	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(80)),
		jsii.String("Allow requests to kwil-indexer."),
		jsii.Bool(false))

	// ssh
	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(22)),
		jsii.String("Allow ssh."),
		jsii.Bool(false))

	subnetType := awsec2.SubnetType_PUBLIC

	// Get key-pair pointer.
	var keyPair *string = nil
	if len(config.KeyPairName(scope)) > 0 {
		keyPair = jsii.String(config.KeyPairName(scope))
	}

	indexerZippedDirPath := jsii.String("/home/ec2-user/kwil-indexer.zip")

	initData := awsec2.CloudFormationInit_FromElements(
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ec2-user/kwil-indexer.zip"), input.IndexerDirAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		}),
	)

	randomBit := random.String(4)

	// comes with pre-installed cloud init requirements
	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)
	instance := awsec2.NewInstance(scope, jsii.String("IndexerInstance"+randomBit), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_T3, indexerInstanceSize),
		Init:         initData,
		MachineImage: AWSLinux2MachineImage,
		Vpc:          input.Vpc,
		VpcSubnets: &awsec2.SubnetSelection{
			SubnetType: subnetType,
		},
		SecurityGroup: instanceSG,
		KeyPair:       awsec2.KeyPair_FromKeyPairName(scope, jsii.String("KeyPair-ind"), keyPair),
		BlockDevices: &[]*awsec2.BlockDevice{
			{
				DeviceName: jsii.String("/dev/sda1"),
				Volume: awsec2.BlockDeviceVolume_Ebs(jsii.Number(IndexerVolumeSize), &awsec2.EbsDeviceOptions{
					DeleteOnTermination: jsii.Bool(true),
					Encrypted:           jsii.Bool(false),
				}),
			},
		},
	})

	AddKwilIndexerStartupScriptsToInstance(AddKwilIndexerStartupScriptsOptions{
		indexerZippedDirPath: indexerZippedDirPath,
		TSNInstance:          input.TSNInstance,
		IndexerInstance:      instance,
	})

	return IndexerInstance{
		Instance:      instance,
		SecurityGroup: instanceSG,
		Role:          role,
	}
}
