package kwil_indexer_instance

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
	"github.com/truflation/tsn-db/infra/lib/tsn"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type KGWConfig struct {
}

type NewIndexerInstanceInput struct {
	Vpc             awsec2.IVpc
	TSNInstance     tsn.TSNInstance
	IndexerDirAsset awss3assets.Asset
	HostedZone      awsroute53.IHostedZone
	Domain          *string
}

type IndexerInstance struct {
	Instance awsec2.Instance
	// security group of the instance to allow communication
	SecurityGroup awsec2.SecurityGroup
	// public DNS name is needed for cloudfront
	InstanceDnsName *string
	// if we need to add policies to the role
	Role awsiam.IRole
}

const IndexerVolumeSize = 50
const indexerInstanceSize = awsec2.InstanceSize_SMALL

func NewIndexerInstance(scope constructs.Construct, input NewIndexerInstanceInput) IndexerInstance {
	role := awsiam.NewRole(scope, jsii.String("IndexerInstanceRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	// create a new elastic ip, because we need the ip before the instance is created
	indexerElasticIp := awsec2.NewCfnEIP(scope, jsii.String("IndexerElasticIp"), &awsec2.CfnEIPProps{})

	// give a name so we can identify the eip
	indexerElasticIp.Tags().SetTag(jsii.String("Name"), jsii.String("IndexerElasticIp"), jsii.Number(10), jsii.Bool(true))

	// Create an A record pointing to the Elastic IP, as EIP doesn't automatically create a DNS record
	aRecord := awsroute53.NewARecord(scope, jsii.String("IndexerElasticIpDnsRecord"), &awsroute53.ARecordProps{
		Zone:       input.HostedZone,
		RecordName: jsii.String("indexer." + *input.Domain),
		Target:     awsroute53.RecordTarget_FromIpAddresses(indexerElasticIp.AttrPublicIp()),
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
	}

	// allow communication from indexer to TSN node
	for _, p := range indexerToTsnPorts {
		input.TSNInstance.SecurityGroup.AddIngressRule(
			// we use the elastic ip of the indexer to allow communication via public ip
			// we need to use the public ip because the node anounces itself with the public ip to the indexer
			awsec2.Peer_Ipv4(jsii.String(fmt.Sprintf("%s/32", *indexerElasticIp.AttrPublicIp()))),
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

	// Get key-pair pointer.
	keyPair := awsec2.KeyPair_FromKeyPairName(scope, jsii.String("KeyPair-ind"), jsii.String(config.KeyPairName(scope)))

	indexerZippedDirPath := jsii.String("/home/ec2-user/kwil-indexer.zip")

	initData := awsec2.CloudFormationInit_FromElements(
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ec2-user/kwil-indexer.zip"), input.IndexerDirAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		}),
	)

	// comes with pre-installed cloud init requirements
	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)

	// Create launch template
	launchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String("IndexerLaunchTemplate"), &awsec2.LaunchTemplateProps{
		InstanceType:  awsec2.InstanceType_Of(awsec2.InstanceClass_T3, indexerInstanceSize),
		MachineImage:  AWSLinux2MachineImage,
		SecurityGroup: instanceSG,
		Role:          role,
		KeyPair:       keyPair,
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

	// Attach init data to the launch template
	initData.Attach(launchTemplate.Node().DefaultChild().(awsec2.CfnLaunchTemplate), &awsec2.AttachInitOptions{
		InstanceRole: role,
		UserData:     launchTemplate.UserData(),
		Platform:     awsec2.OperatingSystemType_LINUX,
	})

	scripts := AddKwilIndexerStartupScripts(AddKwilIndexerStartupScriptsOptions{
		indexerZippedDirPath: indexerZippedDirPath,
		TSNInstance:          input.TSNInstance,
	})

	launchTemplate.UserData().AddCommands(scripts)

	launchTemplate.UserData().AddCommands(utils.MountVolumeToPathAndPersist("nvme1n1", "/data")...)

	return IndexerInstance{
		SecurityGroup:   instanceSG,
		Role:            role,
		InstanceDnsName: aRecord.DomainName(),
	}
}
