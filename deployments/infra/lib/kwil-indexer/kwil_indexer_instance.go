package kwil_indexer_instance

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	domain "github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/tn"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type KGWConfig struct {
}

type NewIndexerInstanceInput struct {
	Vpc             awsec2.IVpc
	TNInstance      tn.TNInstance
	IndexerDirAsset awss3assets.Asset
	// IndexerBinaryAsset points to the S3 location of the indexer binary zip
	IndexerBinaryAsset utils.S3Object
	HostedDomain       *domain.HostedDomain
	InitElements       []awsec2.InitElement
}

type IndexerInstance struct {
	// security group of the instance to allow communication
	SecurityGroup awsec2.SecurityGroup
	// public DNS name is needed for cloudfront
	IndexerFqdn *string
	// if we need to add policies to the role
	Role awsiam.IRole
	// launch template of the instance
	LaunchTemplate awsec2.LaunchTemplate
	// Elastic IP of the instance
	ElasticIp awsec2.CfnEIP
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
	indexerElasticIp.Tags().SetTag(
		jsii.String("Name"),
		jsii.String(fmt.Sprintf("%s/IndexerElasticIp", *awscdk.Aws_STACK_NAME())),
		jsii.Number(10),
		jsii.Bool(true),
	)

	// Create an A record for the indexer using HostedDomain
	subdomain := "inner-indexer"
	aRecord := input.HostedDomain.AddARecord("IndexerElasticIpDnsRecord", subdomain,
		awsroute53.RecordTarget_FromIpAddresses(indexerElasticIp.AttrPublicIp()),
	)
	// Full DNS name includes subdomain
	indexerFqdn := aRecord.DomainName()

	// Create security group
	instanceSG := awsec2.NewSecurityGroup(scope, jsii.String("IndexerSG"), &awsec2.SecurityGroupProps{
		Vpc:              input.Vpc,
		AllowAllOutbound: jsii.Bool(true),
		Description:      jsii.String("Kwil Indexer Security Group."),
	})

	// These ports will be used by the indexer to communicate with the TN node
	indexerToTnPorts := []struct {
		port int
		name string
	}{
		{peer.TnPostgresPort, "TN Postgres port"},
	}

	// allow communication from indexer to TN node
	for _, p := range indexerToTnPorts {
		input.TNInstance.SecurityGroup.AddIngressRule(
			// we use the elastic ip of the indexer to allow communication via public ip
			// we need to use the public ip because the node anounces itself with the public ip to the indexer
			awsec2.Peer_Ipv4(jsii.String(fmt.Sprintf("%s/32", *indexerElasticIp.AttrPublicIp()))),
			awsec2.Port_Tcp(jsii.Number(p.port)),
			jsii.String(fmt.Sprintf("Allow requests to the %s from the indexer.", p.name)),
			jsii.Bool(false))
	}

	// Ingress rules are applied by the KwilCluster construct based on selected fronting type.

	// ssh
	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(22)),
		jsii.String("Allow ssh."),
		jsii.Bool(false))

	// Get key-pair pointer.
	keyPair := awsec2.KeyPair_FromKeyPairName(scope, jsii.String("KeyPair-ind"), jsii.String(config.KeyPairName(scope)))

	indexerDirZipPath := jsii.String("/home/ec2-user/kwil-indexer.zip")
	// Define path for the downloaded binary zip
	indexerBinaryZipPath := jsii.String("/home/ec2-user/kwil-indexer-binary.zip")

	elements := []awsec2.InitElement{
		awsec2.InitFile_FromExistingAsset(indexerDirZipPath, input.IndexerDirAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		}),
		// Add download for the binary zip from S3
		awsec2.InitFile_FromS3Object(indexerBinaryZipPath, input.IndexerBinaryAsset.Bucket,
			input.IndexerBinaryAsset.Key, &awsec2.InitFileOptions{
				Owner: jsii.String("ec2-user"),
			}),
	}

	// Append base InitElements if provided
	if input.InitElements != nil {
		elements = append(elements, input.InitElements...)
	}

	initData := awsec2.CloudFormationInit_FromElements(elements...)

	// comes with pre-installed cloud init requirements
	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)

	// prepare default UserData to attach later commands
	defaultUd := awsec2.UserData_ForLinux(&awsec2.LinuxUserDataOptions{Shebang: jsii.String("#!/bin/bash -xe")})
	defaultUd.AddCommands(jsii.String("echo 'initializing-indexer'"))

	ltConstructID := "IndexerLaunchTemplate"
	userDataLogPathPrefix := ltConstructID + "/UserData"

	// Create launch template
	launchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String(ltConstructID), &awsec2.LaunchTemplateProps{
		InstanceType:       awsec2.InstanceType_Of(awsec2.InstanceClass_T3A, indexerInstanceSize),
		MachineImage:       AWSLinux2MachineImage,
		SecurityGroup:      instanceSG,
		Role:               role,
		KeyPair:            keyPair,
		UserData:           defaultUd,
		LaunchTemplateName: jsii.Sprintf("%s/%s", *awscdk.Aws_STACK_NAME(), ltConstructID),
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

	// Log Launch Template creation
	instanceTypeStr := "T3a." + string(indexerInstanceSize) // indexerInstanceSize is const awsec2.InstanceSize_SMALL
	amiID := *AWSLinux2MachineImage.GetImage(scope).ImageId
	roleArn := *role.RoleArn()
	cdklogger.LogInfo(scope, ltConstructID, "Created Launch Template: InstanceType=%s, MachineImage=%s, Role=%s", instanceTypeStr, amiID, roleArn)

	// UserData Step 1: CloudFormation Init (asset downloads for Indexer dir and binary)
	cdklogger.LogInfo(scope, userDataLogPathPrefix, "[Step 1/3] Adding CloudFormation Init (asset downloads: Indexer directory zip, Indexer binary zip).")
	utils.AttachInitDataToLaunchTemplate(utils.AttachInitDataToLaunchTemplateInput{
		InitData:       initData,
		LaunchTemplate: launchTemplate,
		Role:           role,
		Platform:       awsec2.OperatingSystemType_LINUX,
	})

	// UserData Step 2: Volume Mount (standard for data drive)
	cdklogger.LogInfo(scope, userDataLogPathPrefix, "[Step 2/3] Adding volume mount command for /data.")
	launchTemplate.UserData().AddCommands(utils.MountVolumeToPathAndPersist("nvme1n1", "/data")...)

	// UserData Step 3: Docker installation, app setup (via AddKwilIndexerStartupScripts)
	cdklogger.LogInfo(scope, userDataLogPathPrefix, "[Step 3/3] Adding Docker installation, configuration, asset preparation, and Indexer application startup script (systemd service).")
	scripts := AddKwilIndexerStartupScripts(AddKwilIndexerStartupScriptsOptions{
		// Pass the path to the binary zip to the startup script options
		indexerBinaryZipPath: indexerBinaryZipPath,
		indexerZippedDirPath: indexerDirZipPath, // Keep existing dir path for now
		TNInstance:           input.TNInstance,
	})

	launchTemplate.UserData().AddCommands(scripts)

	return IndexerInstance{
		SecurityGroup:  instanceSG,
		Role:           role,
		IndexerFqdn:    indexerFqdn,
		ElasticIp:      indexerElasticIp,
		LaunchTemplate: launchTemplate,
	}
}
