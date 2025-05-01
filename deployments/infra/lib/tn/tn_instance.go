package tn

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type NewTNInstanceInput struct {
	Index                int
	Id                   string
	Role                 awsiam.IRole
	Vpc                  awsec2.IVpc
	SecurityGroup        awsec2.ISecurityGroup
	TNDockerComposeAsset awss3assets.Asset
	TNDockerImageAsset   awsecrassets.DockerImageAsset
	RenderedConfigAsset  awss3assets.Asset
	GenesisAsset         awss3assets.Asset
	NodeKeyJsonAsset     awss3assets.Asset
	TNConfigImageAsset   awss3assets.Asset
	InitElements         []awsec2.InitElement
	PeerConnection       peer2.TNPeer
	AllPeerConnections   []peer2.TNPeer
	KeyPair              awsec2.IKeyPair
}

type TNInstance struct {
	Index          int
	LaunchTemplate awsec2.LaunchTemplate
	SecurityGroup  awsec2.ISecurityGroup
	Role           awsiam.IRole
	ElasticIp      awsec2.CfnEIP
	PeerConnection peer2.TNPeer
}

func NewTNInstance(scope constructs.Construct, input NewTNInstanceInput) TNInstance {
	name := "TN-Instance-" + input.Id
	index := input.Index

	defaultInstanceUser := jsii.String("ec2-user")

	// Determine instance size based on stage from context
	stage := config.GetStage(scope)

	initAssetsDir := "/home/ec2-user/init-assets/"
	initTempDir := "/tmp/init-tn-files/" // Temporary directory for init files
	mountDataDir := "/data/"
	tnComposeFile := "docker-compose.yaml"
	tnConfigImageFile := "deployments/tn-config.dockerfile"

	elements := []awsec2.InitElement{
		awsec2.InitFile_FromExistingAsset(jsii.String(initAssetsDir+tnComposeFile), input.TNDockerComposeAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String(initAssetsDir+tnConfigImageFile), input.TNConfigImageAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		// Place config and genesis in a temporary directory first
		awsec2.InitFile_FromExistingAsset(
			jsii.String(initTempDir+"config.toml"), // Use temporary path
			input.RenderedConfigAsset,
			&awsec2.InitFileOptions{
				Owner: defaultInstanceUser,
				Group: defaultInstanceUser,
				Mode:  jsii.String("000644"),
			},
		),
		awsec2.InitFile_FromExistingAsset(
			jsii.String(initTempDir+"genesis.json"), // Use temporary path
			input.GenesisAsset,
			&awsec2.InitFileOptions{
				Owner: defaultInstanceUser,
				Group: defaultInstanceUser,
				Mode:  jsii.String("000644"),
			},
		),
		awsec2.InitFile_FromExistingAsset(
			jsii.String(initTempDir+"nodekey.json"), // Target filename in temp dir
			input.NodeKeyJsonAsset,                  // Use the new asset passed in input
			&awsec2.InitFileOptions{
				Owner: defaultInstanceUser,
				Group: defaultInstanceUser,
				Mode:  jsii.String("000600"), // Restrict permissions for the key file
			},
		),
	}

	// Append base InitElements if provided
	if input.InitElements != nil {
		elements = append(elements, input.InitElements...)
	}

	initData := awsec2.CloudFormationInit_FromElements(elements...)

	// instance size is based on the deployment stage parameter
	// TODO this should be just a default, but also an optional parameter to override
	// DEV: t3.small, PROD: t3.medium
	var instanceSize awsec2.InstanceSize
	switch stage {
	case domaincfg.StageDev:
		instanceSize = awsec2.InstanceSize_SMALL
	case domaincfg.StageProd:
		instanceSize = awsec2.InstanceSize_MEDIUM
	default:
		instanceSize = awsec2.InstanceSize_MEDIUM
	}

	var volumeSize int
	switch stage {
	case domaincfg.StageDev:
		volumeSize = 50
	case domaincfg.StageProd:
		volumeSize = 400
	}

	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)
	userData := awsec2.UserData_ForLinux(nil)
	tnLaunchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String(name), &awsec2.LaunchTemplateProps{
		InstanceType:       awsec2.InstanceType_Of(awsec2.InstanceClass_T3, instanceSize),
		MachineImage:       AWSLinux2MachineImage,
		SecurityGroup:      input.SecurityGroup,
		Role:               input.Role,
		KeyPair:            input.KeyPair,
		LaunchTemplateName: jsii.Sprintf("%s/%s", *awscdk.Aws_STACK_NAME(), name),
		BlockDevices: &[]*awsec2.BlockDevice{
			{
				DeviceName: jsii.String("/dev/sda1"),
				Volume: awsec2.BlockDeviceVolume_Ebs(jsii.Number(volumeSize), &awsec2.EbsDeviceOptions{
					DeleteOnTermination: jsii.Bool(true),
					Encrypted:           jsii.Bool(false),
				}),
			},
		},
		UserData: userData,
	})

	// first step is to attach the init data to the launch template
	utils.AttachInitDataToLaunchTemplate(utils.AttachInitDataToLaunchTemplateInput{
		LaunchTemplate: tnLaunchTemplate,
		InitData:       initData,
		Role:           input.Role,
		Platform:       awsec2.OperatingSystemType_LINUX,
	})

	tnLaunchTemplate.UserData().AddCommands(
		utils.MountVolumeToPathAndPersist("nvme1n1", "/data")...,
	)
	// Ensure the target directory exists on the mounted volume and move files
	tnLaunchTemplate.UserData().AddCommands(
		jsii.Sprintf("sudo mkdir -p %stn", mountDataDir),
		jsii.Sprintf("sudo mv %s* %stn/", initTempDir, mountDataDir),
		jsii.Sprintf("sudo chown %s:%s %stn/*", *defaultInstanceUser, *defaultInstanceUser, mountDataDir),
		jsii.Sprintf("sudo rm -rf %s", initTempDir), // Clean up temp dir
	)
	// Move other assets from initAssetsDir after mount
	tnLaunchTemplate.UserData().AddCommands(utils.MoveToPath(initAssetsDir+"*", mountDataDir))

	node := TNInstance{
		LaunchTemplate: tnLaunchTemplate,
		SecurityGroup:  input.SecurityGroup,
		Role:           input.Role,
		PeerConnection: input.PeerConnection,
		Index:          index,
	}

	scripts, err := TnDbStartupScripts(AddStartupScriptsOptions{
		CurrentPeer:       input.PeerConnection,
		AllPeers:          input.AllPeerConnections,
		Region:            input.Vpc.Env().Region,
		TnImageAsset:      input.TNDockerImageAsset,
		DataDirPath:       jsii.String(mountDataDir),
		TnComposePath:     jsii.String(mountDataDir + tnComposeFile),
		TnConfigImagePath: jsii.String(mountDataDir + tnConfigImageFile),
	})
	if err != nil {
		panic(err)
	}
	tnLaunchTemplate.UserData().AddCommands(jsii.String(*scripts))

	return node
}
