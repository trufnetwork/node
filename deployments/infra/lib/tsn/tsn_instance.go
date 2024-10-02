package tsn

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	peer2 "github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type NewTSNInstanceInput struct {
	Index                 int
	Id                    string
	Role                  awsiam.IRole
	Vpc                   awsec2.IVpc
	SecurityGroup         awsec2.ISecurityGroup
	TSNDockerComposeAsset awss3assets.Asset
	TSNDockerImageAsset   awsecrassets.DockerImageAsset
	TSNConfigAsset        awss3assets.Asset
	TSNConfigImageAsset   awss3assets.Asset
	InitElements          []awsec2.InitElement
	PeerConnection        peer2.TSNPeer
	AllPeerConnections    []peer2.TSNPeer
	KeyPair               awsec2.IKeyPair
}

type TSNInstance struct {
	Index          int
	LaunchTemplate awsec2.LaunchTemplate
	SecurityGroup  awsec2.ISecurityGroup
	Role           awsiam.IRole
	ElasticIp      awsec2.CfnEIP
	PeerConnection peer2.TSNPeer
}

func NewTSNInstance(scope constructs.Construct, input NewTSNInstanceInput) TSNInstance {
	name := "TSN-Instance-" + input.Id
	index := input.Index

	defaultInstanceUser := jsii.String("ec2-user")

	initAssetsDir := "/home/ec2-user/init-assets/"
	mountDataDir := "/data/"
	tsnConfigZipFile := "tsn-node-config.zip"
	tsnComposeFile := "docker-compose.yaml"
	tsnConfigImageFile := "deployments/tsn-config.dockerfile"

	elements := []awsec2.InitElement{
		awsec2.InitFile_FromExistingAsset(jsii.String(initAssetsDir+tsnComposeFile), input.TSNDockerComposeAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String(initAssetsDir+tsnConfigZipFile), input.TSNConfigAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String(initAssetsDir+tsnConfigImageFile), input.TSNConfigImageAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
	}

	elements = append(elements, input.InitElements...)

	initData := awsec2.CloudFormationInit_FromElements(elements...)

	// instance size is based on the deployment stage
	// DEV: t3.small
	// STAGING, PROD: t3.medium
	var instanceSize awsec2.InstanceSize
	switch config.DeploymentStage(scope) {
	case config.DeploymentStage_DEV:
		instanceSize = awsec2.InstanceSize_SMALL
	case config.DeploymentStage_STAGING, config.DeploymentStage_PROD:
		instanceSize = awsec2.InstanceSize_MEDIUM
	}

	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)
	tsnLaunchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String(name), &awsec2.LaunchTemplateProps{
		InstanceType:       awsec2.InstanceType_Of(awsec2.InstanceClass_T3, instanceSize),
		MachineImage:       AWSLinux2MachineImage,
		SecurityGroup:      input.SecurityGroup,
		Role:               input.Role,
		KeyPair:            input.KeyPair,
		LaunchTemplateName: jsii.Sprintf("%s/%s", *awscdk.Aws_STACK_NAME(), name),
		BlockDevices: &[]*awsec2.BlockDevice{
			{
				DeviceName: jsii.String("/dev/sda1"),
				Volume: awsec2.BlockDeviceVolume_Ebs(jsii.Number(50), &awsec2.EbsDeviceOptions{
					DeleteOnTermination: jsii.Bool(true),
					Encrypted:           jsii.Bool(false),
				}),
			},
		},
	})

	// first step is to attach the init data to the launch template
	utils.AttachInitDataToLaunchTemplate(utils.AttachInitDataToLaunchTemplateInput{
		LaunchTemplate: tsnLaunchTemplate,
		InitData:       initData,
		Role:           input.Role,
		Platform:       awsec2.OperatingSystemType_LINUX,
	})

	tsnLaunchTemplate.UserData().AddCommands(
		utils.MountVolumeToPathAndPersist("nvme1n1", "/data")...,
	)
	tsnLaunchTemplate.UserData().AddCommands(utils.MoveToPath(initAssetsDir+"*", mountDataDir))

	node := TSNInstance{
		LaunchTemplate: tsnLaunchTemplate,
		SecurityGroup:  input.SecurityGroup,
		Role:           input.Role,
		PeerConnection: input.PeerConnection,
		Index:          index,
	}

	scripts := TsnDbStartupScripts(AddStartupScriptsOptions{
		currentPeer:        input.PeerConnection,
		allPeers:           input.AllPeerConnections,
		Region:             input.Vpc.Env().Region,
		TsnImageAsset:      input.TSNDockerImageAsset,
		DataDirPath:        jsii.String(mountDataDir),
		TsnConfigZipPath:   jsii.String(mountDataDir + tsnConfigZipFile),
		TsnComposePath:     jsii.String(mountDataDir + tsnComposeFile),
		TsnConfigImagePath: jsii.String(mountDataDir + tsnConfigImageFile),
	})

	tsnLaunchTemplate.UserData().AddCommands(scripts)

	return node
}
