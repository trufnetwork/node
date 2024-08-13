package tsn

import (
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
	Id                    string
	Role                  awsiam.IRole
	Vpc                   awsec2.IVpc
	SecurityGroup         awsec2.ISecurityGroup
	TSNDockerComposeAsset awss3assets.Asset
	TSNDockerImageAsset   awsecrassets.DockerImageAsset
	TSNConfigAsset        awss3assets.Asset
	TSNConfigImageAsset   awss3assets.Asset
	PeerConnection        peer2.TSNPeer
	AllPeerConnections    []peer2.TSNPeer
	KeyPair               awsec2.IKeyPair
}

type TSNInstance struct {
	Instance       awsec2.Instance
	SecurityGroup  awsec2.ISecurityGroup
	Role           awsiam.IRole
	PeerConnection peer2.TSNPeer
}

func NewTSNInstance(scope constructs.Construct, input NewTSNInstanceInput) TSNInstance {
	name := "TSN-Instance-" + input.Id

	subnetType := awsec2.SubnetType_PUBLIC
	//if config.DeploymentStage(scope) == config.DeploymentStage_PROD {
	//	subnetType = awsec2.SubnetType_PRIVATE_WITH_NAT
	//}

	defaultInstanceUser := jsii.String("ec2-user")

	initAssetsDir := "/home/ec2-user/init-assets/"
	mountDataDir := "/data/"
	tsnConfigZipFile := "tsn-node-config.zip"
	tsnComposeFile := "docker-compose.yaml"
	tsnConfigImageFile := "deployments/tsn-config.dockerfile"

	initData := awsec2.CloudFormationInit_FromElements(
		awsec2.InitFile_FromExistingAsset(jsii.String(initAssetsDir+tsnComposeFile), input.TSNDockerComposeAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String(initAssetsDir+tsnConfigZipFile), input.TSNConfigAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String(initAssetsDir+tsnConfigImageFile), input.TSNConfigImageAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
	)

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
	instance := awsec2.NewInstance(scope, jsii.String(name), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_T3, instanceSize),
		Init:         initData,
		MachineImage: AWSLinux2MachineImage,
		Vpc:          input.Vpc,
		VpcSubnets: &awsec2.SubnetSelection{
			SubnetType: subnetType,
		},
		SecurityGroup: input.SecurityGroup,
		Role:          input.Role,
		KeyPair:       input.KeyPair,
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

	instance.AddUserData(
		utils.MountVolumeToPathAndPersist("nvme1n1", "/data")...,
	)
	instance.AddUserData(utils.MoveToPath(initAssetsDir+"*", mountDataDir))

	node := TSNInstance{
		Instance:       instance,
		SecurityGroup:  input.SecurityGroup,
		Role:           input.Role,
		PeerConnection: input.PeerConnection,
	}

	AddTsnDbStartupScriptsToInstance(AddStartupScriptsOptions{
		currentPeer:        input.PeerConnection,
		allPeers:           input.AllPeerConnections,
		Instance:           instance,
		Region:             input.Vpc.Env().Region,
		TsnImageAsset:      input.TSNDockerImageAsset,
		DataDirPath:        jsii.String(mountDataDir),
		TsnConfigZipPath:   jsii.String(mountDataDir + tsnConfigZipFile),
		TsnComposePath:     jsii.String(mountDataDir + tsnComposeFile),
		TsnConfigImagePath: jsii.String(mountDataDir + tsnConfigImageFile),
	})

	return node
}
