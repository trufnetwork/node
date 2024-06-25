package tsn

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/kwilteam/kwil-db/core/utils/random"
	peer2 "github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
)

type newTSNInstanceInput struct {
	Id                    string
	Role                  awsiam.IRole
	Vpc                   awsec2.IVpc
	SecurityGroup         awsec2.ISecurityGroup
	TSNDockerComposeAsset awss3assets.Asset
	TSNDockerImageAsset   awsecrassets.DockerImageAsset
	TSNConfigAsset        awss3assets.Asset
	TSNConfigImageAsset   awss3assets.Asset
	PeerConnection        peer2.PeerConnection
	AllPeerConnections    []peer2.PeerConnection
	KeyPair               awsec2.IKeyPair
}

type TSNInstance struct {
	Instance       awsec2.Instance
	SecurityGroup  awsec2.ISecurityGroup
	Role           awsiam.IRole
	PeerConnection peer2.PeerConnection
}

func NewTSNInstance(scope constructs.Construct, input newTSNInstanceInput) TSNInstance {
	randomBit := random.String(4)

	// Create tsnInstance using randomBit so that the tsnInstance is recreated on each deployment.
	name := "TSN-Instance-" + input.Id + "-" + randomBit

	// Creating in private subnet only when deployment cluster in PROD stage.
	subnetType := awsec2.SubnetType_PUBLIC
	//if config.DeploymentStage(scope) == config.DeploymentStage_PROD {
	//	subnetType = awsec2.SubnetType_PRIVATE_WITH_NAT
	//}

	defaultInstanceUser := jsii.String("ec2-user")

	tsnConfigZipPath := "/home/ec2-user/tsn-node-config.zip"
	tsnComposePath := "/home/ec2-user/docker-compose.yaml"
	tsnConfigImagePath := "/home/ec2-user/deployments/tsn-config.dockerfile"

	initData := awsec2.CloudFormationInit_FromElements(
		awsec2.InitFile_FromExistingAsset(jsii.String(tsnComposePath), input.TSNDockerComposeAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String(tsnConfigZipPath), input.TSNConfigAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String(tsnConfigImagePath), input.TSNConfigImageAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
	)

	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)
	instance := awsec2.NewInstance(scope, jsii.String(name), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_MEDIUM),
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

	// Create Elastic Ip association instead of attaching, so dependency is not circular
	awsec2.NewCfnEIPAssociation(scope, jsii.String("TSN-Instance-ElasticIpAssociation-"+input.Id), &awsec2.CfnEIPAssociationProps{
		InstanceId:   instance.InstanceId(),
		AllocationId: input.PeerConnection.ElasticIp.AttrAllocationId(),
	})

	node := TSNInstance{
		Instance:       instance,
		SecurityGroup:  input.SecurityGroup,
		Role:           input.Role,
		PeerConnection: input.PeerConnection,
	}

	AddTsnDbStartupScriptsToInstance(scope, AddStartupScriptsOptions{
		currentPeer:        input.PeerConnection,
		allPeers:           input.AllPeerConnections,
		Instance:           instance,
		Region:             input.Vpc.Env().Region,
		TsnImageAsset:      input.TSNDockerImageAsset,
		TsnConfigZipPath:   &tsnConfigZipPath,
		TsnComposePath:     &tsnComposePath,
		TsnConfigImagePath: &tsnConfigImagePath,
	})

	return node
}
