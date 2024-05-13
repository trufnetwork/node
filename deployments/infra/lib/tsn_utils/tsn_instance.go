package tsn_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
)

type newTSNInstanceInput struct {
	Id                    string
	Role                  awsiam.IRole
	Vpc                   awsec2.IVpc
	SecurityGroup         awsec2.ISecurityGroup
	TSNDockerComposeAsset awss3assets.Asset
	TSNDockerImageAsset   awsecrassets.DockerImageAsset
	TSNConfigAsset        awss3assets.Asset
}

type TSNInstance struct {
	Instance      awsec2.Instance
	SecurityGroup awsec2.ISecurityGroup
	Role          awsiam.IRole
}

func NewTSNInstance(scope constructs.Construct, input newTSNInstanceInput) TSNInstance {
	// Create tsnInstance using tsnImageAsset hash so that the tsnInstance is recreated when the image changes.
	name := "TSN-Instance-" + input.Id + "-" + *input.TSNDockerImageAsset.AssetHash()

	// Creating in private subnet only when deployment cluster in PROD stage.
	subnetType := awsec2.SubnetType_PUBLIC
	//if config.DeploymentStage(scope) == config.DeploymentStage_PROD {
	//	subnetType = awsec2.SubnetType_PRIVATE_WITH_NAT
	//}

	// Get key-pair pointer.
	var keyPair *string = nil
	if len(config.KeyPairName(scope)) > 0 {
		keyPair = jsii.String(config.KeyPairName(scope))
	}

	defaultInstanceUser := jsii.String("ec2-user")

	initData := awsec2.CloudFormationInit_FromElements(
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ec2-user/docker-compose.yaml"), input.TSNDockerComposeAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ec2-user/tsn-node-config.zip"), input.TSNConfigAsset, &awsec2.InitFileOptions{
			Owner: defaultInstanceUser,
		}),
	)

	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)
	instance := awsec2.NewInstance(scope, jsii.String(name), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_SMALL),
		Init:         initData,
		MachineImage: AWSLinux2MachineImage,
		Vpc:          input.Vpc,
		VpcSubnets: &awsec2.SubnetSelection{
			SubnetType: subnetType,
		},
		SecurityGroup: input.SecurityGroup,
		Role:          input.Role,
		KeyPair:       awsec2.KeyPair_FromKeyPairName(scope, jsii.String("KeyPair"), keyPair),
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

	AddTsnDbStartupScriptsToInstance(scope, AddStartupScriptsOptions{
		Instance:      instance,
		TsnImageAsset: nil,
	})

	return TSNInstance{
		Instance:      instance,
		SecurityGroup: input.SecurityGroup,
		Role:          input.Role,
	}
}
