package kwil_gateway

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/tsn"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type KGWConfig struct {
	CorsAllowOrigins *string
	Domain           *string
	SessionSecret    *string
	ChainId          *string
	Nodes            []tsn.TSNInstance
}

type NewKGWInstanceInput struct {
	KGWDirAsset    awss3assets.Asset
	KGWBinaryAsset utils.S3Object
	Vpc            awsec2.IVpc
	Config         KGWConfig
	// Controls the restart of the instance when the hash changes.
	IdHash string
}

type KGWInstance struct {
	Instance      awsec2.Instance
	SecurityGroup awsec2.SecurityGroup
	Role          awsiam.IRole
}

func NewKGWInstance(scope constructs.Construct, input NewKGWInstanceInput) KGWInstance {

	role := awsiam.NewRole(scope, jsii.String("KGWInstanceRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	// Create security group
	instanceSG := awsec2.NewSecurityGroup(scope, jsii.String("NodeSG"), &awsec2.SecurityGroupProps{
		Vpc:              input.Vpc,
		AllowAllOutbound: jsii.Bool(true),
		Description:      jsii.String("TSN-DB Instance security group."),
	})

	// TODO security could be hardened by allowing only specific IPs
	//   relative to cloudfront distribution IPs
	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(80)),
		jsii.String("Allow requests to http."),
		jsii.Bool(false))

	// ssh
	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(22)),
		jsii.String("Allow ssh."),
		jsii.Bool(false))

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

	kgwBinaryPath := jsii.String("/home/ec2-user/kgw-binary.zip")

	initData := awsec2.CloudFormationInit_FromElements(
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ec2-user/kgw.zip"), input.KGWDirAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		}),
		awsec2.InitFile_FromS3Object(kgwBinaryPath, input.KGWBinaryAsset.Bucket,
			input.KGWBinaryAsset.Key, &awsec2.InitFileOptions{
				Owner: jsii.String("ec2-user"),
			}),
	)

	// comes with pre-installed cloud init requirements
	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)
	instance := awsec2.NewInstance(scope, jsii.String("KGWInstance"+input.IdHash), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_SMALL),
		Init:         initData,
		MachineImage: AWSLinux2MachineImage,
		Vpc:          input.Vpc,
		VpcSubnets: &awsec2.SubnetSelection{
			SubnetType: subnetType,
		},
		SecurityGroup: instanceSG,
		KeyPair:       awsec2.KeyPair_FromKeyPairName(scope, jsii.String("KeyPair"), keyPair),
	})

	AddKwilGatewayStartupScriptsToInstance(AddKwilGatewayStartupScriptsOptions{
		Instance:      instance,
		kgwBinaryPath: kgwBinaryPath,
		Config:        input.Config,
	})

	return KGWInstance{
		Instance:      instance,
		SecurityGroup: instanceSG,
		Role:          role,
	}
}
