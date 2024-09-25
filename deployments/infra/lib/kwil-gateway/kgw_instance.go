package kwil_gateway

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
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
	HostedZone     awsroute53.IHostedZone
	KGWDirAsset    awss3assets.Asset
	KGWBinaryAsset utils.S3Object
	Vpc            awsec2.IVpc
	Config         KGWConfig
}

type KGWInstance struct {
	InstanceDnsName *string
	SecurityGroup   awsec2.SecurityGroup
	Role            awsiam.IRole
	LaunchTemplate  awsec2.LaunchTemplate
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

	keyPair := awsec2.KeyPair_FromKeyPairName(scope, jsii.String("KeyPair"), jsii.String(config.KeyPairName(scope)))

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

	// Create launch template
	launchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String("KGWLaunchTemplate"), &awsec2.LaunchTemplateProps{
		InstanceType:  awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_SMALL),
		MachineImage:  AWSLinux2MachineImage,
		SecurityGroup: instanceSG,
		Role:          role,
		KeyPair:       keyPair,
	})

	// Attach init data to the launch template
	initData.Attach(launchTemplate.Node().DefaultChild().(awsec2.CfnLaunchTemplate), &awsec2.AttachInitOptions{
		InstanceRole: role,
		UserData:     launchTemplate.UserData(),
		Platform:     awsec2.OperatingSystemType_LINUX,
	})

	scripts := AddKwilGatewayStartupScriptsToInstance(AddKwilGatewayStartupScriptsOptions{
		kgwBinaryPath: kgwBinaryPath,
		Config:        input.Config,
	})

	launchTemplate.UserData().AddCommands(scripts)

	// so we can later associate when creating the instance
	eip := awsec2.NewCfnEIP(scope, jsii.String("KGWElasticIp"), &awsec2.CfnEIPProps{})

	// give a name so we can identify the eip
	eip.Tags().SetTag(jsii.String("Name"), jsii.String("KGWElasticIp"), jsii.Number(10), jsii.Bool(true))

	domain := config.Domain(scope, "kgw")

	// add record to route53
	awsroute53.NewARecord(scope, jsii.String("KGWARecord"), &awsroute53.ARecordProps{
		Zone:       input.HostedZone,
		Target:     awsroute53.RecordTarget_FromIpAddresses(eip.AttrPublicIp()),
		RecordName: domain,
	})

	return KGWInstance{
		SecurityGroup:   instanceSG,
		Role:            role,
		InstanceDnsName: domain,
		LaunchTemplate:  launchTemplate,
	}
}
