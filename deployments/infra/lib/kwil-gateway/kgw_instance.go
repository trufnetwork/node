package kwil_gateway

import (
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
	"github.com/trufnetwork/node/infra/lib/tn"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type KGWConfig struct {
	CorsAllowOrigins   *string
	Domain             *string
	SessionSecret      *string
	ChainId            *string
	Nodes              []tn.TNInstance
	XffTrustProxyCount *string
}

type NewKGWInstanceInput struct {
	// HostedDomain encapsulates zone lookup and certificate
	HostedDomain   *domain.HostedDomain
	KGWDirAsset    awss3assets.Asset
	KGWBinaryAsset utils.S3Object
	Vpc            awsec2.IVpc
	Config         KGWConfig
	InitElements   []awsec2.InitElement
}

type KGWInstance struct {
	// Specific FQDN created for the gateway instance's A record (e.g., inner-gateway.dev.infra.truf.network)
	GatewayFqdn    *string
	SecurityGroup  awsec2.SecurityGroup
	Role           awsiam.IRole
	LaunchTemplate awsec2.LaunchTemplate
	ElasticIp      awsec2.CfnEIP
}

func NewKGWInstance(scope constructs.Construct, input NewKGWInstanceInput) KGWInstance {
	role := awsiam.NewRole(scope, jsii.String("KGWInstanceRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	// Create security group
	instanceSG := awsec2.NewSecurityGroup(scope, jsii.String("KGWNodeSG"), &awsec2.SecurityGroupProps{
		Vpc:              input.Vpc,
		AllowAllOutbound: jsii.Bool(true),
		Description:      jsii.String("KGW Instance security group."),
	})

	// Ingress rules are applied by the KwilCluster construct based on selected fronting type.

	// ssh
	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(22)),
		jsii.String("Allow ssh."),
		jsii.Bool(false))

	keyPair := awsec2.KeyPair_FromKeyPairName(scope, jsii.String("KGWKeyPair"), jsii.String(config.KeyPairName(scope)))

	kgwBinaryPath := jsii.String("/home/ec2-user/kgw-binary.zip")
	kgwDirZipPath := jsii.String("/home/ec2-user/kgw.zip")

	elements := []awsec2.InitElement{
		awsec2.InitFile_FromExistingAsset(kgwDirZipPath, input.KGWDirAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		}),
		awsec2.InitFile_FromS3Object(kgwBinaryPath, input.KGWBinaryAsset.Bucket,
			input.KGWBinaryAsset.Key, &awsec2.InitFileOptions{
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
	defaultUd.AddCommands(jsii.String("echo 'initializing-kgw'"))

	ltConstructID := "KGWLaunchTemplate"
	userDataLogPathPrefix := ltConstructID + "/UserData"

	// Create launch template
	launchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String(ltConstructID), &awsec2.LaunchTemplateProps{
		InstanceType:       awsec2.InstanceType_Of(awsec2.InstanceClass_T3A, awsec2.InstanceSize_SMALL),
		MachineImage:       AWSLinux2MachineImage,
		SecurityGroup:      instanceSG,
		Role:               role,
		KeyPair:            keyPair,
		UserData:           defaultUd,
		LaunchTemplateName: jsii.Sprintf("%s/%s", *awscdk.Aws_STACK_NAME(), ltConstructID),
	})

	// Log Launch Template creation
	instanceTypeStr := "T3.SMALL" // Hardcoded as per LaunchTemplateProps
	amiID := *AWSLinux2MachineImage.GetImage(scope).ImageId
	roleArn := *role.RoleArn()
	cdklogger.LogInfo(scope, ltConstructID, "Created Launch Template: InstanceType=%s, MachineImage=%s, Role=%s", instanceTypeStr, amiID, roleArn)

	// UserData Step 1: CloudFormation Init (asset downloads for KGW dir and binary)
	cdklogger.LogInfo(scope, userDataLogPathPrefix, "[Step 1/3] Adding CloudFormation Init (asset downloads: KGW directory zip, KGW binary zip).")
	utils.AttachInitDataToLaunchTemplate(utils.AttachInitDataToLaunchTemplateInput{
		InitData:       initData,
		LaunchTemplate: launchTemplate,
		Role:           role,
		Platform:       awsec2.OperatingSystemType_LINUX,
	})

	// UserData Steps 2 & 3: Docker installation, app setup (via AddKwilGatewayStartupScriptsToInstance)
	cdklogger.LogInfo(scope, userDataLogPathPrefix, "[Step 2/3 & 3/3] Adding Docker installation, configuration, asset preparation, and KGW application startup script (systemd service).")
	scripts := AddKwilGatewayStartupScriptsToInstance(AddKwilGatewayStartupScriptsOptions{
		kgwBinaryPath: kgwBinaryPath,
		Config:        input.Config,
		KGWDirZipPath: kgwDirZipPath,
	})
	launchTemplate.UserData().AddCommands(scripts)

	// so we can later associate when creating the instance
	eip := awsec2.NewCfnEIP(scope, jsii.String("KGWElasticIp"), &awsec2.CfnEIPProps{})

	// give a name so we can identify the eip
	eip.Tags().SetTag(
		jsii.String("Name"),
		jsii.Sprintf("%s/KGWElasticIp", *awscdk.Aws_STACK_NAME()),
		jsii.Number(10),
		jsii.Bool(true),
	)

	// Create an A record for the gateway using HostedDomain
	subdomain := "inner-gateway"
	aRecord := input.HostedDomain.AddARecord("GatewayARecord", subdomain,
		awsroute53.RecordTarget_FromIpAddresses(eip.AttrPublicIp()),
	)
	// Full DNS name includes subdomain prefix
	gatewayFqdn := aRecord.DomainName()

	return KGWInstance{
		SecurityGroup:  instanceSG,
		Role:           role,
		GatewayFqdn:    gatewayFqdn,
		LaunchTemplate: launchTemplate,
		ElasticIp:      eip,
	}
}
