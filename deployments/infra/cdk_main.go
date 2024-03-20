package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"os"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"

	"github.com/aws/constructs-go/constructs/v10"
)

type CdkStackProps struct {
	awscdk.StackProps
}

func TsnDBCdkStack(scope constructs.Construct, id string, props *CdkStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)

	awscdk.NewCfnOutput(stack, jsii.String("region"), &awscdk.CfnOutputProps{
		Value: stack.Region(),
	})

	// for some reason this is not working, it's not setting the repo correctly
	//repo := awsecr.NewRepository(stack, jsii.String("ECRRepository"), &awsecr.RepositoryProps{
	//	RepositoryName:     jsii.String(config.EcrRepoName(stack)),
	//	RemovalPolicy:      awscdk.RemovalPolicy_DESTROY,
	//	ImageTagMutability: awsecr.TagMutability_MUTABLE,
	//	ImageScanOnPush:    jsii.Bool(false),
	//	LifecycleRules: &[]*awsecr.LifecycleRule{
	//		{
	//			MaxImageCount: jsii.Number(10),
	//			RulePriority:  jsii.Number(1),
	//		},
	//	},
	//})

	tsnImageAsset := awsecrassets.NewDockerImageAsset(stack, jsii.String("DockerImageAsset"), &awsecrassets.DockerImageAssetProps{
		AssetName: nil,
		BuildArgs: nil,
		CacheFrom: &[]*awsecrassets.DockerCacheOption{
			{
				Type: jsii.String("local"),
				Params: &map[string]*string{
					"src": jsii.String("/tmp/.buildx-cache-tsn-db"),
				},
			},
		},
		CacheTo: &awsecrassets.DockerCacheOption{
			Type: jsii.String("local"),
			Params: &map[string]*string{
				"dest": jsii.String("/tmp/.buildx-cache-tsn-db-new"),
			},
		},
		BuildSecrets: nil,
		File:         jsii.String("deployments/Dockerfile"),
		NetworkMode:  nil,
		Platform:     nil,
		Target:       nil,
		Directory:    jsii.String("../../"),
	})

	pushDataImageAsset := awsecrassets.NewDockerImageAsset(stack, jsii.String("PushDataImageAsset"), &awsecrassets.DockerImageAssetProps{
		AssetName: nil,
		BuildArgs: nil,
		CacheFrom: &[]*awsecrassets.DockerCacheOption{
			{
				Type: jsii.String("local"),
				Params: &map[string]*string{
					"src": jsii.String("/tmp/.buildx-cache-push-data-tsn"),
				},
			},
		},
		CacheTo: &awsecrassets.DockerCacheOption{
			Type: jsii.String("local"),
			Params: &map[string]*string{
				"dest": jsii.String("/tmp/.buildx-cache-push-data-tsn-new"),
			},
		},
		File:      jsii.String("deployments/push-tsn-data.dockerfile"),
		Directory: jsii.String("../../"),
	})

	// Adding our docker compose file to the instance
	dockerComposeAsset := awss3assets.NewAsset(stack, jsii.String("DockerComposeAsset"), &awss3assets.AssetProps{
		Path: jsii.String("../../compose.yaml"),
	})

	initElements := []awsec2.InitElement{
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ec2-user/docker-compose.yaml"), dockerComposeAsset, nil),
	}

	// default vpc
	vpcInstance := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{
		IsDefault: jsii.Bool(true),
	})

	// Create instance using tsnImageAsset hash so that the instance is recreated when the image changes.
	newName := "TsnDBInstance" + *tsnImageAsset.AssetHash()
	instance, instanceRole := createInstance(stack, newName, vpcInstance, &initElements)

	deployImageOnInstance(stack, instance, tsnImageAsset, pushDataImageAsset)

	// make ecr repository available to the instance
	tsnImageAsset.Repository().GrantPull(instanceRole)
	pushDataImageAsset.Repository().GrantPull(instanceRole)
	dockerComposeAsset.GrantRead(instanceRole)

	// Output info.
	awscdk.NewCfnOutput(stack, jsii.String("public-address"), &awscdk.CfnOutputProps{
		Value: instance.InstancePublicIp(),
	})

	return stack
}

func createInstance(stack awscdk.Stack, name string, vpc awsec2.IVpc, initElements *[]awsec2.InitElement) (awsec2.Instance, awsiam.IRole) {
	// Create security group.
	instanceSG := awsec2.NewSecurityGroup(stack, jsii.String("NodeSG"), &awsec2.SecurityGroupProps{
		Vpc:              vpc,
		AllowAllOutbound: jsii.Bool(true),
		Description:      jsii.String("TSN-DB instance security group."),
	})

	// TODO: add 8080 support when it's gateway protected
	//instanceSG.AddIngressRule(
	//	awsec2.Peer_AnyIpv4(),
	//	awsec2.NewPort(&awsec2.PortProps{
	//		Protocol:             awsec2.Protocol_TCP,
	//		FromPort:             jsii.Number(8080),
	//		ToPort:               jsii.Number(8080),
	//		StringRepresentation: jsii.String("Allow requests to common app range."),
	//	}),
	//	jsii.String("Allow requests to common app range."),
	//	jsii.Bool(false))

	// ssh
	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.NewPort(&awsec2.PortProps{
			Protocol:             awsec2.Protocol_TCP,
			FromPort:             jsii.Number(22),
			ToPort:               jsii.Number(22),
			StringRepresentation: jsii.String("Allow ssh."),
		}),
		jsii.String("Allow ssh."),
		jsii.Bool(false))

	// Creating in private subnet only when deployment cluster in PROD stage.
	subnetType := awsec2.SubnetType_PUBLIC
	if config.DeploymentStage(stack) == config.DeploymentStage_PROD {
		subnetType = awsec2.SubnetType_PRIVATE_WITH_NAT
	}

	// Get key-pair pointer.
	var keyPair *string = nil
	if len(config.KeyPairName(stack)) > 0 {
		keyPair = jsii.String(config.KeyPairName(stack))
	}

	// Create instance role.
	instanceRole := awsiam.NewRole(stack, jsii.String("InstanceRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	initData := awsec2.CloudFormationInit_FromElements(
		*initElements...,
	)

	// comes with pre-installed cloud init requirements
	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2(nil)
	instance := awsec2.NewInstance(stack, jsii.String(name), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_SMALL),
		Init:         initData,
		MachineImage: AWSLinux2MachineImage,
		Vpc:          vpc,
		VpcSubnets: &awsec2.SubnetSelection{
			SubnetType: subnetType,
		},
		SecurityGroup: instanceSG,
		Role:          instanceRole,
		KeyPair:       awsec2.KeyPair_FromKeyPairName(stack, jsii.String("KeyPair"), keyPair),
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
	eip := awsec2.NewCfnEIP(stack, jsii.String("EIP"), nil)
	awsec2.NewCfnEIPAssociation(stack, jsii.String("EIPAssociation"), &awsec2.CfnEIPAssociationProps{
		InstanceId:   instance.InstanceId(),
		AllocationId: eip.AttrAllocationId(),
	})

	return instance, instanceRole
}

func deployImageOnInstance(stack awscdk.Stack, instance awsec2.Instance, tsnImageAsset awsecrassets.DockerImageAsset, pushDataImageAsset awsecrassets.DockerImageAsset) {

	// we could improve this script by adding a ResourceSignal, which would signalize to CDK that the instance is ready
	// and fail the deployment otherwise

	// create a script from the asset
	script1Content := `#!/bin/bash
set -e
set -x

# Update the system
yum update -y

# Install Docker
amazon-linux-extras install docker

# Start Docker and enable it to start at boot
systemctl start docker
systemctl enable docker

mkdir -p /usr/local/lib/docker/cli-plugins/
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod a+x /usr/local/lib/docker/cli-plugins/docker-compose

# Add the ec2-user to the docker group (ec2-user is the default user in Amazon Linux 2)
usermod -aG docker ec2-user

# reload the group
newgrp docker

# Install the AWS CLI
yum install -y aws-cli

# Login to ECR
aws ecr get-login-password --region ` + *stack.Region() + ` | docker login --username AWS --password-stdin ` + *tsnImageAsset.Repository().RepositoryUri() + `
# Pull the image
docker pull ` + *tsnImageAsset.ImageUri() + `
# Tag the image as tsn-db:local, as the docker-compose file expects that
docker tag ` + *tsnImageAsset.ImageUri() + ` tsn-db:local

# Login to ECR again for the second repository
aws ecr get-login-password --region ` + *stack.Region() + ` | docker login --username AWS --password-stdin ` + *pushDataImageAsset.Repository().RepositoryUri() + `
# Pull the image
docker pull ` + *pushDataImageAsset.ImageUri() + `
# Tag the image as push-tsn-data:local, as the docker-compose file expects that
docker tag ` + *pushDataImageAsset.ImageUri() + ` push-tsn-data:local

# Create a systemd service file
cat <<EOF > /etc/systemd/system/tsn-db-app.service
[Unit]
Description=My Docker Application
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
# This path comes from the init asset
ExecStart=/bin/bash -c "docker compose -f /home/ec2-user/docker-compose.yaml up -d"
ExecStop=/bin/bash -c "docker compose -f /home/ec2-user/docker-compose.yaml down"
` + getEnvStringsForService(getEnvVars("WHITELIST_WALLETS", "PRIVATE_KEY")) + `

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd to recognize the new service, enable it to start on boot, and start the service
systemctl daemon-reload
systemctl enable tsn-db-app.service
systemctl start tsn-db-app.service`

	instance.AddUserData(&script1Content)
}

// Warning: Used environment variables are not encrypted in the CloudFormation template,
// nor to who have access to the instance if it used on a service configuration file.
// Switch for encryption if necessary.
func getEnvStringsForService(envDict map[string]string) string {
	envStrings := ""
	for k, v := range envDict {
		envStrings += "Environment=\"" + k + "=" + v + "\"\n"
	}
	return envStrings
}

// getEnvVars returns a map of environment variables from the given list of
// environment variable names. If an environment variable is not set, it will
// be an empty string in the map.
func getEnvVars(envNames ...string) map[string]string {
	envDict := make(map[string]string)
	for _, envName := range envNames {
		envDict[envName] = os.Getenv(envName)
	}
	return envDict
}

func main() {
	app := awscdk.NewApp(nil)

	TsnDBCdkStack(app, config.StackName(app), &CdkStackProps{
		awscdk.StackProps{
			Env: env(),
		},
	})

	app.Synth(nil)
}

// env determines the AWS environment (account+region) in which our stack is to
// be deployed. For more information see: https://docs.aws.amazon.com/cdk/latest/guide/environments.html
func env() *awscdk.Environment {
	account := os.Getenv("CDK_DEPLOY_ACCOUNT")
	region := os.Getenv("CDK_DEPLOY_REGION")

	if len(account) == 0 || len(region) == 0 {
		account = os.Getenv("CDK_DEFAULT_ACCOUNT")
		region = os.Getenv("CDK_DEFAULT_REGION")
	}

	return &awscdk.Environment{
		Account: jsii.String(account),
		Region:  jsii.String(region),
	}
}
