package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"infra/config"
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
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ubuntu/docker-compose.yaml"), dockerComposeAsset, nil), // default vpc
	}

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

	instance := awsec2.NewInstance(stack, jsii.String(name), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_SMALL),
		Init:         initData,
		// ubuntu 22.04
		// https://cloud-images.ubuntu.com/locator/ec2/
		MachineImage: awsec2.MachineImage_FromSsmParameter(jsii.String("/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id"), nil),
		Vpc:          vpc,
		VpcSubnets: &awsec2.SubnetSelection{
			SubnetType: subnetType,
		},
		SecurityGroup: instanceSG,
		Role:          instanceRole,
		// takes a long time to deploy, so we raise the timeout
		InitOptions: &awsec2.ApplyCloudFormationInitOptions{
			Timeout: awscdk.Duration_Minutes(jsii.Number(10)),
		},
		KeyPair: awsec2.KeyPair_FromKeyPairName(stack, jsii.String("KeyPair"), keyPair),
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

	// create a script from the asset
	script1Content := `#!/bin/bash
set -e
set -x

# install docker
apt-get update
apt-get install -y docker.io

# add the ubuntu user to the docker group
usermod -aG docker ubuntu
# flush changes
newgrp docker

# install aws cli
apt-get install -y awscli

# login to ecr
aws ecr get-login-password --region ` + *stack.Region() + ` | docker login --username AWS --password-stdin ` + *tsnImageAsset.Repository().RepositoryUri() + `
# pull the image
docker pull ` + *tsnImageAsset.ImageUri() + `
# tag the image as tsn-db:local, as the docker compose file expects that
docker tag ` + *tsnImageAsset.ImageUri() + ` tsn-db:local

# login to ecr
aws ecr get-login-password --region ` + *stack.Region() + ` | docker login --username AWS --password-stdin ` + *pushDataImageAsset.Repository().RepositoryUri() + `
# pull the image
docker pull ` + *pushDataImageAsset.ImageUri() + `
# tag the image as push-tsn-data:local, as the docker compose file expects that
docker tag ` + *pushDataImageAsset.ImageUri() + ` push-tsn-data:local

# create a systemd service file
cat <<EOF > /etc/systemd/system/tsn-db-app.service
[Unit]
Description=My Docker Application
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
# this path comes from the init asset
ExecStart=/bin/bash -c "docker compose -f /home/ubuntu/docker-compose.yaml up -d" 
ExecStop=/bin/bash -c "docker compose -f /home/ubuntu/docker-compose.yaml down"


[Install]
WantedBy=multi-user.target
EOF

# reload systemd to recognize the new service, enable it to start on boot, and start the service
systemctl daemon-reload
systemctl enable tsn-db-app.service
systemctl start tsn-db-app.service
`

	instance.AddUserData(&script1Content)
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
