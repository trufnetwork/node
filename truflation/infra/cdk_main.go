package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecr"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
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

	imageAsset := awsecrassets.NewDockerImageAsset(stack, jsii.String("DockerImageAsset"), &awsecrassets.DockerImageAssetProps{
		AssetName:    nil,
		BuildArgs:    nil,
		BuildSecrets: nil,
		File:         jsii.String("truflation/docker/tsn.dockerfile"),
		NetworkMode:  nil,
		Platform:     nil,
		Target:       nil,
		Directory:    jsii.String("../../"),
	})
	repo := imageAsset.Repository()

	// default vpc
	vpcInstance := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{
		IsDefault: jsii.Bool(true),
	})

	// Create instance using imageAsset hash so that the instance is recreated when the image changes.
	newName := "TsnDBInstance" + *imageAsset.AssetHash()
	instance, instanceRole := createInstance(stack, newName, vpcInstance)

	deployImageOnInstance(stack, instance, imageAsset, repo)

	// make ecr repository available to the instance
	repo.GrantPull(instanceRole)

	// Output info.
	awscdk.NewCfnOutput(stack, jsii.String("public-address"), &awscdk.CfnOutputProps{
		Value: instance.InstancePublicIp(),
	})

	return stack
}

func createInstance(stack awscdk.Stack, name string, vpc awsec2.IVpc) (awsec2.Instance, awsiam.IRole) {
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

	instance := awsec2.NewInstance(stack, jsii.String(name), &awsec2.InstanceProps{
		InstanceType: awsec2.InstanceType_Of(awsec2.InstanceClass_T3, awsec2.InstanceSize_SMALL),
		// ubuntu 22.04
		// https://cloud-images.ubuntu.com/locator/ec2/
		MachineImage: awsec2.MachineImage_FromSsmParameter(jsii.String("/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id"), nil),
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

func deployImageOnInstance(stack awscdk.Stack, instance awsec2.Instance, imageAsset awsecrassets.DockerImageAsset, repo awsecr.IRepository) {
	// create a script from the asset
	script1Content := `#!/bin/bash
set -e
set -x

# lets add repo, region and image uri to /tmp/init-vars just for easier debug
echo "repo=` + *repo.RepositoryUri() + `" > /tmp/init-vars
echo "region=` + *stack.Region() + `" >> /tmp/init-vars
echo "imageUri=` + *imageAsset.ImageUri() + `" >> /tmp/init-vars

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
aws ecr get-login-password --region ` + *stack.Region() + ` | docker login --username AWS --password-stdin ` + *repo.RepositoryUri() + `
# pull the image
docker pull ` + *imageAsset.ImageUri() + `

# create a systemd service file
cat <<EOF > /etc/systemd/system/tsn-db-app.service
[Unit]
Description=My Docker Application
Requires=docker.service
After=docker.service

[Service]
Restart=always
ExecStart=/usr/bin/docker run --network host --name tsn-db-app ` + *imageAsset.ImageUri() + `
ExecStop=/usr/bin/docker stop tsn-db-app
ExecStopPost=/usr/bin/docker rm tsn-db-app

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
