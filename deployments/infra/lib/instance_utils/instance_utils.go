package instance_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
)

func CreateInstance(stack awscdk.Stack, instanceRole awsiam.IRole, name string, vpc awsec2.IVpc, initElements *[]awsec2.InitElement) awsec2.Instance {
	// Create security group.
	instanceSG := awsec2.NewSecurityGroup(stack, jsii.String("NodeSG"), &awsec2.SecurityGroupProps{
		Vpc:              vpc,
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

	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(443)),
		jsii.String("Allow requests to https."),
		jsii.Bool(false))

	// ssh
	instanceSG.AddIngressRule(
		awsec2.Peer_AnyIpv4(),
		awsec2.Port_Tcp(jsii.Number(22)),
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

	return instance
}

type AddStartupScriptsOptions struct {
	Stack         awscdk.Stack
	Instance      awsec2.Instance
	TsnImageAsset awsecrassets.DockerImageAsset
}

func AddTsnDbStartupScriptsToInstance(options AddStartupScriptsOptions) {
	stack := options.Stack
	instance := options.Instance
	tsnImageAsset := options.TsnImageAsset

	// we could improve this script by adding a ResourceSignal, which would signalize to CDK that the Instance is ready
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
ExecStart=/bin/bash -c "docker compose -f /home/ec2-user/docker-compose.yaml up -d --wait || true"
ExecStop=/bin/bash -c "docker compose -f /home/ec2-user/docker-compose.yaml down"

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd to recognize the new service, enable it to start on boot, and start the service
systemctl daemon-reload
systemctl enable tsn-db-app.service
systemctl start tsn-db-app.service`

	instance.AddUserData(&script1Content)
}
