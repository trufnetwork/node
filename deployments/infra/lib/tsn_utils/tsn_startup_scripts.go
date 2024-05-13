package tsn_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/constructs-go/constructs/v10"
)

type AddStartupScriptsOptions struct {
	Instance      awsec2.Instance
	TsnImageAsset awsecrassets.DockerImageAsset
	Region        *string
}

func AddTsnDbStartupScriptsToInstance(scope constructs.Construct, options AddStartupScriptsOptions) {
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
aws ecr get-login-password --region ` + *options.Region + ` | docker login --username AWS --password-stdin ` + *tsnImageAsset.Repository().RepositoryUri() + `
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
