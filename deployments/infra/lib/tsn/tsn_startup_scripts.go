package tsn

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	peer2 "github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type AddStartupScriptsOptions struct {
	Instance           awsec2.Instance
	currentPeer        peer2.PeerConnection
	allPeers           []peer2.PeerConnection
	TsnImageAsset      awsecrassets.DockerImageAsset
	TsnConfigImagePath *string
	TsnConfigZipPath   *string
	TsnComposePath     *string
	DataDirPath        *string
	Region             *string
}

func AddTsnDbStartupScriptsToInstance(scope constructs.Construct, options AddStartupScriptsOptions) {
	instance := options.Instance

	tsnImageAsset := options.TsnImageAsset

	tsnConfigExtractedPath := *options.DataDirPath + "tsn"
	postgresDataPath := *options.DataDirPath + "postgres"
	tsnConfigRelativeToCompose := "./tsn"

	// create a list of persistent peers
	var persistentPeersList []*string
	for _, peer := range options.allPeers {
		persistentPeersList = append(persistentPeersList, peer.GetP2PAddress(true))
	}

	// create a string from the list
	persistentPeers := awscdk.Fn_Join(jsii.String(","), &persistentPeersList)

	tsnConfig := TSNEnvConfig{
		Hostname:           options.currentPeer.ElasticIp.AttrPublicIp(),
		ConfTarget:         jsii.String("external"),
		ExternalConfigPath: jsii.String(tsnConfigRelativeToCompose),
		PersistentPeers:    persistentPeers,
		ExternalAddress:    jsii.String("http://" + *options.currentPeer.GetP2PAddress(false)),
		TsnVolume:          jsii.String(tsnConfigExtractedPath),
		PostgresVolume:     jsii.String(postgresDataPath),
	}

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

# Add the ec2-user to the docker group (ec2-user is the default user in Amazon Linux 2)
usermod -aG docker ec2-user

# reload the group
newgrp docker

mkdir -p /usr/local/lib/docker/cli-plugins/
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod a+x /usr/local/lib/docker/cli-plugins/docker-compose

# extract the config
unzip ` + *options.TsnConfigZipPath + ` -d ` + tsnConfigExtractedPath + ` 

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
ExecStart=/bin/bash -c "docker compose -f ` + *options.TsnComposePath + ` up -d --wait || true"
ExecStop=/bin/bash -c "docker compose -f ` + *options.TsnComposePath + ` down"
` + utils.GetEnvStringsForService(tsnConfig.GetDict()) + `

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd to recognize the new service, enable it to start on boot, and start the service
systemctl daemon-reload
systemctl enable tsn-db-app.service
systemctl start tsn-db-app.service`

	instance.AddUserData(&script1Content)
}

type TSNEnvConfig struct {
	// the hostname of the instance
	Hostname *string `env:"HOSTNAME"`
	// created= generated on docker build command; external= copied from the host
	ConfTarget *string `env:"CONF_TARGET"`
	// if copied from host, where to copy from
	ExternalConfigPath *string `env:"EXTERNAL_CONFIG_PATH"`
	// comma separated list of peers, used for p2p communication
	PersistentPeers *string `env:"PERSISTENT_PEERS"`
	// comma separated list of peers, used for p2p communication
	ExternalAddress *string `env:"EXTERNAL_ADDRESS"`
	TsnVolume       *string `env:"TSN_VOLUME"`
	PostgresVolume  *string `env:"POSTGRES_VOLUME"`
}

// GetDict returns a map of the environment variables and their values
func (c TSNEnvConfig) GetDict() map[string]string {
	return utils.GetDictFromStruct(c)
}
