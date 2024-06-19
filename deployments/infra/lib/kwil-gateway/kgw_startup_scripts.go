package kwil_gateway

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type AddKwilGatewayStartupScriptsOptions struct {
	Instance      awsec2.Instance
	kgwBinaryPath *string
	Config        KGWConfig
}

func AddKwilGatewayStartupScriptsToInstance(options AddKwilGatewayStartupScriptsOptions) {
	instance := options.Instance
	config := options.Config

	var nodeAddresses []*string
	for _, node := range config.Nodes {
		nodeAddresses = append(nodeAddresses, node.PeerConnection.GetHttpAddress())
	}

	// Create the environment variables for the gateway compose file
	kgwEnvConfig := KGWEnvConfig{
		CorsAllowOrigins: config.CorsAllowOrigins,
		SessionSecret:    config.SessionSecret,
		Backends:         awscdk.Fn_Join(jsii.String(","), &nodeAddresses),
		ChainId:          config.ChainId,
		Domain:           config.Domain,
	}

	kgwSetupScript := `#!/bin/bash
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

# Extract the gateway files
unzip /home/ec2-user/kgw.zip -d /home/ec2-user/kgw

unzip ` + *options.kgwBinaryPath + ` -d /tmp/kgw-pkg
mkdir -p /tmp/kgw-binary
tar -xf /tmp/kgw-pkg/kgw_0.3.0-next_linux_amd64.tar.gz -C /tmp/kgw-binary
chmod +x /tmp/kgw-binary/kgw
# we send the binary as it is expected by the docker-compose file
mv /tmp/kgw-binary/kgw /home/ec2-user/kgw/kgw

cat <<EOF > /etc/systemd/system/kgw.service
[Unit]
Description=Kwil Gateway Compose
Restart=on-failure

[Service]
type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -c "docker compose -f /home/ec2-user/kgw/gateway-compose.yaml up -d --wait || true"
ExecStop=/bin/bash -c "docker compose -f /home/ec2-user/kgw/gateway-compose.yaml down"
` + utils.GetEnvStringsForService(kgwEnvConfig.GetDict()) + `


[Install]
WantedBy=multi-user.target

EOF

systemctl daemon-reload
systemctl enable kgw.service
systemctl start kgw.service
`

	instance.AddUserData(jsii.String(kgwSetupScript))
}

type KGWEnvConfig struct {
	Domain           *string `env:"DOMAIN"`
	CorsAllowOrigins *string `env:"CORS_ALLOWED_ORIGINS"`
	SessionSecret    *string `env:"SESSION_SECRET"`
	Backends         *string `env:"BACKENDS"`
	ChainId          *string `env:"CHAIN_ID"`
}

// GetDict returns a map of the environment variables and their values
func (c KGWEnvConfig) GetDict() map[string]string {
	return utils.GetDictFromStruct(c)
}
