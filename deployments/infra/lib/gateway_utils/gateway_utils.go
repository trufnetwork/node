package gateway_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

func AddKwilGatewayStartupScriptsToInstance(options AddKwilGatewayStartupScriptsOptions) {
	instance := options.Instance
	domain := options.Domain

	kgwSetupScript := `#!/bin/bash
aws s3 cp s3://kwil-binaries/gateway/kgw_v0.1.2.zip /tmp/kgw_v0.1.2.zip
unzip /tmp/kgw_v0.1.2.zip -d /tmp/
tar -xf /tmp/kgw_v0.1.2/kgw_0.1.2_linux_amd64.tar.gz -C /tmp/kgw_v0.1.2
chmod +x /tmp/kgw_v0.1.2/kgw
# we send the binary as it is expected by the docker-compose file
mv /tmp/kgw_v0.1.2/kgw /home/ec2-user/kgw/kgw

# Install the AWS Nitro Enclaves CLI, to be able to use the ACM agent
# for certificate management with nginx
sudo amazon-linux-extras enable aws-nitro-enclaves-cli
sudo yum install aws-nitro-enclaves-acm -y

cat <<EOF > /etc/systemd/system/kgw.service
[Unit]
Description=Kwil Gateway Compose
# must come after tsn-db service, as the network is created by the tsn-db service
After=tsn-db-app.service
Requires=tsn-db-app.service\
Restart=on-failure

[Service]
type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -c "docker compose -f /home/ec2-user/kgw/gateway-compose.yaml up -d --wait"
ExecStop=/bin/bash -c "docker compose -f /home/ec2-user/kgw/gateway-compose.yaml down"
Environment="DOMAIN=https://` + *domain + `"
` + utils.GetEnvStringsForService(utils.GetEnvVars("SESSION_SECRET", "CORS_ALLOWED_ORIGINS")) + `


[Install]
WantedBy=multi-user.target

EOF

systemctl daemon-reload
systemctl enable kgw.service
systemctl start kgw.service

# Start the ACM agent
systemctl enable nitro-enclaves-acm.service
systemctl start nitro-enclaves-acm.service
`

	instance.AddUserData(jsii.String(kgwSetupScript))
}

type AddKwilGatewayStartupScriptsOptions struct {
	Instance awsec2.Instance
	Domain   *string
}
