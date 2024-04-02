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
set -e
set -x 

# Extract the gateway files
unzip /home/ec2-user/kgw.zip -d /home/ec2-user/kgw

aws s3 cp s3://kwil-binaries/gateway/kgw-v0.2.0.zip /tmp/kgw-v0.2.0.zip
unzip /tmp/kgw-v0.2.0.zip -d /tmp/
tar -xf /tmp/kgw-v0.2.0/kgw_0.2.0_linux_amd64.tar.gz -C /tmp/kgw-v0.2.0
chmod +x /tmp/kgw-v0.2.0/kgw
# we send the binary as it is expected by the docker-compose file
mv /tmp/kgw-v0.2.0/kgw /home/ec2-user/kgw/kgw

cat <<EOF > /etc/systemd/system/kgw.service
[Unit]
Description=Kwil Gateway Compose
# must come after tsn-db service, as the network is created by the tsn-db service
After=tsn-db-app.service
Requires=tsn-db-app.service
Restart=on-failure

[Service]
type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -c "docker compose -f /home/ec2-user/kgw/gateway-compose.yaml up -d --wait || true"
ExecStop=/bin/bash -c "docker compose -f /home/ec2-user/kgw/gateway-compose.yaml down"
Environment="DOMAIN=` + *domain + `"
` + utils.GetEnvStringsForService(utils.GetEnvVars("SESSION_SECRET", "CORS_ALLOWED_ORIGINS")) + `


[Install]
WantedBy=multi-user.target

EOF

systemctl daemon-reload
systemctl enable kgw.service
systemctl start kgw.service
`

	instance.AddUserData(jsii.String(kgwSetupScript))
}

type AddKwilGatewayStartupScriptsOptions struct {
	Instance awsec2.Instance
	Domain   *string
}
