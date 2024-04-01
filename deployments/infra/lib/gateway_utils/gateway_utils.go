package gateway_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
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

aws s3 cp s3://kwil-binaries/gateway/kgw-0.1.3.zip /tmp/kgw-0.1.3.zip
unzip /tmp/kgw-0.1.3.zip -d /tmp/
tar -xf /tmp/kgw-0.1.3/kgw_0.1.3_linux_amd64.tar.gz -C /tmp/kgw-0.1.3
chmod +x /tmp/kgw-0.1.3/kgw
# we send the binary as it is expected by the docker-compose file
mv /tmp/kgw-0.1.3/kgw /home/ec2-user/kgw/kgw

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

func InstallCertbotOnInstance(instance awsec2.Instance) {
	certbotInstallScript := `#!/bin/bash
set -e
set -x

amazon-linux-extras install epel -y
yum-config-manager --enable epel*
yum install certbot python-certbot-dns-route53 -y
`

	instance.AddUserData(jsii.String(certbotInstallScript))
}

func AddCertbotDnsValidationToInstance(instance awsec2.Instance, domain *string, hostedZone awsroute53.IHostedZone) {
	// add permissions
	role := instance.Role()

	// see https://johnrix.medium.com/automating-dns-challenge-based-letsencrypt-certificates-with-aws-route-53-8ba799dd207b
	role.AddToPrincipalPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions:   jsii.Strings("route53:ChangeResourceRecordSets"),
		Resources: jsii.Strings(*hostedZone.HostedZoneArn()),
	}))
	role.AddToPrincipalPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions:   jsii.Strings("route53:GetChange", "route53:ListHostedZones"),
		Resources: jsii.Strings("*"),
	}))

	certbotDnsValidationScript := `#!/bin/bash
set -e
set -x

# fetch latest certbot nginx config for security updates
curl -o /etc/letsencrypt/options-ssl-nginx.conf https://raw.githubusercontent.com/certbot/certbot/master/certbot-nginx/certbot_nginx/_internal/tls_configs/options-ssl-nginx.conf

certbot certonly --dns-route53 --register-unsafely-without-email -d ` + *domain +
		` --agree-tos --non-interactive --dns-route53-propagation-seconds 30

# add to crontab
echo "0 12 * * * certbot renew --dns-route53 --register-unsafely-without-email --agree-tos --non-interactive --dns-route53-propagation-seconds 30" | crontab -
`

	instance.AddUserData(jsii.String(certbotDnsValidationScript))
}
