package gateway_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/jsii-runtime-go"
)

// Certbot is a tool to automatically obtain and renew SSL certificates.
// Limitations are the rates that Let's Encrypt enforces, such as how many certificates you can get per week for a domain.
// Also the certificate should be handled by us, requiring special measures to enforce their security.
// This imposes some drawbacks instead of using a managed service like AWS ACM.

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
