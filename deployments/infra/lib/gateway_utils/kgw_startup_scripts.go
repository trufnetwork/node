package gateway_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/utils"
	"reflect"
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
		nodeAddresses = append(nodeAddresses, node.Instance.InstancePublicIp())
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

# Extract the gateway files
unzip /home/ec2-user/kgw.zip -d /home/ec2-user/kgw

unzip ` + *options.kgwBinaryPath + ` -d /tmp/kgw-pkg
mkdir -p /tmp/kgw-binary
tar -xf /tmp/kgw-pkg/kgw-v0.2.0/kgw_0.2.0_linux_amd64.tar.gz -C /tmp/kgw-binary
chmod +x /tmp/kgw-binary/kgw
# we send the binary as it is expected by the docker-compose file
mv /tmp/kgw-binary/kgw /home/ec2-user/kgw/kgw

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
// uses reflect
func (c KGWEnvConfig) GetDict() map[string]string {
	envVars := make(map[string]string)
	v := reflect.ValueOf(c)
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() == reflect.Ptr {
			if !field.IsNil() {
				envVars[t.Field(i).Tag.Get("env")] = field.Elem().String()
			}
		}
	}
	return envVars
}
