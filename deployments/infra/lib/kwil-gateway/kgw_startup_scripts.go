package kwil_gateway

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type AddKwilGatewayStartupScriptsOptions struct {
	KGWDirZipPath *string
	kgwBinaryPath *string
	Config        KGWConfig
}

func AddKwilGatewayStartupScriptsToInstance(options AddKwilGatewayStartupScriptsOptions) *string {
	config := options.Config

	var nodeAddresses []*string
	for _, node := range config.Nodes {
		nodeAddresses = append(nodeAddresses, node.PeerConnection.GetRpcHost())
	}

	// Create the environment variables for the gateway compose file
	kgwEnvConfig := KGWEnvConfig{
		CorsAllowOrigins: config.CorsAllowOrigins,
		SessionSecret:    config.SessionSecret,
		Backends:         awscdk.Fn_Join(jsii.String(","), &nodeAddresses),
		ChainId:          config.ChainId,
		Domain:           config.Domain,
	}

	script := "#!/bin/bash\nset -e\nset -x\n\n"
	script += utils.InstallDockerScript() + "\n"
	// script += utils.ConfigureDocker(utils.ConfigureDockerInput{
	// // when we want to enable docker metrics on the host
	// 	MetricsAddr: jsii.String("127.0.0.1:9323"),
	// }) + "\n"
	script += utils.UnzipFileScript(*options.KGWDirZipPath, "/home/ec2-user/kgw") + "\n"
	script += `
unzip ` + *options.kgwBinaryPath + ` kgw_0.3.4_linux_amd64.tar.gz -d /tmp/kgw-pkg
mkdir -p /tmp/kgw-binary
tar -xf /tmp/kgw-pkg/kgw_0.3.4_linux_amd64.tar.gz  -C /tmp/kgw-binary
chmod +x /tmp/kgw-binary/kgw
mv /tmp/kgw-binary/kgw /home/ec2-user/kgw/kgw
` + "\n"
	script += utils.CreateSystemdServiceScript(
		"kgw",
		"Kwil Gateway Compose",
		"/bin/bash -c \"docker compose -f /home/ec2-user/kgw/gateway-compose.yaml up -d --wait || true\"",
		"/bin/bash -c \"docker compose -f /home/ec2-user/kgw/gateway-compose.yaml down\"",
		kgwEnvConfig.GetDict(),
	)

	return jsii.String(script)
}

type KGWEnvConfig struct {
	Domain           *string `env:"DOMAIN"`
	CorsAllowOrigins *string `env:"CORS_ALLOW_ORIGINS"`
	SessionSecret    *string `env:"SESSION_SECRET"`
	Backends         *string `env:"BACKENDS"`
	ChainId          *string `env:"CHAIN_ID"`
}

// GetDict returns a map of the environment variables and their values
func (c KGWEnvConfig) GetDict() map[string]string {
	return utils.GetDictFromStruct(c)
}
