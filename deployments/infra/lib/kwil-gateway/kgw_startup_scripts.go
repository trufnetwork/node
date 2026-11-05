package kwil_gateway

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/utils"
	"github.com/trufnetwork/node/infra/scripts/renderer"
)

const (
	kgwDir             = "/home/ec2-user/kgw"
	kgwComposeFile     = kgwDir + "/gateway-compose.yaml"
	kgwComposeProject  = "kgw"
	kgwStartScriptPath = "/usr/local/bin/start-kgw.sh"
)

type AddKwilGatewayStartupScriptsOptions struct {
	KGWDirZipPath *string
	kgwBinaryPath *string
	Config        KGWConfig
	// SessionSecretParameter is the SSM SecureString the start script reads.
	SessionSecretParameter string
	// Region is the AWS region of that parameter.
	Region string
}

func AddKwilGatewayStartupScriptsToInstance(options AddKwilGatewayStartupScriptsOptions) *string {
	config := options.Config

	// Build HTTP backend URLs (host at port 80) for the gateway compose file
	var nodeAddresses []*string
	for _, node := range config.Nodes {
		url := awscdk.Fn_Join(jsii.String(""), &[]*string{
			jsii.String("http://"),
			node.PeerConnection.GetRpcHost(),
		})
		nodeAddresses = append(nodeAddresses, url)
	}

	// SESSION_SECRET is not set here: the start script reads it from SSM.
	kgwEnvConfig := KGWEnvConfig{
		CorsAllowOrigins:   config.CorsAllowOrigins,
		Backends:           awscdk.Fn_Join(jsii.String(","), &nodeAddresses), // nodeAddresses now contains http://host:port
		ChainId:            config.ChainId,
		Domain:             jsii.String("gateway." + *config.Domain),
		XffTrustProxyCount: config.XffTrustProxyCount,
		CookieSameSite:     config.CookieSameSite,
	}

	startScript, err := renderer.Render(renderer.TplKgwStart, renderer.KgwStartData{
		Region:                 options.Region,
		SessionSecretParameter: options.SessionSecretParameter,
		ComposePath:            kgwComposeFile,
		ProjectName:            kgwComposeProject,
	})
	if err != nil {
		panic(fmt.Errorf("render %s: %w", renderer.TplKgwStart, err))
	}

	script := "#!/bin/bash\nset -e\nset -x\n\n"
	installScript, err := utils.InstallDockerScript()
	if err != nil {
		panic(err)
	}
	script += installScript + "\n"

	// script += utils.ConfigureDocker(utils.ConfigureDockerInput{
	// // when we want to enable docker metrics on the host
	// 	MetricsAddr: jsii.String("127.0.0.1:9323"),
	// }) + "\n"

	script += utils.UnzipFileScript(*options.KGWDirZipPath, kgwDir) + "\n"
	script += `
mkdir -p /tmp/kgw-pkg
unzip ` + *options.kgwBinaryPath + ` v0.4.1/kgw_0.4.1_linux_amd64.tar.gz -d /tmp/kgw-pkg
mkdir -p /tmp/kgw-binary
tar -xf /tmp/kgw-pkg/v0.4.1/kgw_0.4.1_linux_amd64.tar.gz  -C /tmp/kgw-binary
chmod +x /tmp/kgw-binary/kgw
mv /tmp/kgw-binary/kgw /home/ec2-user/kgw/kgw
rm -rf /tmp/kgw-pkg /tmp/kgw-binary
` + "\n"
	script += utils.WrapAsFile(startScript, kgwStartScriptPath)
	// Stop by project name: it needs no compose file, so no session secret.
	script += utils.CreateSystemdServiceScript(
		"kgw",
		"Kwil Gateway Compose",
		kgwStartScriptPath,
		"/bin/bash -c \"docker compose -p "+kgwComposeProject+" down\"",
		kgwEnvConfig.GetDict(),
	)

	return jsii.String(script)
}

type KGWEnvConfig struct {
	Domain             *string `env:"DOMAIN"`
	CorsAllowOrigins   *string `env:"CORS_ALLOW_ORIGINS"`
	XffTrustProxyCount *string `env:"XFF_TRUST_PROXY_COUNT"`
	Backends           *string `env:"BACKENDS"`
	ChainId            *string `env:"CHAIN_ID"`
	CookieSameSite     *string `env:"COOKIE_SAME_SITE"`
}

// GetDict returns a map of the environment variables and their values
func (c KGWEnvConfig) GetDict() map[string]string {
	return utils.GetDictFromStruct(c)
}
