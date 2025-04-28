package tn

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/jsii-runtime-go"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type AddStartupScriptsOptions struct {
	currentPeer       peer2.TNPeer
	allPeers          []peer2.TNPeer
	TnImageAsset      awsecrassets.DockerImageAsset
	TnConfigImagePath *string
	TnConfigZipPath   *string
	TnComposePath     *string
	DataDirPath       *string
	Region            *string
}

func TnDbStartupScripts(options AddStartupScriptsOptions) *string {
	tnConfigExtractedPath := *options.DataDirPath + "tn"
	postgresDataPath := *options.DataDirPath + "postgres"
	tnConfigRelativeToCompose := "./tn"

	// create a list of persistent peers
	var persistentPeersList []*string
	for _, peer := range options.allPeers {
		persistentPeersList = append(persistentPeersList, peer.GetExternalP2PAddress(true))
	}

	// create a string from the list
	persistentPeers := awscdk.Fn_Join(jsii.String(","), &persistentPeersList)

	tnConfig := TNEnvConfig{
		Hostname:           options.currentPeer.Address,
		ConfTarget:         jsii.String("external"),
		ExternalConfigPath: jsii.String(tnConfigRelativeToCompose),
		PersistentPeers:    persistentPeers,
		ExternalAddress:    jsii.String("http://" + *options.currentPeer.GetExternalP2PAddress(false)),
		TnVolume:           jsii.String(tnConfigExtractedPath),
		PostgresVolume:     jsii.String(postgresDataPath),
	}

	script := utils.InstallDockerScript() + "\n"
	script += utils.ConfigureDocker(utils.ConfigureDockerInput{
		DataRoot: jsii.String(*options.DataDirPath + "/docker"),
		// when we want to enable docker metrics on the host
		// MetricsAddr: jsii.String("127.0.0.1:9323"),
	}) + "\n"
	script += utils.UnzipFileScript(*options.TnConfigZipPath, tnConfigExtractedPath) + "\n"
	// Add ECR login and image pulling
	script += `
# Login to ECR
aws ecr get-login-password --region ` + *options.Region + ` | docker login --username AWS --password-stdin ` + *options.TnImageAsset.Repository().RepositoryUri() + `
# Pull the image
docker pull ` + *options.TnImageAsset.ImageUri() + `
# Tag the image as tn-db:local, as the docker-compose file expects that
docker tag ` + *options.TnImageAsset.ImageUri() + ` tn-db:local`

	script += utils.CreateSystemdServiceScript(
		"tn-db-app",
		"TN Docker Application",
		"/bin/bash -c \"docker compose -f "+*options.TnComposePath+" up -d --wait || true\"",
		"/bin/bash -c \"docker compose -f "+*options.TnComposePath+" down\"",
		tnConfig.GetDict(),
	)

	return &script
}

type TNEnvConfig struct {
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
	TnVolume        *string `env:"TN_VOLUME"`
	PostgresVolume  *string `env:"POSTGRES_VOLUME"`
}

// GetDict returns a map of the environment variables and their values
func (c TNEnvConfig) GetDict() map[string]string {
	return utils.GetDictFromStruct(c)
}
