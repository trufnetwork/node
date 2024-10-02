package tsn

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/jsii-runtime-go"
	peer2 "github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type AddStartupScriptsOptions struct {
	currentPeer        peer2.TSNPeer
	allPeers           []peer2.TSNPeer
	TsnImageAsset      awsecrassets.DockerImageAsset
	TsnConfigImagePath *string
	TsnConfigZipPath   *string
	TsnComposePath     *string
	DataDirPath        *string
	Region             *string
}

func TsnDbStartupScripts(options AddStartupScriptsOptions) *string {
	tsnConfigExtractedPath := *options.DataDirPath + "tsn"
	postgresDataPath := *options.DataDirPath + "postgres"
	tsnConfigRelativeToCompose := "./tsn"

	// create a list of persistent peers
	var persistentPeersList []*string
	for _, peer := range options.allPeers {
		persistentPeersList = append(persistentPeersList, peer.GetExternalP2PAddress(true))
	}

	// create a string from the list
	persistentPeers := awscdk.Fn_Join(jsii.String(","), &persistentPeersList)

	tsnConfig := TSNEnvConfig{
		Hostname:           options.currentPeer.Address,
		ConfTarget:         jsii.String("external"),
		ExternalConfigPath: jsii.String(tsnConfigRelativeToCompose),
		PersistentPeers:    persistentPeers,
		ExternalAddress:    jsii.String("http://" + *options.currentPeer.GetExternalP2PAddress(false)),
		TsnVolume:          jsii.String(tsnConfigExtractedPath),
		PostgresVolume:     jsii.String(postgresDataPath),
	}

	script := utils.InstallDockerScript() + "\n"
	script += utils.ConfigureDocker(utils.ConfigureDockerInput{
		DataRoot: jsii.String(*options.DataDirPath + "/docker"),
		// when we want to enable docker metrics on the host
		// MetricsAddr: jsii.String("127.0.0.1:9323"),
	}) + "\n"
	script += utils.UnzipFileScript(*options.TsnConfigZipPath, tsnConfigExtractedPath) + "\n"
	// Add ECR login and image pulling
	script += `
# Login to ECR
aws ecr get-login-password --region ` + *options.Region + ` | docker login --username AWS --password-stdin ` + *options.TsnImageAsset.Repository().RepositoryUri() + `
# Pull the image
docker pull ` + *options.TsnImageAsset.ImageUri() + `
# Tag the image as tsn-db:local, as the docker-compose file expects that
docker tag ` + *options.TsnImageAsset.ImageUri() + ` tsn-db:local`

	script += utils.CreateSystemdServiceScript(
		"tsn-db-app",
		"TSN Docker Application",
		"/bin/bash -c \"docker compose -f "+*options.TsnComposePath+" up -d --wait || true\"",
		"/bin/bash -c \"docker compose -f "+*options.TsnComposePath+" down\"",
		tsnConfig.GetDict(),
	)

	return &script
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
