package tn

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/jsii-runtime-go"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/utils"
)

const kwildConfigFilename = "config.toml" // Needs to match node.go

type AddStartupScriptsOptions struct {
	currentPeer       peer2.TNPeer
	allPeers          []peer2.TNPeer
	TnImageAsset      awsecrassets.DockerImageAsset
	TnConfigImagePath *string
	TnComposePath     *string
	DataDirPath       *string
	Region            *string
}

func TnDbStartupScripts(options AddStartupScriptsOptions) *string {
	// Define paths
	tnDataPath := *options.DataDirPath + "tn"             // e.g., /data/tn - This is the host path mounted as /root/.kwild in container
	postgresDataPath := *options.DataDirPath + "postgres" // e.g., /data/postgres

	// Environment variables needed ONLY by docker-compose/startup script
	tnConfig := TNEnvConfig{
		Hostname:       options.currentPeer.Address,
		TnVolume:       jsii.String(tnDataPath),
		PostgresVolume: jsii.String(postgresDataPath),
	}

	// Start building the script
	script := utils.InstallDockerScript() + "\n"
	script += utils.ConfigureDocker(utils.ConfigureDockerInput{
		DataRoot: jsii.String(*options.DataDirPath + "/docker"),
	}) + "\n"

	// Create necessary directories on host (ensure permissions later if needed)
	// cfn-init should have already placed config.toml and genesis.json into /data/tn
	script += fmt.Sprintf("mkdir -p %s\n", tnDataPath)
	script += fmt.Sprintf("mkdir -p %s\n", postgresDataPath)

	// NOTE: config.toml and genesis.json are now placed by cfn-init via InitFile_FromAsset
	// into /data/tn/config.toml and /data/tn/genesis.json respectively.
	// Ensure the ownership/permissions set in InitFile_FromAsset are correct
	// for the user running the docker container (likely root).

	// ECR Login and Image Pulling
	script += fmt.Sprintf(`
# Login to ECR
aws ecr get-login-password --region %s | docker login --username AWS --password-stdin %s
# Pull the image
docker pull %s
# Tag the image as tn-db:local, as the docker-compose file expects that
docker tag %s tn-db:local
`, *options.Region, *options.TnImageAsset.Repository().RepositoryUri(), *options.TnImageAsset.ImageUri(), *options.TnImageAsset.ImageUri())

	// Create and start systemd service for Docker Compose
	script += utils.CreateSystemdServiceScript(
		"tn-db-app",
		"TN Docker Application",
		fmt.Sprintf("/bin/bash -c \"docker compose -f %s up -d --wait || true\"", *options.TnComposePath),
		fmt.Sprintf("/bin/bash -c \"docker compose -f %s down\"", *options.TnComposePath),
		tnConfig.GetDict(), // Pass reduced env vars
	)

	return &script
}

type TNEnvConfig struct {
	Hostname       *string `env:"HOSTNAME"`
	TnVolume       *string `env:"TN_VOLUME"`       // Host path mapped to /root/.kwild
	PostgresVolume *string `env:"POSTGRES_VOLUME"` // Host path for postgres data
}

// GetDict returns a map of the environment variables and their values
func (c TNEnvConfig) GetDict() map[string]string {
	return utils.GetDictFromStruct(c)
}
