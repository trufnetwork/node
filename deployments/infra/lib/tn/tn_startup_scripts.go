package tn

import (
	"fmt"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/jsii-runtime-go"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/utils"
	"github.com/trufnetwork/node/infra/scripts/renderer"
	"sort"
	"strconv"
)

type AddStartupScriptsOptions struct {
	CurrentPeer       peer2.TNPeer
	AllPeers          []peer2.TNPeer
	TnImageAsset      awsecrassets.DockerImageAsset
	TnConfigImagePath *string
	TnComposePath     *string
	DataDirPath       *string
	Region            *string
}

// TNEnvConfig holds environment variables specific to the TN DB setup.
type TNEnvConfig struct {
	Hostname       *string `env:"HOSTNAME"`
	TnVolume       *string `env:"TN_VOLUME"`       // Host path mapped to /root/.kwild
	PostgresVolume *string `env:"POSTGRES_VOLUME"` // Host path for postgres data
	RpcPort        *string `env:"TN_RPC_PORT"`     // Port for the TN RPC
}

// GetDict returns a map of the environment variables and their values
func (c TNEnvConfig) GetDict() map[string]string {
	return utils.GetDictFromStruct(c)
}

func TnDbStartupScripts(options AddStartupScriptsOptions) (*string, error) {
	// Define paths
	tnDataPath := *options.DataDirPath + "tn"
	postgresDataPath := *options.DataDirPath + "postgres"

	// Environment variables needed for the systemd service within the template
	tnEnvMap := TNEnvConfig{
		Hostname:       options.CurrentPeer.Address,
		TnVolume:       jsii.String(tnDataPath),
		PostgresVolume: jsii.String(postgresDataPath),
		RpcPort:        jsii.String(strconv.Itoa(peer2.TnRPCPort)),
	}.GetDict()

	// Extract and sort keys from the environment map manually
	sortedKeys := make([]string, 0, len(tnEnvMap))
	for k := range tnEnvMap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Start building the script with Docker setup
	installScript, err := utils.InstallDockerScript()
	if err != nil {
		return nil, fmt.Errorf("getting install docker script: %w", err)
	}
	configureScript, err := utils.ConfigureDocker(utils.ConfigureDockerInput{
		DataRoot: jsii.String(*options.DataDirPath + "docker"),
	})
	if err != nil {
		return nil, fmt.Errorf("getting configure docker script: %w", err)
	}
	script := installScript + "\n" + configureScript + "\n"

	// Prepare data for the main template using the DTO from renderer package
	tplData := renderer.TnStartupData{
		Region:           *options.Region,
		RepoURI:          *options.TnImageAsset.Repository().RepositoryUri(),
		ImageURI:         *options.TnImageAsset.ImageUri(),
		ComposePath:      *options.TnComposePath,
		TnDataPath:       tnDataPath,
		PostgresDataPath: postgresDataPath,
		EnvVars:          tnEnvMap,
		SortedEnvKeys:    sortedKeys,
	}

	// Render the main body template
	body, err := renderer.Render(renderer.TplTnDBStartup, tplData)
	if err != nil {
		return nil, fmt.Errorf("render %s: %w", renderer.TplTnDBStartup, err)
	}

	script += body
	return &script, nil
}
