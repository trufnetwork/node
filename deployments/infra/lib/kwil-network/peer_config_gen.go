package kwil_network

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
)

type GeneratePeerConfigInput struct {
	CurrentPeer     peer.TSNPeer
	Peers           []peer.TSNPeer
	GenesisFilePath string
	PrivateKey      *string
}

func GeneratePeerConfig(scope constructs.Construct, input GeneratePeerConfigInput) string {
	// Create a temporary directory for the configuration
	tempDir := awscdk.FileSystem_Mkdtemp(jsii.String("peer-config"))

	// Get environment variables
	envVars := config.GetEnvironmentVariables()

	// Prepare persistent peers string
	var persistentPeers []string
	for _, p := range input.Peers {
		persistentPeers = append(persistentPeers, *p.GetExternalP2PAddress(true))
	}
	persistentPeersStr := strings.Join(persistentPeers, ",")

	// Generate configuration using kwil-admin CLI
	cmd := exec.Command(envVars.KwilAdminBinPath, "setup", "peer",
		"--chain.p2p.external-address", "http://"+*input.CurrentPeer.GetExternalP2PAddress(false),
		"--chain.p2p.persistent-peers", persistentPeersStr,
		"--app.hostname", *input.CurrentPeer.Address,
		"--root-dir", *tempDir,

		"-g", input.GenesisFilePath,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate peer config: %v\nOutput: %s", err, output))
	}

	// replace the private key in the generated configuration
	replacePrivateKeyInConfig(*tempDir, *input.PrivateKey)

	// Return the path of the generated configuration directory
	return *tempDir
}

func replacePrivateKeyInConfig(configDir string, privateKey string) {
	// replace the private key in the generated configuration
	// we know that private key is a plain text file at <dir>/private_key
	privateKeyPath := fmt.Sprintf("%s/private_key", configDir)
	err := os.WriteFile(privateKeyPath, []byte(privateKey), 0644)

	if err != nil {
		panic(fmt.Sprintf("Failed to write private key to file: %v", err))
	}
}
