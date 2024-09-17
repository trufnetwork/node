package kwil_network

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

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
	envVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	validateGenesisFile(input.GenesisFilePath)

	// Generate configuration using kwil-admin CLI
	cmd := exec.Command(envVars.KwilAdminBinPath, "setup", "peer",
		"--chain.p2p.external-address", "should-be-overwritten-by-env",
		"--chain.p2p.persistent-peers", "should-be-overwritten-by-env",
		"--app.hostname", "should-be-overwritten-by-env",
		"--app.snapshots.enabled",
		"--app.snapshots.snapshot-dir", "/root/.kwild/snapshots",
		"--root-dir", *tempDir,
		"-g", input.GenesisFilePath,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate peer config: %v\nOutput: %s", err, output))
	}

	// replace the private key in the generated configuration
	replacePrivateKeyInConfig(*tempDir, *input.PrivateKey)

	// try finding TOKEN in the generated configuration, to be sure we're not using any token strings
	searchTokenCmd := exec.Command("grep", "-r", "\\${Token", *tempDir)
	output, err = searchTokenCmd.CombinedOutput()
	if err == nil {
		panic(fmt.Sprintf("Found TOKEN in generated configuration: %s", output))
	}

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

// validateGenesisFile checks
// - the genesis file exists
// - it's a valid json file
// - it has a "validators" key
func validateGenesisFile(genesisFilePath string) {
	// Read the genesis file
	genesisFileContent, err := os.ReadFile(genesisFilePath)
	if err != nil {
		panic(fmt.Sprintf("Failed to read genesis file at %s: %v", genesisFilePath, err))
	}

	// Check if it's a valid json file
	var genesis map[string]interface{}
	err = json.Unmarshal(genesisFileContent, &genesis)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal genesis file at %s: %v", genesisFilePath, err))
	}

	// Check if it has a "validators" key
	if _, ok := genesis["validators"]; !ok {
		panic(fmt.Sprintf("Genesis file doesn't have a 'validators' key: %s, %v", genesisFilePath, genesis))
	}
}
