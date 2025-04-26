package kwil_network

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
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
	tempDir = jsii.String(*tempDir + "/config")

	// Get environment variables
	envVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	validateGenesisFile(input.GenesisFilePath)

	// Initialize the peer config directory with genesis and default files
	cmd := exec.Command(envVars.KwildCliPath, "setup", "init",
		"--chain-id", envVars.ChainId,
		"--root", *tempDir,
		"--genesis", input.GenesisFilePath,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		zap.L().Panic("Failed to generate peer config", zap.Error(err), zap.String("output", string(output)))
	}

	// now override config.toml with the proper peer settings via print-config
	// compute bootnodes list
	var bootnodes []string
	for _, p := range input.Peers {
		bootnodes = append(bootnodes, *p.Address)
	}
	bootStr := strings.Join(bootnodes, ",")
	printCmd := exec.Command(envVars.KwildCliPath, "print-config",
		"--p2p.external-address", *input.CurrentPeer.Address,
		"--p2p.bootnodes", bootStr,
		"--snapshots.enable",
		"--root", *tempDir,
	)
	cfgBytes, err := printCmd.Output()
	if err != nil {
		zap.L().Panic("Failed to generate config.toml", zap.Error(err))
	}
	configPath := filepath.Join(*tempDir, "config.toml")
	if err := os.WriteFile(configPath, cfgBytes, 0644); err != nil {
		zap.L().Panic("Failed to write config.toml", zap.Error(err))
	}

	// replace the private key in the generated configuration
	replacePrivateKeyInConfig(*tempDir, *input.PrivateKey)

	// try finding TOKEN in the generated configuration, to be sure we're not using any token strings
	searchTokenCmd := exec.Command("grep", "-r", "\\${Token", *tempDir)
	output, err = searchTokenCmd.CombinedOutput()
	if err == nil {
		zap.L().Panic("Found TOKEN in generated configuration", zap.String("output", string(output)))
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
		zap.L().Panic("Failed to write private key to file", zap.Error(err))
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
		zap.L().Panic("Failed to read genesis file", zap.String("genesisFilePath", genesisFilePath), zap.Error(err))
	}

	// Check if it's a valid json file
	var genesis map[string]interface{}
	err = json.Unmarshal(genesisFileContent, &genesis)
	if err != nil {
		zap.L().Panic("Failed to unmarshal genesis file", zap.String("genesisFilePath", genesisFilePath), zap.Error(err))
	}

	// Check if it has a "validators" key
	if _, ok := genesis["validators"]; !ok {
		zap.L().Panic("Genesis file doesn't have a 'validators' key", zap.String("genesisFilePath", genesisFilePath), zap.Any("genesis", genesis))
	}
}
