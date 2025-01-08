package kwil_network

import (
	"encoding/json"
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"go.uber.org/zap"
	"os"
	"os/exec"
	"strconv"
)

type GenerateGenesisFileInput struct {
	PeerConnections []peer.TSNPeer
	ChainId         string
}

// GenerateGenesisFile generates a genesis file, with all peers in the network as validators
// It returns the path of the generated genesis file
// it does that by executing
//   - create temp dir
//   - generate complete config
//     kwil-admin setup init -o <tmp-dir> --chain-id <chainId>
//   - reading the genesis file inside it at <tmp-dir>/genesis.json
//   - modifying the genesis file to include all peers as validators

func GenerateGenesisFile(scope constructs.Construct, input GenerateGenesisFileInput) string {
	// Create a temporary directory for the configuration
	tempDir := awscdk.FileSystem_Mkdtemp(jsii.String("genesis-config"))

	// Prepare Validators list
	var validators []Validator
	for i, p := range input.PeerConnections {
		validators = append(validators, Validator{
			PubKey: p.NodeHexAddress,
			Power:  1,
			Name:   "validator-" + strconv.Itoa(i),
		})
	}
	// Generate configuration using kwil-admin CLI
	// kwil-admin setup init -o <tmp-dir> --chain-id <chainId>
	envVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)
	cmd := exec.Command(envVars.KwilAdminBinPath, "setup", "init",
		"--chain-id", input.ChainId,
		"-o", *tempDir,
	)

	_, err := cmd.CombinedOutput()
	if err != nil {
		zap.L().Panic("Failed to generate genesis file", zap.Error(err))
	}

	// Read the genesis file
	genesisFile := *tempDir + "/genesis.json"
	genesisFileContent, err := os.ReadFile(genesisFile)
	if err != nil {
		zap.L().Panic("Failed to read genesis file", zap.Error(err))
	}

	// Modify the genesis file to include all peers as validators
	genesis := make(map[string]interface{})
	err = json.Unmarshal(genesisFileContent, &genesis)
	if err != nil {
		zap.L().Panic("Failed to unmarshal genesis file", zap.Error(err))
	}

	genesis["validators"] = validators

	// Return the path of the generated configuration directory
	genesisBytes, err := json.Marshal(genesis)

	if err != nil {
		zap.L().Panic("Failed to marshal genesis file", zap.Error(err))
	}

	err = os.WriteFile(genesisFile, genesisBytes, 0644)
	if err != nil {
		zap.L().Panic("Failed to write genesis file", zap.Error(err))
	}

	return genesisFile
}

// Validator represents a validator in the network
//
//	"validators":
//	{
//	"pub_key": "16e826f5e09ff86ab2d5b04a03334ce640b5ca9ec005f57364c0f37890c39d8d",
//	"power": 1,
//	"name": "validator-0"
//	}
type Validator struct {
	PubKey string `json:"pub_key"`
	Power  int    `json:"power"`
	Name   string `json:"name"`
}
