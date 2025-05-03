package kwil_network

import (
	"encoding/json"
	"os/exec"
	"strings"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/trufnetwork/node/infra/config"
	"go.uber.org/zap"
)

// NodeKeys reflects the structure returned by `kwild key gen --output json`
type NodeKeys struct {
	KeyType       string `json:"key_type"`
	PrivateKeyHex string `json:"private_key_text"`
	PublicKeyHex  string `json:"public_key_hex"`
	NodeId        string `json:"node_id"`
	Address       string `json:"user_address"`
}

// KeyGenOutput matches the top-level structure of the CLI output.
type KeyGenOutput struct {
	Result NodeKeys `json:"result"`
	Error  string   `json:"error"`
}

func GenerateNodeKeys(scope constructs.Construct) NodeKeys {
	envVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	// Generate new keys using kwild CLI
	cmd := exec.Command(envVars.KwildCliPath, "key", "gen", "--output", "json")

	// read the output of the command. extract from result
	// and return the NodeKeys struct
	var output KeyGenOutput
	bytesOutput, err := cmd.Output()

	if err != nil {
		// Add command output to the error message for better debugging
		zap.L().Panic("Failed to generate node keys", zap.Error(err), zap.String("output", string(bytesOutput)))
	}

	// Trim potential leading/trailing whitespace/newlines from output
	trimmedOutput := strings.TrimSpace(string(bytesOutput))

	if err := json.Unmarshal([]byte(trimmedOutput), &output); err != nil {
		zap.L().Panic("Failed to unmarshal node keys", zap.Error(err), zap.String("raw_output", trimmedOutput))
	}

	// Basic validation after unmarshaling
	if output.Error != "" {
		zap.L().Panic("kwild key gen reported an error", zap.String("error", output.Error))
	}
	if output.Result.PublicKeyHex == "" {
		zap.L().Panic("kwild key gen did not return a public key", zap.Any("result", output.Result))
	}

	return output.Result
}

// ExtractKeys needs similar updates if used.
// For now, focusing on GenerateNodeKeys.
func ExtractKeys(scope constructs.Construct, privateKey string) NodeKeys {
	envVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)
	cmd := exec.Command(envVars.KwildCliPath, "key", "info", privateKey, "--output", "json")
	var output KeyGenOutput
	bytesOutput, err := cmd.Output()
	if err != nil {
		zap.L().Panic("Failed to extract node keys", zap.Error(err), zap.String("output", string(bytesOutput)))
	}
	trimmedOutput := strings.TrimSpace(string(bytesOutput))
	if err := json.Unmarshal([]byte(trimmedOutput), &output); err != nil {
		zap.L().Panic("Failed to unmarshal extracted node keys", zap.Error(err), zap.String("raw_output", trimmedOutput))
	}
	if output.Error != "" {
		zap.L().Panic("kwild key info reported an error", zap.String("error", output.Error))
	}
	if output.Result.PublicKeyHex == "" {
		zap.L().Panic("kwild key info did not return a public key", zap.Any("result", output.Result))
	}
	return output.Result
}
