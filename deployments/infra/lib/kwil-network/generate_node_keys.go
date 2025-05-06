package kwil_network

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
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
	args := []string{"key", "gen", "--output", "json"}
	commandString := envVars.KwildCliPath + " " + strings.Join(args, " ")
	cdklogger.LogInfo(scope, "NodeKeyGenerator", "Executing: %s", commandString)

	cmd := exec.Command(envVars.KwildCliPath, args...)
	startTime := time.Now()
	bytesOutput, err := cmd.CombinedOutput() // Use CombinedOutput to get stderr as well
	duration := time.Since(startTime)
	trimmedOutput := strings.TrimSpace(string(bytesOutput))

	if err != nil {
		cdklogger.LogError(scope, "NodeKeyGenerator", "Failed to execute 'kwild key gen'. Command: %s, Duration: %s, Error: %s, Output: %s", commandString, duration.String(), err.Error(), trimmedOutput)
		panic(fmt.Sprintf("Failed to generate node keys via 'kwild key gen': %v. Output: %s", err, trimmedOutput))
	}

	var output KeyGenOutput
	if err := json.Unmarshal([]byte(trimmedOutput), &output); err != nil {
		cdklogger.LogError(scope, "NodeKeyGenerator", "Failed to unmarshal 'kwild key gen' output. Duration: %s, Error: %s, RawOutput: %s", duration.String(), err.Error(), trimmedOutput)
		panic(fmt.Sprintf("Failed to unmarshal 'kwild key gen' output: %v. RawOutput: %s", err, trimmedOutput))
	}

	if output.Error != "" {
		cdklogger.LogError(scope, "NodeKeyGenerator", "'kwild key gen' reported an error. Duration: %s, Error: %s, RawOutput: %s", duration.String(), output.Error, trimmedOutput)
		panic(fmt.Sprintf("'kwild key gen' reported an error: %s. RawOutput: %s", output.Error, trimmedOutput))
	}
	if output.Result.PublicKeyHex == "" { // Assuming PublicKeyHex is the correct field name
		cdklogger.LogError(scope, "NodeKeyGenerator", "'kwild key gen' did not return a public key. Duration: %s, Result: %+v, RawOutput: %s", duration.String(), output.Result, trimmedOutput)
		panic(fmt.Sprintf("'kwild key gen' did not return a public key. RawOutput: %s", trimmedOutput))
	}

	cdklogger.LogInfo(scope, "NodeKeyGenerator", "Node keys generated successfully via 'kwild key gen'. Duration: %s, NodeID: %s, PublicKeyHex: %s", duration.String(), output.Result.NodeId, output.Result.PublicKeyHex)
	return output.Result
}

func ExtractKeys(scope constructs.Construct, privateKey string) NodeKeys {
	envVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)
	args := []string{"key", "info", privateKey, "--output", "json"}
	commandString := envVars.KwildCliPath + " " + strings.Join(args, " ")
	cdklogger.LogInfo(scope, "NodeKeyExtractor", "Executing: %s", commandString)

	cmd := exec.Command(envVars.KwildCliPath, args...)
	startTime := time.Now()
	bytesOutput, err := cmd.CombinedOutput()
	duration := time.Since(startTime)
	trimmedOutput := strings.TrimSpace(string(bytesOutput))

	if err != nil {
		cdklogger.LogError(scope, "NodeKeyExtractor", "Failed to execute 'kwild key info'. Command: %s, Duration: %s, Error: %s, Output: %s", commandString, duration.String(), err.Error(), trimmedOutput)
		panic(fmt.Sprintf("Failed to extract node keys via 'kwild key info': %v. Output: %s", err, trimmedOutput))
	}

	var output KeyGenOutput
	if err := json.Unmarshal([]byte(trimmedOutput), &output); err != nil {
		cdklogger.LogError(scope, "NodeKeyExtractor", "Failed to unmarshal 'kwild key info' output. Duration: %s, Error: %s, RawOutput: %s", duration.String(), err.Error(), trimmedOutput)
		panic(fmt.Sprintf("Failed to unmarshal 'kwild key info' output: %v. RawOutput: %s", err, trimmedOutput))
	}

	if output.Error != "" {
		cdklogger.LogError(scope, "NodeKeyExtractor", "'kwild key info' reported an error. Duration: %s, Error: %s, RawOutput: %s", duration.String(), output.Error, trimmedOutput)
		panic(fmt.Sprintf("'kwild key info' reported an error: %s. RawOutput: %s", output.Error, trimmedOutput))
	}
	if output.Result.PublicKeyHex == "" {
		cdklogger.LogError(scope, "NodeKeyExtractor", "'kwild key info' did not return a public key. Duration: %s, Result: %+v, RawOutput: %s", duration.String(), output.Result, trimmedOutput)
		panic(fmt.Sprintf("'kwild key info' did not return a public key. RawOutput: %s", trimmedOutput))
	}
	cdklogger.LogInfo(scope, "NodeKeyExtractor", "Node keys extracted successfully via 'kwild key info'. Duration: %s, NodeID: %s, PublicKeyHex: %s", duration.String(), output.Result.NodeId, output.Result.PublicKeyHex)
	return output.Result
}
