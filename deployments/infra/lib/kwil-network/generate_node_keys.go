package kwil_network

import (
	"encoding/json"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/truflation/tsn-db/infra/config"
	"os/exec"
)

type NodeKeys struct {
	PrivateKeyHex         string `json:"private_key_hex"`
	PrivateKeyBase64      string `json:"private_key_base64"`
	PublicKeyBase64       string `json:"public_key_base64"`
	PublicKeyCometizedHex string `json:"public_key_cometized_hex"`
	PublicKeyPlainHex     string `json:"public_key_plain_hex"`
	Address               string `json:"address"`
	NodeId                string `json:"node_id"`
}

type KeyGenOutput struct {
	Result NodeKeys `json:"result"`
	Error  string   `json:"error"`
}

func GenerateNodeKeys(scope constructs.Construct) NodeKeys {
	envVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	cmd := exec.Command(envVars.KwilAdminBinPath, "key", "gen", "--output", "json")

	// read the output of the command. extract from result
	// and return the NodeKeys struct
	var output KeyGenOutput
	bytesOutput, err := cmd.Output()

	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(bytesOutput, &output)
	if err != nil {
		panic(err)
	}

	return output.Result
}

func ExtractKeys(scope constructs.Construct, privateKey string) NodeKeys {
	envVars := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	cmd := exec.Command(envVars.KwilAdminBinPath, "key", "info", privateKey, "--output", "json")

	// read the output of the command. extract from result
	// and return the NodeKeys struct
	var output KeyGenOutput
	bytesOutput, err := cmd.Output()

	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(bytesOutput, &output)
	if err != nil {
		panic(err)
	}

	return output.Result
}
