package config

import (
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/caarlos0/env/v11"
)

type MainEnvironmentVariables struct {
	KwilAdminBinPath string `env:"KWIL_ADMIN_BIN_PATH" required:"true"`
	CdkDocker        string `env:"CDK_DOCKER" required:"true"`
	ChainId          string `env:"CHAIN_ID" required:"true"`
	PrivateKey       string `env:"PRIVATE_KEY" required:"true"`
}

type AutoStackEnvironmentVariables struct {
	IncludeObserver bool `env:"INCLUDE_OBSERVER" default:"false"`
}

type ConfigStackEnvironmentVariables struct {
	// comma separated list of private keys for the nodes
	NodePrivateKeys string `env:"NODE_PRIVATE_KEYS" required:"true"`
	GenesisPath     string `env:"GENESIS_PATH" required:"true"`
}

func GetEnvironmentVariables[T any](scope constructs.Construct) T {
	var envObj T

	// only run if we are synthesizing the stack
	if !IsStackInSynthesis(scope) {
		return envObj
	}

	err := env.Parse(&envObj)
	if err != nil {
		panic(err)
	}

	return envObj
}
