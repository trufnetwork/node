package config

import (
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/caarlos0/env/v11"
)

type MainEnvironmentVariables struct {
	KwildCliPath  string `env:"KWILD_CLI_PATH" required:"true"`
	CdkDocker     string `env:"CDK_DOCKER" required:"true"`
	ChainId       string `env:"CHAIN_ID" required:"true"`
	SessionSecret string `env:"SESSION_SECRET" required:"true"`
}

type AutoStackEnvironmentVariables struct {
	// IncludeObserver is true if we want to test metrics setup
	IncludeObserver bool `env:"INCLUDE_OBSERVER" default:"false"`
	// DB_OWNER must be external, otherwise it will always be unknown
	DbOwner string `env:"DB_OWNER" required:"true"`
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
