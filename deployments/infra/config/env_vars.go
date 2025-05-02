package config

import (
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/caarlos0/env/v11"
)

type MainEnvironmentVariables struct {
	KwildCliPath    string `env:"KWILD_CLI_PATH,required"`
	CdkDocker       string `env:"CDK_DOCKER,required"`
	ChainId         string `env:"CHAIN_ID,required"`
	SessionSecret   string `env:"SESSION_SECRET,required"`
	IncludeObserver bool   `env:"INCLUDE_OBSERVER" envDefault:"false"`
}

type AutoStackEnvironmentVariables struct {
	MainEnvironmentVariables
	// DB_OWNER must be external, otherwise it will always be unknown
	DbOwner string `env:"DB_OWNER,required"`
}

type ConfigStackEnvironmentVariables struct {
	MainEnvironmentVariables
	NodePrivateKeys string `env:"NODE_PRIVATE_KEYS,required"`
	GenesisPath     string `env:"GENESIS_PATH,required"`
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
