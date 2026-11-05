package config

import (
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/caarlos0/env/v11"
)

// Node keys and the gateway session secret are not synth inputs: they live in
// SSM Parameter Store (see SecretsStore) and instances read them at boot.
type MainEnvironmentVariables struct {
	KwildCliPath    string `env:"KWILD_CLI_PATH,required"`
	CdkDocker       string `env:"CDK_DOCKER,required"`
	ChainId         string `env:"CHAIN_ID,required"`
	IncludeObserver bool   `env:"INCLUDE_OBSERVER" envDefault:"false"`
}

type AutoStackEnvironmentVariables struct {
	MainEnvironmentVariables
	// DB_OWNER must be external, otherwise it will always be unknown
	DbOwner string `env:"DB_OWNER,required"`
	// NodePublicKeys lists the validators' node IDs (<public key hex>#<key type>), comma separated, in node order.
	NodePublicKeys string `env:"NODE_PUBLIC_KEYS,required"`
}

type ConfigStackEnvironmentVariables struct {
	MainEnvironmentVariables
	// NodePublicKeys lists the validators' node IDs (<public key hex>#<key type>), comma separated, in node order.
	NodePublicKeys string `env:"NODE_PUBLIC_KEYS,required"`
	GenesisPath    string `env:"GENESIS_PATH,required"`
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
