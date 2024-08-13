package config

import (
	"os"
	"reflect"
)

type MainEnvironmentVariables struct {
	KwilAdminBinPath string `env:"KWIL_ADMIN_BIN_PATH" required:"true"`
	CdkDocker        string `env:"CDK_DOCKER" required:"true"`
	ChainId          string `env:"CHAIN_ID" required:"true"`
	PrivateKey       string `env:"PRIVATE_KEY" required:"true"`
}

type AutoStackEnvironmentVariables struct {
	// when this hash changes, all instances will be redeployed
	RestartHash string `env:"RESTART_HASH"`
}

type ConfigStackEnvironmentVariables struct {
	// comma separated list of private keys for the nodes
	NodePrivateKeys string `env:"NODE_PRIVATE_KEYS" required:"true"`
	GenesisFilePath string `env:"GENESIS_FILE_PATH" required:"true"`
}

func GetEnvironmentVariables[T any]() T {
	var env T
	t := reflect.TypeOf(env)
	v := reflect.ValueOf(&env).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("env")
		value := GetEnv(tag)

		if value == "" {
			if field.Tag.Get("required") == "true" {
				panic("Required environment variable not set: " + tag)
			}
			continue
		}

		v.Field(i).SetString(value)
	}

	return env
}

func GetEnv(key string) string {
	return os.Getenv(key)
}
