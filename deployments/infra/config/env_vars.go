package config

import (
	"os"
	"reflect"
)

type EnvironmentVariables struct {
	KwilAdminBinPath string `env:"KWIL_ADMIN_BIN_PATH" required:"true"`
	CdkDocker        string `env:"CDK_DOCKER" required:"true"`
	ChainId          string `env:"CHAIN_ID" required:"true"`
	PrivateKey       string `env:"PRIVATE_KEY" required:"true"`
}

func GetEnvironmentVariables() EnvironmentVariables {
	var env EnvironmentVariables
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
