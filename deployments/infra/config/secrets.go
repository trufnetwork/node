package config

import (
	"fmt"
	"strings"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	"github.com/trufnetwork/node/infra/lib/cdklogger"
	"github.com/trufnetwork/node/infra/lib/secrets"
)

// Context keys for the SSM parameters that hold node keys and the gateway
// session secret.
const (
	// ContextSecretsPrefix overrides the SSM path prefix. Default: /tsn/<stage>,
	// or /tsn/<stage>/<devPrefix> when a dev prefix is set.
	ContextSecretsPrefix = "secretsPrefix"
	// ContextSecretsKmsKeyArn is the customer managed KMS key of the
	// parameters. Leave it unset for the AWS managed aws/ssm key.
	ContextSecretsKmsKeyArn = "secretsKmsKeyArn"
)

// SecretsStore returns where node keys and the gateway session secret are
// stored in SSM Parameter Store.
func SecretsStore(scope constructs.Construct) secrets.Store {
	prefix := contextString(scope, ContextSecretsPrefix)
	if prefix == "" {
		prefix = secrets.DefaultPrefix(string(GetStage(scope)), GetDevPrefix(scope))
	}
	if err := secrets.ValidatePrefix(prefix); err != nil {
		panic(fmt.Sprintf("context %s: %v", ContextSecretsPrefix, err))
	}

	kmsKeyArn := contextString(scope, ContextSecretsKmsKeyArn)
	if kmsKeyArn != "" {
		if err := secrets.ValidateKmsKeyArn(kmsKeyArn); err != nil {
			panic(fmt.Sprintf("context %s: %v", ContextSecretsKmsKeyArn, err))
		}
	}

	cdklogger.LogInfo(scope, "", "Node keys and the gateway session secret are read from SSM under %s (from CDK context '%s' or the stage default)", prefix, ContextSecretsPrefix)
	return secrets.Store{Prefix: prefix, KmsKeyArn: kmsKeyArn}
}

// contextString reads an optional string context value.
func contextString(scope constructs.Construct, key string) string {
	raw := scope.Node().TryGetContext(jsii.String(key))
	if raw == nil {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		panic(fmt.Sprintf("Context variable '%s' must be a string, got %T", key, raw))
	}
	return strings.TrimSpace(value)
}
