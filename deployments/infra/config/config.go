package config

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
)

// Context keys
const (
	ContextKeyPairName = "keyPairName"
	ContextNumOfNodes  = "numOfNodes"
	ContextStage       = "stage"
	ContextDevPrefix   = "devPrefix" // Key for the dev prefix context variable
	ContextStackSuffix = "stackSuffix"
	// ContextAltDomainConfigPath defines the CDK context key used to specify the
	// path to the alternative domains configuration YAML file.
	ContextAltDomainConfigPath = "altDomainConfigPath"
)

var devPrefixRegex = regexp.MustCompile(`^[a-zA-Z0-9-]*$`)

// Stack suffix is intended to be used after the stack name to differentiate between different stages.
func WithStackSuffix(scope constructs.Construct, stackName string) string {
	// Always append the standard suffix
	stackSuffix := StackSuffix(scope)
	return stackName + "-" + stackSuffix + "-Stack"
}

// DO NOT modify this function, change stack suffix by 'cdk.json/context/stackSuffix'.
func StackSuffix(scope constructs.Construct) string {
	stackSuffix := "default"

	ctxValue := scope.Node().TryGetContext(jsii.String(ContextStackSuffix))
	if v, ok := ctxValue.(string); ok && v != "" {
		stackSuffix = v
	}
	return stackSuffix
}

// DO NOT modify this function, change EC2 key pair name by 'cdk.json/context/keyPairName'.
func KeyPairName(scope constructs.Construct) string {
	keyPairName := "MyKeyPair" // Default value

	ctxValue := scope.Node().TryGetContext(jsii.String(ContextKeyPairName))
	if v, ok := ctxValue.(string); ok && v != "" {
		keyPairName = v
	}

	return keyPairName
}

func NumOfNodes(scope constructs.Construct) int {
	numOfNodes := 1 // Default value

	ctxValue := scope.Node().TryGetContext(jsii.String(ContextNumOfNodes))
	if ctxValue != nil {
		// ctxValue may be a float64 or a string
		switch v := ctxValue.(type) {
		case float64:
			numOfNodes = int(v)
		case string:
			var err error
			numOfNodes, err = strconv.Atoi(v)
			if err != nil {
				panic(fmt.Sprintf("numOfNodes context value is not a number: %s", v))
			}
		}
	}

	cdklogger.LogInfo(scope, "", "Number of validator nodes: %d (from CDK context '%s')", numOfNodes, ContextNumOfNodes)
	return numOfNodes
}

// GetStage retrieves the deployment stage ('prod' or 'dev') from the CDK context.
// It panics if the 'stage' context variable is missing or invalid, as it must be provided explicitly during synthesis.
func GetStage(scope constructs.Construct) domaincfg.StageType {
	ctxValue := scope.Node().TryGetContext(jsii.String(ContextStage)) // Using constant
	if ctxValue == nil {
		panic(fmt.Sprintf("Mandatory context variable '%s' is missing. Provide it using -c %s=<value> (e.g., 'dev' or 'prod').", ContextStage, ContextStage))
	}

	stageStr, ok := ctxValue.(string)
	if !ok {
		panic(fmt.Sprintf("Context variable '%s' must be a string, got %T", ContextStage, ctxValue))
	}

	stage := domaincfg.StageType(stageStr)
	switch stage {
	case domaincfg.StageProd, domaincfg.StageDev:
		cdklogger.LogInfo(scope, "", "Using stage: '%s' (from CDK context '%s')", stage, ContextStage)
		return stage // Valid stage
	default:
		panic(fmt.Sprintf("Invalid value for context variable '%s': '%s'. Must be '%s' or '%s'.",
			ContextStage, stageStr, domaincfg.StageProd, domaincfg.StageDev))
	}
}

// GetDevPrefix retrieves the development prefix from the CDK context.
// Returns an empty string if not specified.
// Panics if the prefix is provided but contains invalid characters.
func GetDevPrefix(scope constructs.Construct) string {
	devPrefix := "" // Default to empty string

	ctxValue := scope.Node().TryGetContext(jsii.String(ContextDevPrefix))
	if ctxValue != nil {
		prefixStr, ok := ctxValue.(string)
		if !ok {
			panic(fmt.Sprintf("Context variable '%s' must be a string, got %T", ContextDevPrefix, ctxValue))
		}
		// Validate against the pattern previously used in the CfnParameter
		if !devPrefixRegex.MatchString(prefixStr) {
			panic(fmt.Sprintf("Invalid value for context variable '%s': '%s'. Must be alphanumeric and may include hyphens.", ContextDevPrefix, prefixStr))
		}
		devPrefix = prefixStr
	}

	// Log current devPrefix, even if it's the default empty string, to confirm it was processed.
	cdklogger.LogInfo(scope, "", "DevPrefix: '%s' (from CDK context '%s')", devPrefix, ContextDevPrefix)
	// Note: Validation that devPrefix is empty for 'prod' stage might be better placed
	// where both stage and prefix are known, e.g., in domain_config.go or stack logic.
	return devPrefix
}

// GetAltDomainConfigPath retrieves the alternative domain configuration file path from CDK context.
// It looks for the key defined by ContextAltDomainConfigPath.
// If the context variable is not set or is empty, it returns a default path.
func GetAltDomainConfigPath(scope constructs.Construct) string {
	// Default path relative to the CDK application root (e.g., deployments/infra).
	defaultPath := "config/alternative-domains.yaml"
	ctxValue := scope.Node().TryGetContext(jsii.String(ContextAltDomainConfigPath))
	if v, ok := ctxValue.(string); ok && v != "" {
		return v // Return context value if provided and non-empty.
	}
	return defaultPath // Return default path otherwise.
}
