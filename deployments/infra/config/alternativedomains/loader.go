package alternativedomains

import (
	"fmt"
	"os"

	"github.com/aws/constructs-go/constructs/v10"
	"gopkg.in/yaml.v3"

	infraCfg "github.com/trufnetwork/node/infra/config"
)

// LoadConfig reads the alternative domains configuration from the specified YAML file.
// It returns an error if the file cannot be read or parsed.
func LoadConfig(filePath string) (*AlternativeDomainConfig, error) {
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		// If the file doesn't exist, it's not an error, just return nil config.
		if os.IsNotExist(err) {
			return nil, nil // No config file found, proceed without alternative domains.
		}
		return nil, fmt.Errorf("error reading alternative domains config file %s: %w", filePath, err)
	}

	var config AlternativeDomainConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling alternative domains config from %s: %w", filePath, err)
	}

	return &config, nil
}

// GetConfigForStack retrieves the specific StackSuffixConfig for the current stack's suffix.
// It uses the infraCfg.StackSuffix function to determine the current suffix.
// Returns nil if the overall config is nil, or if no configuration exists for the current stack suffix.
func GetConfigForStack(scope constructs.Construct, cfg *AlternativeDomainConfig) *StackSuffixConfig {
	if cfg == nil {
		return nil // No configuration loaded.
	}

	stackSuffix := infraCfg.StackSuffix(scope)
	if stackConfig, ok := (*cfg)[stackSuffix]; ok {
		return &stackConfig // Found config for this stack suffix.
	}

	return nil // No configuration found for this specific stack suffix.
}
