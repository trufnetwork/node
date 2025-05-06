package alternativedomains

// AlternativeMapping defines the target for a specific alternative FQDN.
type AlternativeMapping struct {
	// TargetComponentId is the logical ID of the CDK component this FQDN points to.
	// Must match an ID registered in the stack's resource target map.
	TargetComponentId string `yaml:"targetComponentId"`
	// RequiresTlsSan specifies if a TLS SAN entry is needed. Defaults to true.
	// Use pointer to distinguish between explicitly false and not set.
	RequiresTlsSan *bool `yaml:"requiresTlsSan,omitempty"`
}

// RequiresTlsSanOrDefault returns the value of RequiresTlsSan, defaulting to true if not set.
func (m *AlternativeMapping) RequiresTlsSanOrDefault() bool {
	if m.RequiresTlsSan == nil {
		return true // Default to true if not specified
	}
	return *m.RequiresTlsSan
}

// StackSuffixConfig holds the configuration for a specific stack suffix.
type StackSuffixConfig struct {
	// AlternativeHostedZoneDomain is the domain name (e.g., "truf.network")
	// where the alternative A records will be created.
	AlternativeHostedZoneDomain string `yaml:"alternativeHostedZoneDomain"`
	// Alternatives maps the desired alternative FQDN to its target configuration.
	Alternatives map[string]AlternativeMapping `yaml:"alternatives"`
}

// AlternativeDomainConfig is the root structure for the configuration file.
// It maps stack suffixes (e.g., "mainnet", "testnet") to their specific configurations.
type AlternativeDomainConfig map[string]StackSuffixConfig
