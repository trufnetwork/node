package alternativedomainmanager

import (
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigatewayv2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53targets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	infraCfg "github.com/trufnetwork/node/infra/config"
	altcfg "github.com/trufnetwork/node/infra/config/alternativedomains"
	"github.com/trufnetwork/node/infra/lib/constructs/fronting"
	validator_set "github.com/trufnetwork/node/infra/lib/constructs/validator_set"
)

// safeZoneName is a helper to get a zone name string for logging, handling nil cases.
func safeZoneName(zone awsroute53.IHostedZone) string {
	if zone == nil || zone.ZoneName() == nil || *zone.ZoneName() == "" {
		return "[unspecified zone]"
	}
	return *zone.ZoneName()
}

// Define constants for well-known logical target component IDs used in the
// alternative domains configuration and target registration.
const (
	TargetGateway = "Gateway" // Logical ID for the Gateway fronting target.
	TargetIndexer = "Indexer" // Logical ID for the Indexer fronting target.
)

// NodeTargetID generates a consistent logical ID string for a validator node target based on its index.
// Example: NodeTargetID(0) -> "Node-1"
func NodeTargetID(index int) string {
	// Uses 1-based indexing for node IDs in configuration.
	return fmt.Sprintf("Node-%d", index+1)
}

// AlternativeRecordConstructID generates a unique and valid CDK construct ID for an alternative A record.
// It replaces dots in the FQDN with hyphens to conform to CloudFormation ID constraints.
func AlternativeRecordConstructID(altFqdn string) string {
	cleanFqdn := strings.ReplaceAll(altFqdn, ".", "-")
	return fmt.Sprintf("AltARecord-%s", cleanFqdn)
}

// SanListBuilder provides a helper for collecting and deduplicating domain names
// intended for use as Subject Alternative Names (SANs) in a TLS certificate.
// It ensures the final list is unique and deterministically ordered.
type SanListBuilder struct {
	sans map[string]struct{} // Use map for efficient presence tracking and deduplication.
}

// NewSanListBuilder creates and initializes a new SanListBuilder.
func NewSanListBuilder() *SanListBuilder {
	return &SanListBuilder{
		sans: make(map[string]struct{}),
	}
}

// Add incorporates one or more FQDNs into the builder's internal set.
// Nil or empty strings are ignored. Duplicates are automatically handled by the map.
func (b *SanListBuilder) Add(fqdns ...*string) {
	for _, fqdnPtr := range fqdns {
		if fqdnPtr != nil && *fqdnPtr != "" {
			b.sans[*fqdnPtr] = struct{}{}
		}
	}
}

// List returns the final, deduplicated list of SANs as a slice of string pointers,
// sorted alphabetically to ensure deterministic output for CDK.
// Returns nil if no SANs were added, as expected by AWS CDK certificate constructs.
func (b *SanListBuilder) List() []*string {
	if len(b.sans) == 0 {
		return nil // Return nil if empty, as CDK expects
	}

	// Extract keys (SANs) from the map for sorting
	sanKeys := make([]string, 0, len(b.sans))
	for san := range b.sans {
		sanKeys = append(sanKeys, san)
	}

	// Sort the keys to ensure deterministic order
	sort.Strings(sanKeys)

	listValues := make([]string, 0, len(sanKeys))
	for _, san := range sanKeys {
		listValues = append(listValues, san)
	}

	// jsii.Strings returns *[]*string
	jsiiList := jsii.Strings(listValues...)
	// Dereference the pointer to return []*string, matching expected type for CertificateProps.SubjectAlternativeNames
	return *jsiiList
}

// AlternativeDomainManagerProps defines the input properties for the AlternativeDomainManager construct.
type AlternativeDomainManagerProps struct {

	// AlternativeHostedZoneDomainOverride optionally specifies a hosted zone domain name
	// to use instead of the one defined in the loaded configuration file.
	// Useful for testing or specific deployment scenarios.
	AlternativeHostedZoneDomainOverride *string
}

// AlternativeDomainManager is a CDK construct responsible for orchestrating the creation
// of alternative domain name resources based on a YAML configuration file.
// It handles loading the config, collecting required TLS SANs, registering target resources
// (like Nodes, Gateways), and creating Route 53 A records and associated API Gateway
// resources in a designated hosted zone.
type AlternativeDomainManager struct {
	constructs.Construct
	scope           constructs.Construct // The parent CDK scope (usually the Stack) for annotations and context access.
	props           *AlternativeDomainManagerProps
	configFilePath  string                          // Path to the config file, resolved from context or default.
	stackSuffix     string                          // Deployment suffix (e.g., "prod"), resolved from context.
	fullConfig      *altcfg.AlternativeDomainConfig // Stores the entire loaded configuration.
	stackConfig     *altcfg.StackSuffixConfig       // Pointer to the config specific to the current stackSuffix (derived from fullConfig).
	dnsTargets      map[string]fronting.DnsTarget   // Registry of components (Nodes, Gateway, etc.) identified by logical ID.
	resolvedAltZone awsroute53.IHostedZone          // The looked-up Route 53 hosted zone for creating alternative A records.
	sanBuilder      *SanListBuilder                 // Internal SAN builder.
}

// NewAlternativeDomainManager creates and initializes a new AlternativeDomainManager instance.
// It reads the configuration file path and stack suffix from the CDK context via the provided scope.
func NewAlternativeDomainManager(scope constructs.Construct, id string, props *AlternativeDomainManagerProps) *AlternativeDomainManager {
	construct := constructs.NewConstruct(scope, jsii.String(id))
	mgr := &AlternativeDomainManager{
		Construct:      construct,
		scope:          scope,
		props:          props,
		dnsTargets:     make(map[string]fronting.DnsTarget),
		configFilePath: infraCfg.GetAltDomainConfigPath(scope),
		stackSuffix:    infraCfg.StackSuffix(scope),
		sanBuilder:     NewSanListBuilder(), // Initialize internal SanListBuilder
	}

	mgr.annotateInfo("[ADM Phase 1/3] Initializing: Loading configuration and preparing for target registration.")

	// Load configuration immediately upon creation based on context.
	mgr.loadConfig()
	// Targets are registered by external calls to mgr.RegisterTarget(), which also logs.
	return mgr
}

// loadConfig reads and parses the alternative domains YAML configuration file.
// It uses the configFilePath and stackSuffix determined during manager instantiation.
// Sets the internal stackConfig field if applicable configuration is found.
func (m *AlternativeDomainManager) loadConfig() {
	if m.configFilePath == "" {
		m.annotateInfo("Alternative domain ConfigFilePath (from context or default) is empty. Skipping setup.")
		return
	}

	loadedConfig, err := altcfg.LoadConfig(m.configFilePath)
	if err != nil {
		m.annotateWarning("Failed to load alternative domains config from '%s': %s. Skipping setup.", m.configFilePath, err.Error())
		return
	}
	if loadedConfig == nil {
		m.annotateInfo("Alternative domains config file '%s' not found or empty. Skipping setup.", m.configFilePath)
		return
	}
	m.fullConfig = loadedConfig // Store the full config

	if m.stackSuffix == "" {
		// This case should ideally not happen if StackSuffix() has a default
		m.annotateWarning("StackSuffix (from context or default) is empty. Cannot determine alternative domain config. Skipping setup.")
		return
	}

	if stackCfg, ok := (*m.fullConfig)[m.stackSuffix]; ok {
		m.stackConfig = &stackCfg
		m.annotateInfo("Loaded alternative domain configuration for stack suffix: %s (from file: %s)", m.stackSuffix, m.configFilePath)
	} else {
		m.annotateInfo("No alternative domain configuration found for stack suffix: '%s' in file '%s'. Skipping setup.", m.stackSuffix, m.configFilePath)
		m.stackConfig = nil // Ensure it's nil if not found
	}
}

// RegisterTarget adds a resource that implements the fronting.DnsTarget interface
// to the manager's internal registry, associating it with a logical ID.
// This ID must match the `targetComponentId` used in the alternative-domains.yaml file.
// Warns if the ID is already registered and overwrites the previous target.
// Parameter target: The resource (e.g., NodeTarget, FrontingResult) to register.
func (m *AlternativeDomainManager) RegisterTarget(id string, target fronting.DnsTarget) {
	if id == "" || target == nil {
		m.annotateWarning("Attempted to register target with empty ID or nil target. Skipping.")
		return
	}
	// Ensure PrimaryFQDN is not nil before dereferencing for logging
	var primaryFqdnMsg string
	if target.PrimaryFQDN() != nil {
		primaryFqdnMsg = *target.PrimaryFQDN()
	} else {
		primaryFqdnMsg = "[PrimaryFQDN not available]"
	}

	if _, exists := m.dnsTargets[id]; exists {
		m.annotateWarning("Target ID '%s' is already registered. Overwriting with target whose primary FQDN is %s.", id, primaryFqdnMsg)
	}
	m.dnsTargets[id] = target
	m.annotateInfo("Registered alternative domain target: %s -> %s", id, primaryFqdnMsg)
}

// collectAndAddConfiguredSansToBuilderInternal iterates through the loaded configuration and adds FQDNs marked
// with `requiresTlsSan: true` to the internal sanBuilder.
// This method is now internal and called by GetCertificateRequirements.
func (m *AlternativeDomainManager) collectAndAddConfiguredSansToBuilderInternal() {
	if m.stackConfig == nil {
		m.annotateInfo("stackConfig is nil in collectAndAddConfiguredSansToBuilderInternal. No alternative domains configured or loaded for this stack suffix. No SANs will be added from config.")
		return
	}

	for altFqdn, mapping := range m.stackConfig.Alternatives {
		if mapping.RequiresTlsSanOrDefault() {
			m.annotateInfo("Adding configured SAN '%s' (target: %s) to internal certificate builder.", altFqdn, mapping.TargetComponentId)
			m.sanBuilder.Add(jsii.String(altFqdn))
		}
	}
}

// GetCertificateRequirements analyzes the configuration and explicit inputs to determine
// all necessary properties for creating a shared TLS certificate.
func (m *AlternativeDomainManager) GetCertificateRequirements(
	primaryZone awsroute53.IHostedZone,
	additionalExplicitSans ...*string, // FQDNs the stack explicitly wants on the cert
) (
	certificateDomainName *string, // The suggested primary domain for the certificate
	allSubjectAlternativeNames []*string, // The complete, deduplicated, sorted list of SANs
	validationMethod awscertificatemanager.CertificateValidation, // The appropriate validation
	err error,
) {
	m.annotateInfo("[ADM Phase 2/3] Collecting SANs for certificate.")

	if m.sanBuilder == nil { // Should be initialized in NewAlternativeDomainManager
		return nil, nil, nil, fmt.Errorf("internal SanListBuilder not initialized")
	}
	m.sanBuilder = NewSanListBuilder() // Clear any previous state, ensure fresh build

	// 1. Collect SANs from alternative domain configuration
	m.collectAndAddConfiguredSansToBuilderInternal()

	if len(additionalExplicitSans) > 0 {
		explicitSanStrings := []string{}
		for _, sanPtr := range additionalExplicitSans {
			if sanPtr != nil && *sanPtr != "" {
				explicitSanStrings = append(explicitSanStrings, *sanPtr)
			}
		}
		if len(explicitSanStrings) > 0 {
			m.annotateInfo("[ADM Phase 2/3] Adding explicitly provided SANs to builder: [%s]", strings.Join(explicitSanStrings, ", "))
		}
	}
	m.sanBuilder.Add(additionalExplicitSans...)

	allSubjectAlternativeNames = m.sanBuilder.List()

	// 2. Determine the primary domain name for the certificate.
	// The certificate's DomainName property *must* be one of the SubjectAlternativeNames.
	// Precedence: First explicit SAN, then first from config/combined list.
	if len(additionalExplicitSans) > 0 && additionalExplicitSans[0] != nil && *additionalExplicitSans[0] != "" {
		certificateDomainName = additionalExplicitSans[0]
		m.annotateInfo("Using first explicitly provided SAN '%s' as primary certificate domain name. This SAN is already included in the SAN builder via sanBuilder.Add().", *certificateDomainName)
	} else if len(allSubjectAlternativeNames) > 0 && allSubjectAlternativeNames[0] != nil && *allSubjectAlternativeNames[0] != "" {
		// If no explicit SANs were given to prioritize (or they were empty),
		// use the first SAN from the list derived from the builder (which includes configured SANs and any explicit ones previously added).
		certificateDomainName = allSubjectAlternativeNames[0]
		m.annotateInfo("Using first SAN from the combined list (config and/or explicit) '%s' as primary certificate domain name.", *certificateDomainName)
	} else {
		// No SANs from config and no explicit SANs provided by the caller.
		// Cannot determine a primary domain for the certificate.
		m.annotateError("No SANs from configuration and no explicit SANs were provided. Cannot determine certificate parameters.")
		return nil, nil, nil, fmt.Errorf("no SANs available (from configuration or explicit input) to determine a primary domain name for the certificate")
	}
	// At this point, if we haven't returned with an error, certificateDomainName is non-nil.
	// The SAN list (allSubjectAlternativeNames) is also prepared.
	// The chosen certificateDomainName is inherently part of allSubjectAlternativeNames because:
	// 1. If chosen from additionalExplicitSans[0], these were added to sanBuilder.
	// 2. If chosen from allSubjectAlternativeNames[0], it's directly from the list built by sanBuilder.

	// 3. Determine if multi-zone validation is needed
	// This primarily depends on whether an alternative hosted zone is configured and different from the primary.
	altZoneDomainOverride := ""
	if m.props != nil && m.props.AlternativeHostedZoneDomainOverride != nil && *m.props.AlternativeHostedZoneDomainOverride != "" {
		altZoneDomainOverride = *m.props.AlternativeHostedZoneDomainOverride
	}

	altZoneNameFromConfig := ""
	if m.stackConfig != nil && m.stackConfig.AlternativeHostedZoneDomain != "" {
		altZoneNameFromConfig = m.stackConfig.AlternativeHostedZoneDomain
	}

	altZoneName := altZoneDomainOverride // Override takes precedence
	if altZoneName == "" {
		altZoneName = altZoneNameFromConfig
	}

	needsMultiZoneValidation := false
	if altZoneName != "" && primaryZone != nil && primaryZone.ZoneName() != nil && strings.TrimSuffix(altZoneName, ".") != strings.TrimSuffix(*primaryZone.ZoneName(), ".") {
		needsMultiZoneValidation = true
		m.annotateInfo("Multi-zone validation is potentially needed. Primary zone: '%s', Configured alternative zone name: '%s'.", *primaryZone.ZoneName(), altZoneName)
	} else {
		if altZoneName == "" {
			m.annotateInfo("No alternative zone domain configured or overridden. Using single-zone validation with primary zone: '%s'.", safeZoneName(primaryZone))
		} else if primaryZone == nil || primaryZone.ZoneName() == nil {
			m.annotateWarning("Primary zone is nil, cannot compare for multi-zone validation. Defaulting to single-zone validation with primary zone (if available).")
		} else { // altZoneName is same as primaryZone.ZoneName()
			m.annotateInfo("Configured alternative zone '%s' is the same as the primary zone '%s'. Using single-zone validation.", altZoneName, *primaryZone.ZoneName())
		}
		// Fallback to single-zone DNS validation if multi-zone is not applicable or primaryZone is nil.
		if primaryZone != nil {
			validationMethod = awscertificatemanager.CertificateValidation_FromDns(primaryZone)
		} else {
			// This is a critical issue, as no zone is available for validation.
			m.annotateError("PrimaryHostedZone is nil and multi-zone validation is not applicable. Cannot set DNS validation method.")
			return nil, nil, nil, fmt.Errorf("primaryHostedZone is nil, cannot determine certificate validation method")
		}
	}

	if needsMultiZoneValidation {
		// Attempt to lookup/resolve the alternative hosted zone.
		// altZoneName is the string name (e.g., "mainnet.truf.network")
		// m.lookupAlternativeZone expects the domain name string and sets m.resolvedAltZone.
		normalizedAltZoneNameForLookup := strings.TrimSuffix(altZoneName, ".")
		if m.resolvedAltZone == nil || // If not resolved yet
			(m.resolvedAltZone.ZoneName() != nil && strings.TrimSuffix(*m.resolvedAltZone.ZoneName(), ".") != normalizedAltZoneNameForLookup) { // Or resolved to something different
			m.annotateInfo("Looking up alternative zone: '%s'", normalizedAltZoneNameForLookup)
			m.lookupAlternativeZone(normalizedAltZoneNameForLookup) // This method sets m.resolvedAltZone
		}

		if m.resolvedAltZone != nil && m.resolvedAltZone.ZoneName() != nil && *m.resolvedAltZone.ZoneName() != "" {
			m.annotateInfo("Successfully resolved alternative hosted zone: '%s'. Proceeding with multi-zone validation mapping.", *m.resolvedAltZone.ZoneName())

			currentValidationDomains := make(map[string]awsroute53.IHostedZone)

			// Collect all unique FQDNs for the certificate.
			// certificateDomainName is the primary name.
			// allSubjectAlternativeNames contains all SANs (which should include the primary name if correctly added to sanBuilder).
			allFqdnsForCertSet := make(map[string]struct{})
			if certificateDomainName != nil && *certificateDomainName != "" {
				allFqdnsForCertSet[*certificateDomainName] = struct{}{}
			}
			if len(allSubjectAlternativeNames) > 0 {
				for _, sanPtr := range allSubjectAlternativeNames {
					if sanPtr != nil && *sanPtr != "" {
						allFqdnsForCertSet[*sanPtr] = struct{}{}
					}
				}
			}

			if len(allFqdnsForCertSet) == 0 {
				m.annotateWarning("Multi-zone validation indicated, but no FQDNs (primary or SANs) found for the certificate. Falling back to single-zone validation with primary zone '%s'.", safeZoneName(primaryZone))
				if primaryZone != nil {
					validationMethod = awscertificatemanager.CertificateValidation_FromDns(primaryZone)
				} else {
					m.annotateError("PrimaryHostedZone is nil during fallback for empty FQDN set. Cannot set DNS validation method.")
					return nil, nil, nil, fmt.Errorf("primaryHostedZone is nil and FQDNs empty, cannot set validation")
				}
			} else {
				var fqdnKeys []string
				for k := range allFqdnsForCertSet {
					fqdnKeys = append(fqdnKeys, k)
				}
				m.annotateInfo("Preparing validation map for %d FQDNs: [%s]", len(fqdnKeys), strings.Join(fqdnKeys, ", "))

				for fqdn := range allFqdnsForCertSet {
					// Ensure primaryZone and its ZoneName are not nil before dereferencing.
					// primaryZone is guaranteed not nil if needsMultiZoneValidation was true.
					if primaryZone == nil || primaryZone.ZoneName() == nil || *primaryZone.ZoneName() == "" {
						// This should ideally not be hit if needsMultiZoneValidation was true,
						// as primaryZone is checked when setting needsMultiZoneValidation.
						m.annotateError("Assertion failed: Primary zone or its name is nil within FQDN mapping logic for multi-zone. Skipping FQDN '%s'.", fqdn)
						continue
					}

					mappedToZone := false
					normPZoneName := strings.TrimSuffix(*primaryZone.ZoneName(), ".")
					// Check if FQDN matches primary zone (exact or subdomain)
					if fqdn == normPZoneName || strings.HasSuffix(fqdn, "."+normPZoneName) {
						currentValidationDomains[fqdn] = primaryZone
						m.annotateInfo("Mapping FQDN '%s' to primary zone '%s' for validation.", fqdn, *primaryZone.ZoneName())
						mappedToZone = true
					}

					// Check alternative zone (m.resolvedAltZone and its ZoneName are confirmed not nil here)
					if !mappedToZone {
						normAZoneName := strings.TrimSuffix(*m.resolvedAltZone.ZoneName(), ".")
						if fqdn == normAZoneName || strings.HasSuffix(fqdn, "."+normAZoneName) {
							currentValidationDomains[fqdn] = m.resolvedAltZone
							m.annotateInfo("Mapping FQDN '%s' to alternative zone '%s' for validation.", fqdn, *m.resolvedAltZone.ZoneName())
							mappedToZone = true
						}
					}

					if !mappedToZone {
						m.annotateWarning("FQDN '%s' could not be mapped to primary zone ('%s') or alternative zone ('%s'). Defaulting to primary zone '%s' for validation.",
							fqdn, *primaryZone.ZoneName(), *m.resolvedAltZone.ZoneName(), *primaryZone.ZoneName())
						currentValidationDomains[fqdn] = primaryZone // Default to primary zone
					}
				}

				if len(currentValidationDomains) == 0 {
					// This implies allFqdnsForCertSet was not empty, but nothing got mapped (e.g., primaryZone was nil or malformed).
					m.annotateError("Validation domains map is empty after processing FQDNs, though FQDNs were present. This indicates a problem with zone matching. Falling back to single-zone validation with primary zone '%s'.", safeZoneName(primaryZone))
					if primaryZone != nil {
						validationMethod = awscertificatemanager.CertificateValidation_FromDns(primaryZone)
					} else {
						m.annotateError("PrimaryHostedZone is nil during fallback for empty currentValidationDomains. Cannot set DNS validation method.")
						return nil, nil, nil, fmt.Errorf("primaryHostedZone is nil and validation map empty, cannot set validation")
					}
				} else {
					logMsgParts := []string{}
					for d, z := range currentValidationDomains {
						if z != nil && z.ZoneName() != nil {
							logMsgParts = append(logMsgParts, fmt.Sprintf("'%s' -> '%s'", d, *z.ZoneName()))
						} else {
							logMsgParts = append(logMsgParts, fmt.Sprintf("'%s' -> [nil zone]", d))
						}
					}
					m.annotateInfo("Using multi-zone DNS validation with %d FQDN-to-Zone mapping(s): [%s]", len(logMsgParts), strings.Join(logMsgParts, "; "))
					validationMethod = awscertificatemanager.CertificateValidation_FromDnsMultiZone(&currentValidationDomains)
				}
			}
		} else { // m.resolvedAltZone is nil after lookup attempt
			m.annotateError("Failed to resolve the alternative hosted zone named '%s'. Falling back to single-zone validation with primary zone '%s'. Certificate SANs intended for the alternative zone may not validate.", altZoneName, safeZoneName(primaryZone))
			if primaryZone != nil {
				validationMethod = awscertificatemanager.CertificateValidation_FromDns(primaryZone)
			} else {
				m.annotateError("PrimaryHostedZone is nil during fallback from failed alt zone resolution. Cannot set DNS validation method.")
				return nil, nil, nil, fmt.Errorf("primaryHostedZone is nil and alt zone resolution failed, cannot set validation")
			}
		}
	}
	// The 'else' case for 'if needsMultiZoneValidation' (i.e., single-zone validation)
	// is handled by the logic that sets needsMultiZoneValidation and its associated 'else' block above.
	// That 'else' block already sets: validationMethod = awscertificatemanager.CertificateValidation_FromDns(primaryZone)

	if validationMethod == nil {
		// This is a safeguard. If validationMethod hasn't been set by this point,
		// it indicates a logic flaw or an unhandled edge case.
		m.annotateError("ValidationMethod is unexpectedly nil after all logic paths. This should not happen. Defaulting to error to prevent deployment with invalid cert config.")
		return nil, nil, nil, fmt.Errorf("certificate validation method could not be determined due to an internal logic error")
	}

	return certificateDomainName, allSubjectAlternativeNames, validationMethod, err
}

// ProvisionAlternativeDomains orchestrates the creation of all AWS resources
// (A-records, API Gateway DomainName, ApiMapping) implied by the alternative-domains.yaml
// configuration for the current stack.
// It uses previously registered DnsTargets and the provided shared certificate.
func (m *AlternativeDomainManager) ProvisionAlternativeDomains(
	sharedCertificate awscertificatemanager.ICertificate,
) error {
	if m.stackConfig == nil || len(m.stackConfig.Alternatives) == 0 {
		m.annotateInfo("No alternative domains configured for stack suffix '%s'. Skipping A record provisioning.", m.stackSuffix)
		return nil
	}

	altZoneName := m.GetAlternativeHostedZoneDomain()
	if altZoneName == "" {
		errMsg := fmt.Sprintf("AlternativeHostedZoneDomain is not defined in config for stack suffix '%s' and no override provided. Cannot provision alternative A records.", m.stackSuffix)
		m.annotateError(errMsg)
		return fmt.Errorf(errMsg)
	}

	m.annotateInfo("[ADM Phase 3/3] Looking up alternative zone '%s' and creating A records.", altZoneName)

	// Ensure the alternative zone is resolved.
	// lookupAlternativeZone sets m.resolvedAltZone and logs if it fails.
	m.lookupAlternativeZone(altZoneName)
	if m.resolvedAltZone == nil { // Check if lookup failed
		// Error already logged by lookupAlternativeZone
		return fmt.Errorf("failed to resolve alternative hosted zone '%s', cannot create A records", altZoneName)
	}

	// ... (rest of ProvisionAlternativeDomains - loop and A record creation) ...
	numRecordsCreated := 0
	for altFqdnString, mapping := range m.stackConfig.Alternatives {
		targetID := mapping.TargetComponentId
		registeredTarget, targetExists := m.dnsTargets[targetID]
		if !targetExists {
			m.annotateWarning("Target component ID '%s' (for alternative FQDN '%s') not found in registered DNS targets. Skipping A record.", targetID, altFqdnString)
			continue
		}

		var recordTarget awsroute53.RecordTarget
		targetTypeDisplay := ""
		primaryFqdnMsg := "[PrimaryFQDN not available]"
		if registeredTarget.PrimaryFQDN() != nil {
			primaryFqdnMsg = *registeredTarget.PrimaryFQDN()
		}

		if frontingResult, ok := registeredTarget.(*fronting.FrontingResult); ok {
			if targetID == TargetGateway || targetID == TargetIndexer {
				m.annotateInfo("Provisioning API Gateway alternative domain resources for target '%s' (FQDN: '%s') in zone '%s'.", targetID, altFqdnString, *m.resolvedAltZone.ZoneName())

				if frontingResult.Api == nil {
					m.annotateError("Registered FrontingResult for target '%s' (FQDN: '%s') has a nil API. Cannot create API Gateway alternative domain.", targetID, altFqdnString)
					continue
				}
				if sharedCertificate == nil {
					m.annotateError("Shared certificate is nil. Cannot create API Gateway alternative domain for FQDN '%s'.", altFqdnString)
					continue
				}

				altApiGwDomainNameConstructID := fmt.Sprintf("AltApiGwDomain-%s", strings.ReplaceAll(altFqdnString, ".", "-"))
				altSpecificDomainName := awsapigatewayv2.NewDomainName(m.Construct, jsii.String(altApiGwDomainNameConstructID),
					&awsapigatewayv2.DomainNameProps{
						DomainName:  jsii.String(altFqdnString),
						Certificate: sharedCertificate,
					})

				defaultStage := frontingResult.Api.DefaultStage()
				if defaultStage == nil {
					err := fmt.Errorf("target API for %s (alternative FQDN %s) does not have a default stage for ApiMapping", targetID, altFqdnString)
					m.annotateError(err.Error())
					return err
				}
				apiMappingConstructID := fmt.Sprintf("AltApiMap-%s", strings.ReplaceAll(altFqdnString, ".", "-"))
				awsapigatewayv2.NewApiMapping(m.Construct, jsii.String(apiMappingConstructID),
					&awsapigatewayv2.ApiMappingProps{
						Api:        frontingResult.Api,
						DomainName: altSpecificDomainName,
						Stage:      defaultStage,
					})

				recordTarget = awsroute53.RecordTarget_FromAlias(awsroute53targets.NewApiGatewayv2DomainProperties(altSpecificDomainName.RegionalDomainName(), altSpecificDomainName.RegionalHostedZoneId()))
				targetTypeDisplay = "API Gateway Alias"
			} else {
				m.annotateWarning("Target '%s' (for FQDN '%s') is a FrontingResult but not Gateway/Indexer. Alternative A record will point to its alias target if available.", targetID, altFqdnString)
				recordTarget = registeredTarget.RecordTarget()
				targetTypeDisplay = "Alias"
			}
		} else if _, ok := registeredTarget.(*validator_set.NodeTarget); ok {
			recordTarget = registeredTarget.RecordTarget()
			targetTypeDisplay = "IP Address"
		} else {
			m.annotateWarning("Target component ID '%s' (for FQDN '%s') is of an unknown type. Attempting to use its RecordTarget().", targetID, altFqdnString)
			recordTarget = registeredTarget.RecordTarget()
			targetTypeDisplay = "Unknown (using RecordTarget())"
		}

		if recordTarget == nil {
			m.annotateWarning("Could not determine RecordTarget for '%s' (target: %s). Skipping A record.", altFqdnString, targetID)
			continue
		}

		aRecordConstructID := AlternativeRecordConstructID(altFqdnString)
		awsroute53.NewARecord(m.Construct, jsii.String(aRecordConstructID), &awsroute53.ARecordProps{
			Zone:       m.resolvedAltZone,
			RecordName: jsii.String(altFqdnString),
			Target:     recordTarget,
		})
		numRecordsCreated++
		m.annotateInfo("Created alternative A record: %s -> %s (%s). Primary: %s", altFqdnString, targetID, targetTypeDisplay, primaryFqdnMsg)
	}

	if numRecordsCreated > 0 {
		m.annotateInfo("[ADM Phase 3/3] Finished creating %d alternative A records in zone '%s'.", numRecordsCreated, *m.resolvedAltZone.ZoneName())
	} else {
		m.annotateInfo("[ADM Phase 3/3] No alternative A records were created for zone '%s' (either none configured or targets missing).", *m.resolvedAltZone.ZoneName())
	}
	return nil
}

// lookupAlternativeZone resolves the IHostedZone for the given domain name and stores it in m.resolvedAltZone.
// Stores the result in m.resolvedAltZone or annotates an error if the lookup fails.
func (m *AlternativeDomainManager) lookupAlternativeZone(domainName string) {
	lookupConstructID := "AlternativeHostedZoneLookup" // Static ID for the lookup construct

	// If m.resolvedAltZone is already set, we potentially have a resolved zone.
	if m.resolvedAltZone != nil {
		// Check if it's for the same domain name.
		if m.resolvedAltZone.ZoneName() != nil && *m.resolvedAltZone.ZoneName() == domainName {
			m.annotateInfo("Alternative hosted zone '%s' (ID: %s) already resolved and matches current request. Skipping new lookup.", *m.resolvedAltZone.ZoneName(), *m.resolvedAltZone.HostedZoneId())
			return // Already resolved correctly
		} else {
			// It's resolved, but to a different domain name than currently requested.
			// This indicates a potential logic flaw as the manager assumes one primary alternative zone.
			// We will not attempt to re-lookup with the same static construct ID for a different domain.
			resolvedZoneNameStr := "unknown"
			if m.resolvedAltZone.ZoneName() != nil {
				resolvedZoneNameStr = *m.resolvedAltZone.ZoneName()
			}
			m.annotateError("Previously resolved alternative zone was '%s', but now asked to lookup '%s' using the same internal lookup ID '%s'. The existing resolved zone will be kept. This might indicate a configuration or logic error.",
				resolvedZoneNameStr, domainName, lookupConstructID)
			return // Do not proceed with lookup if already resolved to a different zone.
		}
	}

	// At this point, m.resolvedAltZone is nil, so we can safely perform the lookup.
	m.annotateInfo("Performing lookup for alternative hosted zone: %s using construct ID '%s'", domainName, lookupConstructID)

	m.resolvedAltZone = awsroute53.HostedZone_FromLookup(m.Construct, jsii.String(lookupConstructID), &awsroute53.HostedZoneProviderProps{
		DomainName: jsii.String(domainName),
	})

	// Check if the lookup was successful (i.e., the construct was created and could resolve).
	if m.resolvedAltZone == nil || m.resolvedAltZone.HostedZoneId() == nil {
		m.annotateError("Alternative Hosted Zone lookup construct processed, but resolution may have failed for domain: '%s' (HostedZoneId is nil or construct is nil). Check CDK logs for details.", domainName)
		m.resolvedAltZone = nil // Ensure it's nil if lookup effectively failed
	} else {
		m.annotateInfo("Alternative Hosted Zone '%s' (ID: %s) looked up successfully via construct '%s'.", *m.resolvedAltZone.ZoneName(), *m.resolvedAltZone.HostedZoneId(), lookupConstructID)
	}
}

// --- Annotation Helpers --- //

func (m *AlternativeDomainManager) annotateInfo(format string, args ...interface{}) {
	awscdk.Annotations_Of(m.scope).AddInfo(jsii.Sprintf(format, args...))
}

func (m *AlternativeDomainManager) annotateWarning(format string, args ...interface{}) {
	awscdk.Annotations_Of(m.scope).AddWarning(jsii.Sprintf(format, args...))
}

func (m *AlternativeDomainManager) annotateError(format string, args ...interface{}) {
	awscdk.Annotations_Of(m.scope).AddError(jsii.Sprintf(format, args...))
}

// GetAlternativeHostedZoneDomain returns the configured alternative hosted zone domain name,
// considering any override. Returns an empty string if no domain is configured or found.
func (m *AlternativeDomainManager) GetAlternativeHostedZoneDomain() string {
	// Prioritize override
	if m.props.AlternativeHostedZoneDomainOverride != nil && *m.props.AlternativeHostedZoneDomainOverride != "" {
		return *m.props.AlternativeHostedZoneDomainOverride
	}
	// Then use stack config
	if m.stackConfig != nil && m.stackConfig.AlternativeHostedZoneDomain != "" {
		return m.stackConfig.AlternativeHostedZoneDomain
	}
	return ""
}
