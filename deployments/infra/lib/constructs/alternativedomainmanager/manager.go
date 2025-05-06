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
	// Load configuration immediately upon creation based on context.
	mgr.loadConfig()
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
// It collects SANs from the alternative domain configuration and any additional explicit SANs,
// determines the primary domain for the certificate, and figures out the correct
// DNS validation method (single or multi-zone).
func (m *AlternativeDomainManager) GetCertificateRequirements(
	primaryZone awsroute53.IHostedZone, // The stack's primary zone
	additionalExplicitSans ...*string, // FQDNs the stack explicitly wants on the cert
) (
	certificateDomainName *string, // The suggested primary domain for the certificate
	allSubjectAlternativeNames []*string, // The complete, deduplicated, sorted list of SANs
	validationMethod awscertificatemanager.CertificateValidation, // The appropriate validation
	err error,
) {
	if m.sanBuilder == nil { // Should be initialized in NewAlternativeDomainManager
		return nil, nil, nil, fmt.Errorf("internal SanListBuilder not initialized")
	}
	m.sanBuilder = NewSanListBuilder() // Clear any previous state, ensure fresh build

	// 1. Collect SANs from alternative domain configuration
	m.collectAndAddConfiguredSansToBuilderInternal()

	// 2. Add any explicitly provided SANs from the stack
	m.sanBuilder.Add(additionalExplicitSans...)

	allSans := m.sanBuilder.List()
	if len(allSans) == 0 {
		// No SANs means no certificate is strictly needed based on inputs.
		// However, a primary FQDN might still be provided for a default cert.
		// If additionalExplicitSans has one entry, use it. Otherwise, it's an issue.
		if len(additionalExplicitSans) == 1 && additionalExplicitSans[0] != nil && *additionalExplicitSans[0] != "" {
			m.annotateInfo("No SANs from config, using the single explicit FQDN as certificate domain name.")
			certificateDomainName = additionalExplicitSans[0]
			// For a single domain cert with no explicit SANs from config, validation is simple.
			validationMethod = awscertificatemanager.CertificateValidation_FromDns(primaryZone)
			return certificateDomainName, nil, validationMethod, nil // No SANs to return in allSubjectAlternativeNames
		}
		// If no explicit SANs and no config SANs, this is likely an issue or unhandled case.
		// Stack might still create a cert for a primary non-alternative domain.
		// For now, let's assume at least one primary FQDN is always given if a cert is made.
		m.annotateWarning("No SANs collected from config and no/multiple explicit SANs provided to GetCertificateRequirements. Certificate setup might be problematic.")
		// Default to primaryZone validation if no other info.
		// The primary domain for the cert would need to be determined by the caller if no SANs.
		// This path should ideally not be hit if the stack intends to create a cert.
		// If no explicit SANs are provided, the caller of this method must provide a primaryDomainName for the certificate.
		if len(additionalExplicitSans) > 0 && additionalExplicitSans[0] != nil {
			certificateDomainName = additionalExplicitSans[0] // Pick first explicit as a guess
		}
		validationMethod = awscertificatemanager.CertificateValidation_FromDns(primaryZone)
		return certificateDomainName, nil, validationMethod, nil // No SANs
	}

	// 3. Determine the certificate's primary DomainName.
	// Pick the first FQDN from the explicit list if available, otherwise first from all SANs.
	// This matches common behavior for NewCertificate.
	if len(additionalExplicitSans) > 0 && additionalExplicitSans[0] != nil && *additionalExplicitSans[0] != "" {
		certificateDomainName = additionalExplicitSans[0]
	} else {
		certificateDomainName = allSans[0] // First from the sorted, deduplicated list
	}
	m.annotateInfo("Selected '%s' as primary DomainName for the shared certificate.", *certificateDomainName)

	// 4. Determine DNS Validation method (single or multi-zone)
	// This requires checking if any SANs belong to the alternative hosted zone,
	// and if that zone is different from the primaryZone.

	altHostedZoneDomainConfigured := ""
	if m.stackConfig != nil && m.stackConfig.AlternativeHostedZoneDomain != "" {
		altHostedZoneDomainConfigured = strings.TrimSuffix(m.stackConfig.AlternativeHostedZoneDomain, ".")
	}
	if m.props.AlternativeHostedZoneDomainOverride != nil && *m.props.AlternativeHostedZoneDomainOverride != "" {
		altHostedZoneDomainConfigured = strings.TrimSuffix(*m.props.AlternativeHostedZoneDomainOverride, ".")
		m.annotateInfo("Using overridden alternative hosted zone domain for validation check: %s", altHostedZoneDomainConfigured)
	}

	primaryZoneName := ""
	if primaryZone != nil && primaryZone.ZoneName() != nil {
		primaryZoneName = strings.TrimSuffix(*primaryZone.ZoneName(), ".")
	}

	needsMultiZoneValidation := false
	var alternativeHostedZone awsroute53.IHostedZone // Will be looked up if needed

	if altHostedZoneDomainConfigured != "" && altHostedZoneDomainConfigured != primaryZoneName {
		// Potential for multi-zone. Check if any SANs actually fall into this alternative zone.
		for _, sanPtr := range allSans {
			san := *sanPtr
			// A simple suffix check is usually sufficient for determining if a domain belongs to a zone.
			if strings.HasSuffix(san, "."+altHostedZoneDomainConfigured) || san == altHostedZoneDomainConfigured {
				needsMultiZoneValidation = true
				m.annotateInfo("SAN '%s' requires validation in alternative zone '%s'. Multi-zone validation likely needed.", san, altHostedZoneDomainConfigured)
				break
			}
		}
	}

	if needsMultiZoneValidation {
		m.annotateInfo("Multi-zone certificate validation is required. Primary zone: '%s', Alternative zone: '%s'.", primaryZoneName, altHostedZoneDomainConfigured)
		// Lookup the alternative hosted zone construct
		// Note: Re-lookup for safety, even if m.resolvedAltZone might exist from a previous Provision call.
		// This method should be self-contained for cert requirements.
		altZoneLookupId := fmt.Sprintf("AltHZLookupForCert-%s", strings.ReplaceAll(altHostedZoneDomainConfigured, ".", "-"))
		alternativeHostedZone = awsroute53.HostedZone_FromLookup(m.scope, jsii.String(altZoneLookupId),
			&awsroute53.HostedZoneProviderProps{DomainName: jsii.String(altHostedZoneDomainConfigured)})

		if alternativeHostedZone == nil || alternativeHostedZone.HostedZoneId() == nil { // Basic check for successful lookup
			return nil, nil, nil, fmt.Errorf("failed to lookup alternative hosted zone '%s' for multi-zone certificate validation", altHostedZoneDomainConfigured)
		}

		// Build the map for multi-zone validation
		domainZoneMap := make(map[string]awsroute53.IHostedZone)
		for _, sanPtr := range allSans {
			san := *sanPtr
			if strings.HasSuffix(san, "."+altHostedZoneDomainConfigured) || san == altHostedZoneDomainConfigured {
				domainZoneMap[san] = alternativeHostedZone
			} else if primaryZone != nil { // Assume others fall into the primary zone
				domainZoneMap[san] = primaryZone
			} else {
				// This case is problematic: SAN doesn't fit alt zone, and no primary zone provided.
				return nil, nil, nil, fmt.Errorf("cannot determine validation zone for SAN '%s'; no primary zone and not in alternative zone '%s'", san, altHostedZoneDomainConfigured)
			}
		}
		validationMethod = awscertificatemanager.CertificateValidation_FromDnsMultiZone(&domainZoneMap)
	} else {
		m.annotateInfo("Single-zone certificate validation will be used in primary zone: '%s'.", primaryZoneName)
		if primaryZone == nil {
			return nil, nil, nil, fmt.Errorf("primaryZone is required for single-zone certificate validation but was nil")
		}
		validationMethod = awscertificatemanager.CertificateValidation_FromDns(primaryZone)
	}

	return certificateDomainName, allSans, validationMethod, nil
}

// ProvisionAlternativeDomains orchestrates the creation of all AWS resources
// (A-records, API Gateway DomainName, ApiMapping) implied by the alternative-domains.yaml
// configuration for the current stack.
// It uses previously registered DnsTargets and the provided shared certificate.
func (m *AlternativeDomainManager) ProvisionAlternativeDomains(
	sharedCertificate awscertificatemanager.ICertificate,
) error {
	if m.stackConfig == nil {
		m.annotateInfo("No stack-specific alternative domain configuration loaded. Skipping provisioning.")
		return nil
	}

	// 1. Determine and lookup the alternative hosted zone.
	altZoneDomain := m.stackConfig.AlternativeHostedZoneDomain
	if m.props.AlternativeHostedZoneDomainOverride != nil && *m.props.AlternativeHostedZoneDomainOverride != "" {
		altZoneDomain = *m.props.AlternativeHostedZoneDomainOverride
		m.annotateInfo("Using overridden alternative hosted zone domain for provisioning: %s", altZoneDomain)
	}

	if altZoneDomain == "" {
		m.annotateWarning("Alternative domain configuration for suffix '%s' is missing 'alternativeHostedZoneDomain'. Skipping resource provisioning.", m.stackSuffix)
		return nil // Not an error, but nothing to do.
	}

	// Cache the resolved alternative zone if not already done.
	if m.resolvedAltZone == nil || (m.resolvedAltZone.ZoneName() != nil && strings.TrimSuffix(*m.resolvedAltZone.ZoneName(), ".") != strings.TrimSuffix(altZoneDomain, ".")) {
		m.lookupAlternativeZone(altZoneDomain) // This sets m.resolvedAltZone or logs errors.
	}

	if m.resolvedAltZone == nil {
		// lookupAlternativeZone would have logged an error.
		return fmt.Errorf("failed to resolve alternative hosted zone '%s'; cannot provision resources", altZoneDomain)
	}

	// 2. Iterate through configured alternatives and create resources.
	for altFqdnString, mapping := range m.stackConfig.Alternatives {
		targetID := mapping.TargetComponentId
		registeredTarget, targetFound := m.dnsTargets[targetID]
		if !targetFound {
			m.annotateWarning("No DnsTarget registered for ID '%s' (referenced by alternative FQDN '%s'). Skipping resource creation for this FQDN.", targetID, altFqdnString)
			continue
		}

		// Attempt to determine target type.
		if nodeTarget, ok := registeredTarget.(*validator_set.NodeTarget); ok {
			m.annotateInfo("Provisioning A-record for Node target '%s' (FQDN: '%s') in zone '%s'.", targetID, altFqdnString, *m.resolvedAltZone.ZoneName())
			awsroute53.NewARecord(m.Construct, jsii.String(AlternativeRecordConstructID(altFqdnString)),
				&awsroute53.ARecordProps{
					Zone:       m.resolvedAltZone,
					RecordName: jsii.String(altFqdnString),
					Target:     nodeTarget.RecordTarget(), // Uses EIP from NodeTarget
					Comment:    jsii.String(fmt.Sprintf("Alternative A-record for %s, pointing to %s", altFqdnString, targetID)),
				})
		} else if frontingResult, ok := registeredTarget.(*fronting.FrontingResult); ok {
			// This handles Gateway and Indexer targets if they are registered as FrontingResult.
			if targetID == TargetGateway || targetID == TargetIndexer {
				m.annotateInfo("Provisioning API Gateway alternative domain resources for target '%s' (FQDN: '%s') in zone '%s'.", targetID, altFqdnString, *m.resolvedAltZone.ZoneName())

				if frontingResult.Api == nil {
					m.annotateError("Registered FrontingResult for target '%s' (FQDN: '%s') has a nil API. Cannot create API Gateway alternative domain.", targetID, altFqdnString)
					continue
				}
				if sharedCertificate == nil {
					m.annotateError("Shared certificate is nil. Cannot create API Gateway alternative domain for FQDN '%s'.", altFqdnString)
					continue // Cannot proceed without a certificate
				}

				// Create API Gateway v2 DomainName for the alternative FQDN
				altApiGwDomainNameConstructID := fmt.Sprintf("AltApiGwDomain-%s", strings.ReplaceAll(altFqdnString, ".", "-"))
				altSpecificDomainName := awsapigatewayv2.NewDomainName(m.Construct, jsii.String(altApiGwDomainNameConstructID),
					&awsapigatewayv2.DomainNameProps{
						DomainName:  jsii.String(altFqdnString),
						Certificate: sharedCertificate,
						// EndpointType defaults to REGIONAL.
					})

				// Create ApiMapping to link the new DomainName to the target API
				defaultStage := frontingResult.Api.DefaultStage()
				if defaultStage == nil {
					err := fmt.Errorf("target API for %s (alternative FQDN %s) does not have a default stage for ApiMapping", targetID, altFqdnString)
					m.annotateError(err.Error())
					return err // Fail fast if fundamental setup is missing
				}
				apiMappingConstructID := fmt.Sprintf("AltApiMap-%s", strings.ReplaceAll(altFqdnString, ".", "-"))
				awsapigatewayv2.NewApiMapping(m.Construct, jsii.String(apiMappingConstructID),
					&awsapigatewayv2.ApiMappingProps{
						Api:        frontingResult.Api,
						DomainName: altSpecificDomainName,
						Stage:      defaultStage,
					})

				// Create A-Record in the alternative hosted zone pointing to the new API GW DomainName
				aRecordConstructID := AlternativeRecordConstructID(altFqdnString)
				awsroute53.NewARecord(m.Construct, jsii.String(aRecordConstructID),
					&awsroute53.ARecordProps{
						Zone:       m.resolvedAltZone,
						RecordName: jsii.String(altFqdnString),
						Target: awsroute53.RecordTarget_FromAlias(
							awsroute53targets.NewApiGatewayv2DomainProperties(
								altSpecificDomainName.RegionalDomainName(),
								altSpecificDomainName.RegionalHostedZoneId(),
							),
						),
						Comment: jsii.String(fmt.Sprintf("Alternative A-record for %s, pointing to API GW custom domain for %s", altFqdnString, targetID)),
					})
			} else {
				m.annotateWarning("Registered target '%s' (FQDN: '%s') is a FrontingResult but not recognized as Gateway/Indexer. Skipping API GW specific resource creation.", targetID, altFqdnString)
			}
		} else {
			m.annotateWarning("Registered target '%s' (FQDN: '%s') is of an unknown or unhandled type for provisioning: %T. Skipping resource creation.", targetID, altFqdnString, registeredTarget)
		}
	}

	return nil
}

// lookupAlternativeZone resolves the IHostedZone for the given domain name and stores it in m.resolvedAltZone.
// Stores the result in m.resolvedAltZone or annotates an error if the lookup fails.
func (m *AlternativeDomainManager) lookupAlternativeZone(domainName string) {
	m.annotateInfo("Looking up alternative hosted zone: %s", domainName)
	// Use the manager's construct scope for the lookup so it's part of this construct's resources
	m.resolvedAltZone = awsroute53.HostedZone_FromLookup(m.Construct, jsii.String("AlternativeHostedZoneLookup"), &awsroute53.HostedZoneProviderProps{
		DomainName: jsii.String(domainName),
	})

	// Check if the lookup was successful. The ID will be a token.
	// A simple nil check isn't enough; we rely on CDK's resolution or potential deployment error.
	// However, we add an error annotation if the input domain was clearly invalid/empty previously.
	if m.resolvedAltZone.HostedZoneId() == nil {
		// This condition might be hit if the lookup construct itself fails, though typically CDK errors out later.
		m.annotateError("Alternative Hosted Zone lookup failed unexpectedly for domain: '%s'. Check CDK logs for details.", domainName)
		m.resolvedAltZone = nil // Ensure it's nil if lookup failed
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
