package alternativedomainmanager

import (
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	infraCfg "github.com/trufnetwork/node/infra/config" // Import infra config for new accessor
	altcfg "github.com/trufnetwork/node/infra/config/alternativedomains"
	"github.com/trufnetwork/node/infra/lib/constructs/fronting"
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
	// CertSanBuilder is a required reference to the SanListBuilder that collects SANs
	// for the shared TLS certificate used by fronting components (e.g., API Gateway).
	// The manager will add alternative FQDNs to this builder if configured.
	CertSanBuilder *SanListBuilder
	// AlternativeHostedZoneDomainOverride optionally specifies a hosted zone domain name
	// to use instead of the one defined in the loaded configuration file.
	// Useful for testing or specific deployment scenarios.
	AlternativeHostedZoneDomainOverride *string
}

// AlternativeDomainManager is a CDK construct responsible for orchestrating the creation
// of alternative domain name resources based on a YAML configuration file.
// It handles loading the config, collecting required TLS SANs, registering target resources
// (like Nodes, Gateways), and creating Route 53 A records in a designated hosted zone.
type AlternativeDomainManager struct {
	constructs.Construct
	scope           constructs.Construct // The parent CDK scope (usually the Stack) for annotations and context access.
	props           *AlternativeDomainManagerProps
	configFilePath  string                        // Path to the config file, resolved from context or default.
	stackSuffix     string                        // Deployment suffix (e.g., "prod"), resolved from context.
	stackConfig     *altcfg.StackSuffixConfig     // Loaded configuration specific to the current stackSuffix.
	dnsTargets      map[string]fronting.DnsTarget // Registry of components (Nodes, Gateway, etc.) identified by logical ID.
	resolvedAltZone awsroute53.IHostedZone        // The looked-up Route 53 hosted zone for creating alternative A records.
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

	config, err := altcfg.LoadConfig(m.configFilePath)
	if err != nil {
		m.annotateWarning("Failed to load alternative domains config from '%s': %s. Skipping setup.", m.configFilePath, err.Error())
		return
	}
	if config == nil {
		m.annotateInfo("Alternative domains config file '%s' not found or empty. Skipping setup.", m.configFilePath)
		return
	}

	if m.stackSuffix == "" {
		// This case should ideally not happen if StackSuffix() has a default
		m.annotateWarning("StackSuffix (from context or default) is empty. Cannot determine alternative domain config. Skipping setup.")
		return
	}

	if stackCfg, ok := (*config)[m.stackSuffix]; ok {
		m.stackConfig = &stackCfg
		m.annotateInfo("Loaded alternative domain configuration for stack suffix: %s (from file: %s)", m.stackSuffix, m.configFilePath)
	} else {
		m.annotateInfo("No alternative domain configuration found for stack suffix: '%s' in file '%s'. Skipping setup.", m.stackSuffix, m.configFilePath)
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
	if _, exists := m.dnsTargets[id]; exists {
		m.annotateWarning("Target ID '%s' is already registered. Overwriting.", id)
	}
	m.dnsTargets[id] = target
	m.annotateInfo("Registered alternative domain target: %s -> %s", id, *target.PrimaryFQDN())
}

// Bind finalizes the alternative domain setup.
// It should be called after all DnsTarget resources have been registered and
// after CollectAndAddConfiguredSansToBuilder has been called by the stack.
// This method now primarily focuses on creating the Route 53 A records.
func (m *AlternativeDomainManager) Bind() {
	if m.stackConfig == nil {
		// Config wasn't loaded or applicable, nothing to do.
		return
	}

	// SANs are now added by the stack calling CollectAndAddConfiguredSansToBuilder directly.
	// 1. Add required SANs --- This call is removed.
	// m.addSansToBuilder()

	// 2. Create A Records
	m.createARecords()
}

// CollectAndAddConfiguredSansToBuilder iterates through the loaded configuration and adds FQDNs marked
// with `requiresTlsSan: true` to the CertSanBuilder provided in props.
// It only adds SANs for targets that have been successfully registered.
// This method should be called by the stack *before* the certificate is created.
func (m *AlternativeDomainManager) CollectAndAddConfiguredSansToBuilder() {
	if m.props.CertSanBuilder == nil {
		// No builder provided, maybe SANs aren't needed or handled elsewhere.
		// This might be a valid scenario if no SANs are expected from this manager.
		m.annotateInfo("CertSanBuilder is nil in CollectAndAddConfiguredSansToBuilder. No SANs will be added by AlternativeDomainManager.")
		return
	}

	if m.stackConfig == nil {
		m.annotateInfo("stackConfig is nil in CollectAndAddConfiguredSansToBuilder. No alternative domains configured or loaded for this stack suffix. No SANs will be added.")
		return
	}

	for altFqdn, mapping := range m.stackConfig.Alternatives {
		if mapping.RequiresTlsSanOrDefault() {
			// Check if the target component actually exists before adding SAN
			if _, found := m.dnsTargets[mapping.TargetComponentId]; found {
				m.annotateInfo("Adding SAN '%s' for target '%s' to certificate builder.", altFqdn, mapping.TargetComponentId)
				m.props.CertSanBuilder.Add(jsii.String(altFqdn))
			} else {
				m.annotateWarning("TargetComponentId '%s' for SAN '%s' not found in registered targets. Skipping SAN addition.", mapping.TargetComponentId, altFqdn)
			}
		}
	}
}

// createARecords performs the Route 53 operations:
// 1. Determines the target alternative hosted zone domain (using override if provided).
// 2. Looks up the IHostedZone corresponding to that domain name.
// 3. Iterates through the configured alternatives.
// 4. For each alternative, finds the registered DnsTarget.
// 5. Creates an A record in the alternative zone pointing to the DnsTarget.
func (m *AlternativeDomainManager) createARecords() {
	altZoneDomain := m.stackConfig.AlternativeHostedZoneDomain
	if m.props.AlternativeHostedZoneDomainOverride != nil && *m.props.AlternativeHostedZoneDomainOverride != "" {
		altZoneDomain = *m.props.AlternativeHostedZoneDomainOverride
		m.annotateInfo("Using overridden alternative hosted zone domain: %s", altZoneDomain)
	}

	if altZoneDomain == "" {
		m.annotateWarning("Alternative domain configuration for suffix '%s' is missing 'alternativeHostedZoneDomain'. Skipping A record creation.", m.stackSuffix)
		return
	}

	m.lookupAlternativeZone(altZoneDomain)
	if m.resolvedAltZone == nil {
		// Error already annotated during lookup
		return
	}

	for altFqdn, mapping := range m.stackConfig.Alternatives {
		target, found := m.dnsTargets[mapping.TargetComponentId]
		if !found {
			m.annotateWarning("Alternative domain targetComponentId '%s' not found in registered targets for FQDN '%s'. Skipping A record creation.", mapping.TargetComponentId, altFqdn)
			continue
		}

		// Create the A record in the alternative zone
		recordConstructId := AlternativeRecordConstructID(altFqdn)
		route53Record := awsroute53.NewARecord(m.Construct, jsii.String(recordConstructId), &awsroute53.ARecordProps{
			Zone:       m.resolvedAltZone,
			RecordName: jsii.String(altFqdn),  // The full alternative FQDN
			Target:     target.RecordTarget(), // Use the DnsTarget interface method
			// Ttl: Use default TTL
		})

		// Add descriptive output/annotation
		primaryTargetDesc := "IP Address"
		// Check if the target record is an Alias record via the RecordTarget interface.
		concreteTarget := target.RecordTarget()
		if concreteTarget.AliasTarget() != nil {
			primaryTargetDesc = "Alias Target"
		}
		m.annotateInfo("Created alternative A record: %s -> %s (Primary: %s, TargetType: %s) [Resource: %s]",
			altFqdn, mapping.TargetComponentId, *target.PrimaryFQDN(), primaryTargetDesc, *route53Record.Node().Path())
	}
}

// lookupAlternativeZone performs the CDK lookup for the hosted zone specified by domainName.
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
