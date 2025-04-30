package domain

import (
	"strings"

	jsii "github.com/aws/jsii-runtime-go"
)

// MainDomain is the fixed base domain; dev deployments prepend Spec.DevPrefix before this label
const MainDomain = "infra.truf.network"

// StageType defines allowed deployment stages.
type StageType string

const (
	// StageProd is the production stage
	StageProd StageType = "prod"
	// StageDev is the development stage
	StageDev StageType = "dev"
)

// Spec encapsulates the stage, optional leaf subdomain, and (for dev) mandatory DevPrefix.
// It builds FQDNs by prepending labels before the fixed MainDomain.
type Spec struct {
	Stage     StageType
	Sub       string // optional leaf subdomain
	DevPrefix string // required when Stage==StageDev
}

// root always returns the fixed base domain
func (s Spec) root() string {
	return MainDomain
}

// fqdnParts returns labels in order: Sub (if any), DevPrefix (dev only), root
func (s Spec) fqdnParts() []string {
	// Ensure prod does not carry a DevPrefix
	if s.Stage == StageProd && s.DevPrefix != "" {
		panic("DevPrefix must be empty for prod stages")
	}
	parts := []string{}
	if s.Sub != "" {
		parts = append(parts, s.Sub)
	}
	if s.Stage == StageDev {
		// Dev requires a DevPrefix label
		if s.DevPrefix == "" {
			panic("dev deployments must set Spec.DevPrefix")
		}
		parts = append(parts, s.DevPrefix)
	}
	parts = append(parts, s.root())
	return parts
}

// FQDN returns the fully-qualified domain by joining fqdnParts with a dot.
func (s Spec) FQDN() *string {
	return jsii.String(strings.Join(s.fqdnParts(), "."))
}

// Subdomain returns a fully-qualified subdomain for the given label.
// It prepends the label to the Spec's FQDN parts, e.g., "gateway.dev.infra…".
func (s Spec) Subdomain(label string) *string {
	parts := append([]string{label}, s.fqdnParts()...)
	return jsii.String(strings.Join(parts, "."))
}
