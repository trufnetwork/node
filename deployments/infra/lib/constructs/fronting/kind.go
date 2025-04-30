package fronting

import "fmt"

// Kind represents the type of fronting implementation.
type Kind string

const (
	KindAPI        Kind = "api"
	KindCloudFront Kind = "cloudfront"
	KindALB        Kind = "alb"
)

// ParseKind converts a raw string into a Kind, returning an error for invalid values.
func ParseKind(s string) (Kind, error) {
	s = string(Kind(s))
	switch Kind(s) {
	case KindAPI, KindCloudFront, KindALB:
		return Kind(s), nil
	default:
		return "", fmt.Errorf("invalid fronting type %q", s)
	}
}

// New returns a Fronting implementation for the given Kind.
func New(kind Kind) Fronting {
	switch kind {
	case KindAPI:
		return NewApiGatewayFronting()
	case KindCloudFront:
		return NewCloudFrontFronting()
	case KindALB:
		return NewAlbFronting()
	default:
		// ParseKind should prevent this, but panic as a safeguard
		panic(fmt.Sprintf("unsupported fronting kind %q", kind))
	}
}
