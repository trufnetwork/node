package config

import (
	"fmt"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/constructs/fronting"
)

// GetFrontingKind reads "frontingType" from CDK context *at synth time*.
// • Absence → default "api"
// • Bad value → panic with a clear message.
func GetFrontingKind(scope constructs.Construct) fronting.Kind {
	const ctxKey = "frontingType"

	raw := scope.Node().TryGetContext(jsii.String(ctxKey))
	if raw == nil {
		raw = "api" // sane default
	}
	kindStr, ok := raw.(string)
	if !ok {
		panic(fmt.Sprintf("context %q must be a string, got %T", ctxKey, raw))
	}

	kind, err := fronting.ParseKind(kindStr)
	if err != nil {
		panic(fmt.Errorf("invalid %s=%q – allowed: api | alb | cloudfront", ctxKey, kindStr))
	}
	return kind
}
