package config

import (
	"fmt"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
	"github.com/trufnetwork/node/infra/lib/constructs/fronting"
)

const frontingTypeCtxKey = "frontingType"

// GetFrontingKind reads "frontingType" from CDK context *at synth time*.
// • Absence → default "api"
// • Bad value → panic with a clear message.
func GetFrontingKind(scope constructs.Construct) fronting.Kind {
	raw := scope.Node().TryGetContext(jsii.String(frontingTypeCtxKey))
	var kindStr string
	if raw == nil {
		kindStr = string(fronting.KindAPI)
		cdklogger.LogInfo(scope, "", "Fronting type not specified in CDK context ('%s'), using default: '%s'", frontingTypeCtxKey, kindStr)
	} else {
		var ok bool
		kindStr, ok = raw.(string)
		if !ok {
			panic(fmt.Sprintf("context '%s' must be a string, got %T", frontingTypeCtxKey, raw))
		}
		cdklogger.LogInfo(scope, "", "Selected fronting type: '%s' (from CDK context '%s')", kindStr, frontingTypeCtxKey)
	}

	kind, err := fronting.ParseKind(kindStr)
	if err != nil {
		panic(fmt.Errorf("invalid %s='%s' – allowed: api | alb | cloudfront. Error: %w", frontingTypeCtxKey, kindStr, err))
	}
	return kind
}
