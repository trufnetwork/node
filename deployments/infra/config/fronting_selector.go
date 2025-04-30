package config

import (
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
)

// FrontingSelector creates and parses the frontingType CFN parameter.
type FrontingSelector struct {
	Kind fronting.Kind
}

// NewFrontingSelector defines a CFN parameter 'frontingType' with allowed values,
// reads the context (if supplied) to set the *default*, and parses it into a Kind.
func NewFrontingSelector(scope constructs.Construct) FrontingSelector {
	// 1.  Calculate the default value
	def := string(fronting.KindAPI) // fallback
	if ctx := scope.Node().TryGetContext(jsii.String("frontingType")); ctx != nil {
		if s, ok := ctx.(string); ok && s != "" {
			def = strings.ToLower(s) // normalise, ParseKind expects lower-case
		}
	}

	// 2.  Declare the parameter
	param := awscdk.NewCfnParameter(scope, jsii.String("frontingType"), &awscdk.CfnParameterProps{
		Type:          jsii.String("String"),
		Default:       jsii.String(def),
		AllowedValues: jsii.Strings(string(fronting.KindAPI), string(fronting.KindCloudFront), string(fronting.KindALB)),
		Description:   jsii.String("Type of edge proxy to expose (api | cloudfront | alb)"),
	})

	// 3.  Parse & validate
	val := param.ValueAsString()
	return FrontingSelector{Kind: fronting.Kind(*val)}
}
