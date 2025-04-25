package config

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config/domain"
)

// Constants for CDK parameter names
const (
	StageParamName     = "stage"
	DevPrefixParamName = "devPrefix"
	CorsParamName      = "corsAllowOrigins"
	SessionParamName   = "sessionSecret"
)

type CDKParams struct {
	Stage            awscdk.CfnParameter
	DevPrefix        awscdk.CfnParameter
	CorsAllowOrigins awscdk.CfnParameter
	SessionSecret    awscdk.CfnParameter
}

// paramsCache stores CDKParams per construct path to avoid duplicate parameters
var paramsCache = make(map[string]CDKParams)

func NewCDKParams(scope constructs.Construct) CDKParams {
	// Use construct path as cache key
	path := *scope.Node().Path()
	if existing, found := paramsCache[path]; found {
		return existing
	}

	// Create stage parameter
	stageParam := awscdk.NewCfnParameter(scope, jsii.String(StageParamName), &awscdk.CfnParameterProps{
		Type:          jsii.String("String"),
		Description:   jsii.String("Deployment stage ('prod' or 'dev')"),
		AllowedValues: jsii.Strings(string(domain.StageProd), string(domain.StageDev)), // Reference StageType constants
	})

	// Create dev prefix parameter
	devPrefixParam := awscdk.NewCfnParameter(scope, jsii.String(DevPrefixParamName), &awscdk.CfnParameterProps{
		Type:                  jsii.String("String"),
		Description:           jsii.String("Dev prefix for 'dev' stage (mandatory for dev, empty for prod)"),
		Default:               jsii.String(""),
		AllowedPattern:        jsii.String("^[a-zA-Z0-9-]*$"),
		MinLength:             jsii.Number(0),
		ConstraintDescription: jsii.String("DevPrefix must be alphanumeric and may include hyphens (0 or more characters)."),
	})

	// Create CORS allow origins parameter
	corsAllowOrigins := awscdk.NewCfnParameter(scope, jsii.String(CorsParamName), &awscdk.CfnParameterProps{
		Type:        jsii.String("String"),
		Description: jsii.String("CORS allow origins"),
		Default:     jsii.String("*"),
	})

	// Create session secret parameter
	sessionSecret := awscdk.NewCfnParameter(scope, jsii.String(SessionParamName), &awscdk.CfnParameterProps{
		Type:        jsii.String("String"),
		Description: jsii.String("Kwil Gateway session secret"),
		NoEcho:      jsii.Bool(true),
	})

	params := CDKParams{
		Stage:            stageParam,
		DevPrefix:        devPrefixParam,
		CorsAllowOrigins: corsAllowOrigins,
		SessionSecret:    sessionSecret,
	}

	// Store in cache
	paramsCache[path] = params

	return params
}
