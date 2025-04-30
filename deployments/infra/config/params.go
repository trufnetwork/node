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
)

type CDKParams struct {
	Stage            awscdk.CfnParameter
	DevPrefix        awscdk.CfnParameter
	CorsAllowOrigins awscdk.CfnParameter
}

func NewCDKParams(scope constructs.Construct) CDKParams {

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

	params := CDKParams{
		Stage:            stageParam,
		DevPrefix:        devPrefixParam,
		CorsAllowOrigins: corsAllowOrigins,
	}

	return params
}
