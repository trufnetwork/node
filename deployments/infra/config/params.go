package config

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// Constants for CDK parameter names
const (
	CorsParamName = "corsAllowOrigins"
)

type CDKParams struct {
	CorsAllowOrigins awscdk.CfnParameter
}

func NewCDKParams(scope constructs.Construct) CDKParams {
	// Create CORS allow origins parameter
	corsAllowOrigins := awscdk.NewCfnParameter(scope, jsii.String(CorsParamName), &awscdk.CfnParameterProps{
		Type:        jsii.String("String"),
		Description: jsii.String("CORS allow origins"),
		Default:     jsii.String("*"),
	})

	params := CDKParams{
		CorsAllowOrigins: corsAllowOrigins,
	}

	return params
}
