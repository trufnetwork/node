package config

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type CDKParams struct {
	CorsAllowOrigins awscdk.CfnParameter
	SessionSecret    awscdk.CfnParameter
}

func NewCDKParams(scope constructs.Construct) CDKParams {
	corsAllowOrigins := awscdk.NewCfnParameter(scope, jsii.String("corsAllowOrigins"), &awscdk.CfnParameterProps{
		Type:        jsii.String("String"),
		Description: jsii.String("CORS allow origins"),
		Default:     jsii.String("*"),
	})

	sessionSecret := awscdk.NewCfnParameter(scope, jsii.String("sessionSecret"), &awscdk.CfnParameterProps{
		Type:        jsii.String("String"),
		Description: jsii.String("Kwil Gateway session secret"),
		NoEcho:      jsii.Bool(true),
	})

	return CDKParams{
		CorsAllowOrigins: corsAllowOrigins,
		SessionSecret:    sessionSecret,
	}
}
