package config

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type CDKParams struct {
	CorsAllowOrigins awscdk.CfnParameter
	SessionSecret    awscdk.CfnParameter
	ChainId          awscdk.CfnParameter
}

func NewCDKParams(scope constructs.Construct) *CDKParams {
	corsAllowOrigins := awscdk.NewCfnParameter(scope, jsii.String("CorsAllowOrigins"), &awscdk.CfnParameterProps{
		Type:        jsii.String("String"),
		Description: jsii.String("CORS allow origins"),
		Default:     jsii.String("*"),
	})

	sessionSecret := awscdk.NewCfnParameter(scope, jsii.String("SessionSecret"), &awscdk.CfnParameterProps{
		Type:        jsii.String("String"),
		Description: jsii.String("Kwil Gateway session secret"),
		NoEcho:      jsii.Bool(true),
	})

	chainId := awscdk.NewCfnParameter(scope, jsii.String("ChainId"), &awscdk.CfnParameterProps{
		Type:        jsii.String("String"),
		Description: jsii.String("Chain ID"),
	})

	return &CDKParams{
		CorsAllowOrigins: corsAllowOrigins,
		SessionSecret:    sessionSecret,
		ChainId:          chainId,
	}
}
