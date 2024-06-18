package system_contract

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/customresources"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type DeployContractResourceOptions struct {
	DeployContractLambdaFnOptions
	PrivateKey  string
	ProviderUrl *string
	// to decide when to deploy again
	Hash *string
}

func DeployContractResource(scope constructs.Construct, options DeployContractResourceOptions) awscdk.CustomResource {
	lambdaFn := DeployContractLambdaFn(scope, options.DeployContractLambdaFnOptions)
	// Define Lambda function as a custom resource provider
	provider := customresources.NewProvider(scope, jsii.String("CustomResourceProvider"), &customresources.ProviderProps{
		OnEventHandler: lambdaFn,
		TotalTimeout:   awscdk.Duration_Minutes(jsii.Number(15)),
	})

	// Create custom resource
	return awscdk.NewCustomResource(scope, jsii.String("DeployContractResource"), &awscdk.CustomResourceProps{
		ServiceToken: provider.ServiceToken(),
		Properties: &map[string]interface{}{
			"PrivateKey":         options.PrivateKey,
			"ProviderUrl":        options.ProviderUrl,
			"SystemContractPath": options.SystemContractPath,
		},
	})
}
