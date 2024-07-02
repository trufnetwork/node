package system_contract

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsssm"
	"github.com/aws/aws-cdk-go/awscdk/v2/customresources"
	"github.com/aws/jsii-runtime-go"
)

type DeployContractResourceOptions struct {
	SystemContractPath *string
	PrivateKey         string
	ProviderUrl        *string
	// to decide when to deploy again
	Hash *string
}

func DeployContractResource(stack awscdk.Stack, options DeployContractResourceOptions) awscdk.CustomResource {
	containerSystemContractPath := jsii.String("/root/system-contract.kf")

	lambdaFn := DeployContractLambdaFn(stack, DeployContractLambdaFnOptions{
		HostSystemContractPath:      options.SystemContractPath,
		ContainerSystemContractPath: containerSystemContractPath,
	})

	// Define Lambda function as a custom resource provider
	provider := customresources.NewProvider(stack, jsii.String("CustomResourceProvider"), &customresources.ProviderProps{
		OnEventHandler: lambdaFn,
		//TotalTimeout:   awscdk.Duration_Minutes(jsii.Number(15)),
	})

	// create privatekey to store in SSM
	// TODO use kms to encrypt the private key, otherwise aws accounts will be able to read it
	ssmPrivateKey := awsssm.NewStringParameter(stack, jsii.String("PrivateKey"), &awsssm.StringParameterProps{
		SimpleName:  jsii.Bool(true),
		StringValue: jsii.String(options.PrivateKey),
	})

	// give the lambda permission to read the private key
	ssmPrivateKey.GrantRead(lambdaFn)

	// create s3 asset for system contract
	systemContractAsset := awss3assets.NewAsset(stack, jsii.String("SystemContractAsset"), &awss3assets.AssetProps{
		Path: options.SystemContractPath,
	})

	// permit reading
	systemContractAsset.GrantRead(lambdaFn)

	stackArn := stack.FormatArn(&awscdk.ArnComponents{
		Service:      jsii.String("cloudformation"),
		Resource:     jsii.String("stack"),
		ResourceName: stack.StackName(),
	})

	// permit lambda to describe the stack events from this stack
	lambdaFn.Role().AttachInlinePolicy(awsiam.NewPolicy(stack, jsii.String("DescribeStackEventsPolicy"), &awsiam.PolicyProps{
		Statements: &[]awsiam.PolicyStatement{awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions: jsii.Strings("cloudformation:DescribeStackEvents"),
			Effect:  awsiam.Effect_ALLOW,
			Resources: jsii.Strings(
				*stackArn,
				*stackArn+"/*",
			),
		})},
	}))

	// Create custom resource
	return awscdk.NewCustomResource(stack, jsii.String("DeployContractResource"), &awscdk.CustomResourceProps{
		ServiceToken: provider.ServiceToken(),
		// Properties to pass to the Lambda
		// if any of these keys change, we need to update the lambda code
		Properties: &map[string]interface{}{
			// the intention of UpdateHash is just updating some of the custom resource
			// properties. Then we'll be triggering an resource Update event every time
			// the hash changes.
			"UpdateHash":           options.Hash,
			"PrivateKeySSMId":      ssmPrivateKey.ParameterName(),
			"ProviderUrl":          options.ProviderUrl,
			"SystemContractBucket": systemContractAsset.Bucket().BucketName(),
			"SystemContractKey":    systemContractAsset.S3ObjectKey(),
		},
	})
}
