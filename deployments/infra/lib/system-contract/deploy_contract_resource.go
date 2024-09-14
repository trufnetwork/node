package system_contract

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsssm"
	"github.com/aws/aws-cdk-go/awscdklambdagoalpha/v2"
	"github.com/aws/jsii-runtime-go"
)

type DeployContractResourceOptions struct {
	SystemContractPath *string
	PrivateKey         string
	ProviderUrl        *string
}

type SystemContractDeployerOutput struct {
	DeployContractLambdaFn awscdklambdagoalpha.GoFunction
}

func SystemContractDeployer(stack awscdk.Stack, options DeployContractResourceOptions) SystemContractDeployerOutput {
	containerSystemContractPath := jsii.String("/root/system-contract.kf")

	// ## Necessary Resources

	// create privatekey to store in SSM
	// TODO use kms to encrypt the private key, otherwise aws accounts will be able to read it
	ssmPrivateKey := awsssm.NewStringParameter(stack, jsii.String("PrivateKey"), &awsssm.StringParameterProps{
		SimpleName:  jsii.Bool(true),
		StringValue: jsii.String(options.PrivateKey),
	})

	// create s3 asset for system contract
	systemContractAsset := awss3assets.NewAsset(stack, jsii.String("SystemContractAsset"), &awss3assets.AssetProps{
		Path: options.SystemContractPath,
	})

	stackArn := stack.FormatArn(&awscdk.ArnComponents{
		Service:      jsii.String("cloudformation"),
		Resource:     jsii.String("stack"),
		ResourceName: stack.StackName(),
	})

	lambdaFn := DeployContractLambdaFn(stack, DeployContractLambdaFnOptions{
		HostSystemContractPath:      options.SystemContractPath,
		ContainerSystemContractPath: containerSystemContractPath,
		PrivateKeySSMId:             ssmPrivateKey.ParameterName(),
		ProviderUrl:                 options.ProviderUrl,
		SystemContractBucket:        systemContractAsset.Bucket().BucketName(),
		SystemContractKey:           systemContractAsset.S3ObjectKey(),
	})

	// ## Lambda permissions

	// give the lambda permission to read the private key
	ssmPrivateKey.GrantRead(lambdaFn)

	// permit reading
	systemContractAsset.GrantRead(lambdaFn)

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

	return SystemContractDeployerOutput{
		DeployContractLambdaFn: lambdaFn,
	}
}
