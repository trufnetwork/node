package system_contract

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdklambdagoalpha/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type DeployContractLambdaFnOptions struct {
	SystemContractPath *string
}

func DeployContractLambdaFn(scope constructs.Construct, options DeployContractLambdaFnOptions) awscdklambdagoalpha.GoFunction {
	return awscdklambdagoalpha.NewGoFunction(scope, jsii.String("GoFunction"), &awscdklambdagoalpha.GoFunctionProps{
		Entry: jsii.String("../cmd/init-system/main.go"),
		Bundling: &awscdklambdagoalpha.BundlingOptions{
			GoBuildFlags: &[]*string{
				jsii.String("-ldflags \"-s -w\""),
			},
			// 	Volumes *[]*DockerVolume `field:"optional" json:"volumes" yaml:"volumes"`
			Volumes: &[]*awscdk.DockerVolume{
				{
					ContainerPath: options.SystemContractPath,
					HostPath:      options.SystemContractPath,
				},
			},
		},
	})
}
