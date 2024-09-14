package system_contract

import (
	"os"
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdklambdagoalpha/v2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type DeployContractLambdaFnOptions struct {
	HostSystemContractPath      *string
	ContainerSystemContractPath *string
	PrivateKeySSMId             *string
	ProviderUrl                 *string
	SystemContractBucket        *string
	SystemContractKey           *string
}

func DeployContractLambdaFn(scope constructs.Construct, options DeployContractLambdaFnOptions) awscdklambdagoalpha.GoFunction {
	// get the absolute path of the system contract
	// i.e. it might be ../../system-contract.kf
	absoluteHostSystemContractPath := filepath.Join(os.Getenv("PWD"), *options.HostSystemContractPath)

	return awscdklambdagoalpha.NewGoFunction(scope, jsii.String("GoFunction"), &awscdklambdagoalpha.GoFunctionProps{
		Entry:   jsii.String("../../cmd/init-system/main.go"),
		Timeout: awscdk.Duration_Minutes(jsii.Number(15)),
		Bundling: &awscdklambdagoalpha.BundlingOptions{
			ForcedDockerBundling: jsii.Bool(true),
			// to make it work in CI and local ACT
			BundlingFileAccess: awscdk.BundlingFileAccess_VOLUME_COPY,
			GoBuildFlags: &[]*string{
				jsii.String("-ldflags \"-s -w\""),
			},
			Volumes: &[]*awscdk.DockerVolume{
				{
					ContainerPath: options.ContainerSystemContractPath,
					HostPath:      jsii.String(absoluteHostSystemContractPath),
				},
			},
		},
		Environment: &map[string]*string{
			"PRIVATE_KEY_SSM_ID":     options.PrivateKeySSMId,
			"PROVIDER_URL":           options.ProviderUrl,
			"SYSTEM_CONTRACT_BUCKET": options.SystemContractBucket,
			"SYSTEM_CONTRACT_KEY":    options.SystemContractKey,
		},
	})
}
