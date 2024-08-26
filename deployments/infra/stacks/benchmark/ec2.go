package benchmark

import (
	"fmt"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type (
	CreateLaunchTemplateInput struct {
		ID            string
		InstanceType  awsec2.InstanceType
		BinaryS3Asset awss3assets.Asset
		SecurityGroup awsec2.ISecurityGroup
		IAMRole       awsiam.IRole
		KeyPair       awsec2.IKeyPair
	}
	CreateLaunchTemplateOutput struct {
		LaunchTemplate      awsec2.LaunchTemplate
		InstanceType        awsec2.InstanceType
		BenchmarkBinaryPath string
	}
)

// EC2 related functions
func createLaunchTemplate(scope constructs.Construct, input CreateLaunchTemplateInput) CreateLaunchTemplateOutput {
	launchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String(input.ID), &awsec2.LaunchTemplateProps{
		InstanceType:  input.InstanceType,
		SecurityGroup: input.SecurityGroup,
		KeyPair:       input.KeyPair,
	})

	instanceType := input.InstanceType.ToString()

	launchTemplate.UserData().AddCommands(
		*jsii.Strings(
			// we need to tell what type of instance this is
			fmt.Sprintf("INSTANCE_TYPE=%s", *instanceType),
			"echo INSTANCE_TYPE=$INSTANCE_TYPE >> /etc/environment",
		)...,
	)

	benchmarkBinaryPath := "/home/ec2-user/benchmark"

	// copy the binary from S3
	launchTemplate.UserData().AddCommands(
		*jsii.Strings(
			fmt.Sprintf("aws s3 cp s3://%s/%s %s",
				*input.BinaryS3Asset.S3BucketName(),
				*input.BinaryS3Asset.S3ObjectKey(),
				benchmarkBinaryPath,
			),
			fmt.Sprintf("chmod +x %s", benchmarkBinaryPath),
		)...,
	)

	return CreateLaunchTemplateOutput{
		LaunchTemplate:      launchTemplate,
		InstanceType:        input.InstanceType,
		BenchmarkBinaryPath: benchmarkBinaryPath,
	}
}
