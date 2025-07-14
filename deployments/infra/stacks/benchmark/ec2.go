package benchmark

import (
	"fmt"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
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
		LaunchTemplate         awsec2.LaunchTemplate
		InstanceType           awsec2.InstanceType
		BenchmarkBinaryZipPath string
	}
)

// EC2 related functions
func createLaunchTemplate(scope constructs.Construct, input CreateLaunchTemplateInput) CreateLaunchTemplateOutput {
	ltConstructID := input.ID
	AWSLinux2MachineImage := awsec2.MachineImage_LatestAmazonLinux2023(nil)

	// Create a new EC2 launch template with specified properties
	launchTemplate := awsec2.NewLaunchTemplate(scope, jsii.String(ltConstructID), &awsec2.LaunchTemplateProps{
		InstanceType:  input.InstanceType,
		MachineImage:  AWSLinux2MachineImage,
		SecurityGroup: input.SecurityGroup,
		KeyPair:       input.KeyPair,
		Role:          input.IAMRole,
		UserData:      awsec2.UserData_ForLinux(nil),
	})

	// Log Launch Template creation
	instanceTypeStr := input.InstanceType.ToString()
	amiID := *AWSLinux2MachineImage.GetImage(scope).ImageId
	roleArn := *input.IAMRole.RoleArn()
	cdklogger.LogInfo(scope, ltConstructID, "Created Launch Template: InstanceType=%s, MachineImage=%s, Role=%s", *instanceTypeStr, amiID, roleArn)

	// Check if launchTemplate is nil
	if launchTemplate == nil {
		panic("Failed to create launch template")
	}

	instanceType := input.InstanceType.ToString()

	// Check if instanceType is empty
	if instanceType == nil || *instanceType == "" {
		panic("Instance type is empty")
	}

	// Add user data
	userData := launchTemplate.UserData()
	if userData == nil {
		panic("UserData is nil")
	}

	// install docker
	userData.AddCommands(
		*jsii.Strings(
			"sudo dnf update -y",
			"sudo dnf install -y docker",
			"sudo service docker start",
			"sudo usermod -a -G docker ec2-user",
			"newgrp docker",
		)...,
	)

	userData.AddCommands(
		*jsii.Strings(
			fmt.Sprintf("INSTANCE_TYPE=%s", *instanceType),
			"echo INSTANCE_TYPE=$INSTANCE_TYPE >> /etc/environment",
		)...,
	)

	benchmarkBinaryZipPath := "/home/ec2-user/benchmark.zip"

	// Check if S3 bucket name and object key are not nil
	if input.BinaryS3Asset.S3BucketName() == nil || input.BinaryS3Asset.S3ObjectKey() == nil {
		panic("S3 bucket name or object key is nil")
	}

	// Add user data to download and set up the benchmark binary
	userData.AddCommands(
		*jsii.Strings(
			fmt.Sprintf("aws s3 cp s3://%s/%s %s",
				*input.BinaryS3Asset.S3BucketName(),
				*input.BinaryS3Asset.S3ObjectKey(),
				benchmarkBinaryZipPath,
			),
			fmt.Sprintf("chmod +x %s", benchmarkBinaryZipPath),
		)...,
	)

	return CreateLaunchTemplateOutput{
		LaunchTemplate:         launchTemplate,
		InstanceType:           input.InstanceType,
		BenchmarkBinaryZipPath: benchmarkBinaryZipPath,
	}
}
