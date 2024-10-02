package utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
)

type AttachInitDataToLaunchTemplateInput struct {
	LaunchTemplate awsec2.LaunchTemplate
	InitData       awsec2.CloudFormationInit
	Role           awsiam.IRole
	Platform       awsec2.OperatingSystemType
}

func AttachInitDataToLaunchTemplate(input AttachInitDataToLaunchTemplateInput) {
	input.InitData.Attach(input.LaunchTemplate.Node().DefaultChild().(awsec2.CfnLaunchTemplate), &awsec2.AttachInitOptions{
		InstanceRole: input.Role,
		UserData:     input.LaunchTemplate.UserData(),
		Platform:     input.Platform,
	})
}
