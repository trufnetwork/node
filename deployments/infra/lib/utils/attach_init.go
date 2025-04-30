package utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/jsii-runtime-go"
)

type AttachInitDataToLaunchTemplateInput struct {
	LaunchTemplate awsec2.LaunchTemplate
	InitData       awsec2.CloudFormationInit
	Role           awsiam.IRole
	Platform       awsec2.OperatingSystemType
}

func AttachInitDataToLaunchTemplate(input AttachInitDataToLaunchTemplateInput) {
	// Ensure the UserData script is initialized (non-nil) before attaching init data
	// Some CDK versions do not default userData on LaunchTemplate, so initialize if nil
	ud := input.LaunchTemplate.UserData()
	if ud == nil {
		// Default to Linux user data
		ud = awsec2.UserData_ForLinux(nil)
	}
	// Initialize UserData with an empty command
	ud.AddCommands(jsii.String(""))
	// Attach CloudFormation Init metadata and commands to the LaunchTemplate
	input.InitData.Attach(
		input.LaunchTemplate.Node().DefaultChild().(awsec2.CfnLaunchTemplate),
		&awsec2.AttachInitOptions{
			InstanceRole: input.Role,
			UserData:     ud,
			Platform:     input.Platform,
		},
	)
}
