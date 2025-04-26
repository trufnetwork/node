package utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type InstanceFromLaunchTemplateOnPublicSubnetInput struct {
	LaunchTemplate awsec2.LaunchTemplate
	ElasticIp      awsec2.CfnEIP
	Vpc            awsec2.IVpc
}

func InstanceFromLaunchTemplateOnPublicSubnetWithElasticIp(
	scope constructs.Construct,
	id *string,
	input InstanceFromLaunchTemplateOnPublicSubnetInput,
) awsec2.CfnInstance {
	subnetId := *input.Vpc.SelectSubnets(&awsec2.SubnetSelection{
		SubnetType: awsec2.SubnetType_PUBLIC,
	}).SubnetIds

	instance := awsec2.NewCfnInstance(scope, jsii.Sprintf("%s-instance", *id), &awsec2.CfnInstanceProps{
		LaunchTemplate: &awsec2.CfnInstance_LaunchTemplateSpecificationProperty{
			LaunchTemplateId: input.LaunchTemplate.LaunchTemplateId(),
			Version:          input.LaunchTemplate.LatestVersionNumber(),
		},
		SubnetId: subnetId[0],
	})

	instance.AddDependency(input.LaunchTemplate.Node().DefaultChild().(awscdk.CfnResource))

	awsec2.NewCfnEIPAssociation(scope, jsii.Sprintf("%s-eip-association", *id), &awsec2.CfnEIPAssociationProps{
		InstanceId:   instance.AttrInstanceId(),
		AllocationId: input.ElasticIp.AttrAllocationId(),
	})

	return instance
}
