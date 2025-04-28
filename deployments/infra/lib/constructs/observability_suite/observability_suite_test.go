package observability_suite_test

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/jsii-runtime-go"
	observability "github.com/trufnetwork/node/infra/lib/constructs/observability_suite"
)

func TestObservabilitySuiteSynth(t *testing.T) {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("TestStack"), &awscdk.StackProps{
		Env: &awscdk.Environment{
			Account: jsii.String("123456789012"),
			Region:  jsii.String("us-east-1"),
		},
	})

	// VPC for testing
	vpc := awsec2.NewVpc(stack, jsii.String("Vpc"), &awsec2.VpcProps{
		NatGateways: jsii.Number(0),
	})
	// Dummy security groups for validator and gateway
	validatorSg := awsec2.NewSecurityGroup(stack, jsii.String("ValidatorSG"), &awsec2.SecurityGroupProps{Vpc: vpc})
	gatewaySg := awsec2.NewSecurityGroup(stack, jsii.String("GatewaySG"), &awsec2.SecurityGroupProps{Vpc: vpc})
	prefix := jsii.String("/test/")

	// Synthesize the ObservabilitySuite construct
	_ = observability.NewObservabilitySuite(stack, "OBS", &observability.ObservabilitySuiteProps{
		Vpc:          vpc,
		ValidatorSg:  validatorSg,
		GatewaySg:    gatewaySg,
		ParamsPrefix: prefix,
	})

	template := assertions.Template_FromStack(stack, nil)
	// Expect six SSM parameters, one security group, and one EC2 instance
	template.ResourceCountIs(jsii.String("AWS::SSM::Parameter"), jsii.Number(6))
	template.ResourceCountIs(jsii.String("AWS::EC2::SecurityGroup"), jsii.Number(1))
	template.ResourceCountIs(jsii.String("AWS::EC2::Instance"), jsii.Number(1))
}
