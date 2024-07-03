package utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/jsii-runtime-go"
)

type PublishInstanceToSubdomainInput struct {
	Instance   awsec2.Instance
	Subdomain  string
	HostedZone awsroute53.IHostedZone
	Domain     *string
}

type PublishInstanceToSubdomainOutput struct {
	AliasRecord awsroute53.ARecord
}

// PublishInstanceToSubdomain creates a Route 53 alias record to point the subdomain to the instance
func PublishInstanceToSubdomain(stack awscdk.Stack, input PublishInstanceToSubdomainInput) PublishInstanceToSubdomainOutput {
	completeSubdomain := input.Subdomain + "." + *input.Domain
	// will create a Route 53 alias record to point the subdomain to the instance
	aRecord := awsroute53.NewARecord(stack, jsii.String("AliasRecord"+input.Subdomain), &awsroute53.ARecordProps{
		Zone:       input.HostedZone,
		RecordName: jsii.String(completeSubdomain),
		Target:     awsroute53.RecordTarget_FromIpAddresses(input.Instance.InstancePublicIp()),
	})

	return PublishInstanceToSubdomainOutput{
		AliasRecord: aRecord,
	}
}
