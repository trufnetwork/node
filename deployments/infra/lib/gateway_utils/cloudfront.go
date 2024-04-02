package gateway_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscloudfront"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscloudfrontorigins"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53targets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// CloudfrontForEc2Instance creates a CloudFront distribution for an EC2 instance without using a load balancer.
// and disables caching while forwarding all headers to the instance.
// it makes easier to use TSL certificate for the domain name.
func CloudfrontForEc2Instance(scope constructs.Construct, instancePublicDnsName *string, domainName *string,
	hostedZone awsroute53.IHostedZone, certificate awscertificatemanager.Certificate) awscloudfront.Distribution {

	// Define the CloudFront distribution
	distribution := awscloudfront.NewDistribution(scope, jsii.String("CloudFrontDistribution"), &awscloudfront.DistributionProps{
		DefaultBehavior: &awscloudfront.BehaviorOptions{
			Origin: awscloudfrontorigins.NewHttpOrigin(jsii.String(*instancePublicDnsName), &awscloudfrontorigins.HttpOriginProps{
				HttpPort:       jsii.Number(80),
				HttpsPort:      jsii.Number(443),
				ProtocolPolicy: awscloudfront.OriginProtocolPolicy_HTTP_ONLY,
			}),
			AllowedMethods:       awscloudfront.AllowedMethods_ALLOW_ALL(),
			CachePolicy:          awscloudfront.CachePolicy_CACHING_DISABLED(),
			OriginRequestPolicy:  awscloudfront.OriginRequestPolicy_ALL_VIEWER(),
			ViewerProtocolPolicy: awscloudfront.ViewerProtocolPolicy_REDIRECT_TO_HTTPS,
		},
		DomainNames: &[]*string{domainName},
		Certificate: certificate,
	})

	// Create a Route 53 alias record to point the domain name to the CloudFront distribution
	awsroute53.NewARecord(scope, jsii.String("AliasRecord"), &awsroute53.ARecordProps{
		Zone:       hostedZone,
		RecordName: domainName,
		Target:     awsroute53.RecordTarget_FromAlias(awsroute53targets.NewCloudFrontTarget(distribution)),
	})

	return distribution
}
