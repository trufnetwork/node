package kwil_gateway

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscloudfront"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscloudfrontorigins"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53targets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type TSNCloudfrontConfig struct {
	DomainName           *string
	KgwPublicDnsName     *string
	IndexerPublicDnsName *string
	HostedZone           awsroute53.IHostedZone
	Certificate          awscertificatemanager.Certificate
}

// TSNCloudfrontInstance creates a CloudFront distribution for an EC2 instance without using a load balancer.
// and disables caching while forwarding all headers to the instance.
// it makes easier to use TSL certificate for the domain name.
func TSNCloudfrontInstance(scope constructs.Construct, id *string, config TSNCloudfrontConfig) awscloudfront.Distribution {
	// Define the CloudFront distribution
	distribution := awscloudfront.NewDistribution(scope, id, &awscloudfront.DistributionProps{
		DefaultBehavior: &awscloudfront.BehaviorOptions{
			Origin: awscloudfrontorigins.NewHttpOrigin(jsii.String(*config.KgwPublicDnsName), &awscloudfrontorigins.HttpOriginProps{
				HttpPort:       jsii.Number(80),
				HttpsPort:      jsii.Number(443),
				ProtocolPolicy: awscloudfront.OriginProtocolPolicy_HTTP_ONLY,
			}),
			AllowedMethods:       awscloudfront.AllowedMethods_ALLOW_ALL(),
			CachePolicy:          awscloudfront.CachePolicy_CACHING_DISABLED(),
			OriginRequestPolicy:  awscloudfront.OriginRequestPolicy_ALL_VIEWER(),
			ViewerProtocolPolicy: awscloudfront.ViewerProtocolPolicy_REDIRECT_TO_HTTPS,
		},
		AdditionalBehaviors: &map[string]*awscloudfront.BehaviorOptions{
			// Redirects all requests to the indexer to the indexer instance
			// Note that this will fail if indexer expects a different path for some requests
			"/v0/*": {
				Origin: awscloudfrontorigins.NewHttpOrigin(jsii.String(*config.IndexerPublicDnsName), &awscloudfrontorigins.HttpOriginProps{
					HttpPort:       jsii.Number(80),
					ProtocolPolicy: awscloudfront.OriginProtocolPolicy_HTTP_ONLY,
				}),
				AllowedMethods:       awscloudfront.AllowedMethods_ALLOW_ALL(),
				CachePolicy:          awscloudfront.CachePolicy_CACHING_DISABLED(),
				OriginRequestPolicy:  awscloudfront.OriginRequestPolicy_ALL_VIEWER(),
				ViewerProtocolPolicy: awscloudfront.ViewerProtocolPolicy_ALLOW_ALL,
			},
		},
		DomainNames: &[]*string{config.DomainName},
		Certificate: config.Certificate,
	})

	// Create a Route 53 alias record to point the domain name to the CloudFront distribution
	awsroute53.NewARecord(scope, jsii.Sprintf("%sRecord", *id), &awsroute53.ARecordProps{
		Zone:       config.HostedZone,
		RecordName: config.DomainName,
		Target:     awsroute53.RecordTarget_FromAlias(awsroute53targets.NewCloudFrontTarget(distribution)),
	})

	return distribution
}
