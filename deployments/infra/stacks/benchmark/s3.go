package benchmark

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// S3 related functions
func createBucket(scope constructs.Construct, name string) awss3.IBucket {
	return awss3.NewBucket(scope, jsii.String(name), &awss3.BucketProps{
		// private
		PublicReadAccess: jsii.Bool(false),
		BucketName:       jsii.String(name),
	})
}
