package utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
)

type S3Object struct {
	Bucket awss3.IBucket
	Key    *string
}

// GrantRead grants read access to the principal
func (o S3Object) GrantRead(grantable awsiam.IGrantable) {
	o.Bucket.GrantRead(grantable, o.Key)
}
