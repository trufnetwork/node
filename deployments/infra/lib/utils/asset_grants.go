package utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
)

// The CDK grants for assets (Asset.GrantRead, InitFile_FromExistingAsset)
// cover every object in the shared assets bucket. These helpers grant read on
// the asset's own object only.

// InitFileFromAsset is InitFile_FromExistingAsset with a read grant scoped to
// the asset object.
func InitFileFromAsset(fileName *string, asset awss3assets.Asset, options *awsec2.InitFileOptions) awsec2.InitFile {
	return awsec2.InitFile_FromS3Object(fileName, asset.Bucket(), asset.S3ObjectKey(), options)
}

// GrantReadAsset lets grantee read the asset object and no other object in
// the assets bucket.
func GrantReadAsset(asset awss3assets.Asset, grantee awsiam.IGrantable) {
	asset.Bucket().GrantRead(grantee, asset.S3ObjectKey())
}
