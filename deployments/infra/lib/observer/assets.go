package observer

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

const (
	ObserverAssetsDirRelativePath = "../observer"
	ObserverZipAssetDir           = "/home/ec2-user/observer.zip"
)

func GetObserverAsset(scope constructs.Construct, id *string) awss3assets.Asset {

	asset := awss3assets.NewAsset(scope, id, &awss3assets.AssetProps{
		Path: jsii.String(ObserverAssetsDirRelativePath),
		Exclude: jsii.Strings(
			".env*",
			"*.md",
			"*dev*",
		),
	})

	return asset
}
