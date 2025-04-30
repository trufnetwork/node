package observer

import (
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/utils"
)

const (
	ObserverZipAssetDir = "/home/ec2-user/observer.zip"
)

func GetObserverAsset(scope constructs.Construct, id *string) awss3assets.Asset {
	rootDir := utils.GetProjectRootDir()
	ObserverAssetsDirRelativePath := filepath.Join(rootDir, "deployments/infra/lib/observer")

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
