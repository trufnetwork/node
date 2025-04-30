package validator_set

import (
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	jsii "github.com/aws/jsii-runtime-go"
)

type TNAssets struct {
	DockerImage   awsecrassets.DockerImageAsset
	DockerCompose awss3assets.Asset
	ConfigImage   awss3assets.Asset
}

type TNAssetOptions struct {
	RootDir string // path to the TN-DB compose directory
}

// BuildTNAssets packages the TN Docker image, compose file, and config file
func BuildTNAssets(scope constructs.Construct, opts TNAssetOptions) TNAssets {
	img := awsecrassets.NewDockerImageAsset(scope, jsii.String("TNImage"), &awsecrassets.DockerImageAssetProps{
		Directory: jsii.String(opts.RootDir),
		File:      jsii.String("deployments/Dockerfile"),
		Exclude:   jsii.Strings("infra"),
	})
	compose := awss3assets.NewAsset(scope, jsii.String("TNCompose"), &awss3assets.AssetProps{
		Path: jsii.String(filepath.Join(opts.RootDir, "compose.yaml")),
	})
	cfg := awss3assets.NewAsset(scope, jsii.String("TNConfig"), &awss3assets.AssetProps{
		Path: jsii.String(filepath.Join(opts.RootDir, "deployments/tn-config.dockerfile")),
	})
	return TNAssets{DockerImage: img, DockerCompose: compose, ConfigImage: cfg}
}
