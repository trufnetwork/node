package testhelpers

import (
	"os"
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

// DummyAsset returns an S3 asset backed by an empty temp file.
func DummyAsset(scope constructs.Construct, id string) awss3assets.Asset {
	tmp, _ := os.CreateTemp("", "dummy-*")
	tmp.Close()
	// Use filepath.Clean to normalize the path
	return awss3assets.NewAsset(scope, jsii.String(id), &awss3assets.AssetProps{
		Path: jsii.String(filepath.Clean(tmp.Name())),
	})
}
