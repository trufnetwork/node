package kwil_cluster

import (
	"path/filepath"

	awss3 "github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type GatewayAssets struct {
	DirAsset awss3assets.Asset
	Binary   utils.S3Object
}

type IndexerAssets struct {
	DirAsset awss3assets.Asset
}

type KwilAssets struct {
	Gateway GatewayAssets
	Indexer IndexerAssets
}

type KwilAssetOptions struct {
	RootDir            string // base path for deployments directory
	BinariesBucketName string
	KGWBinaryKey       string
}

// BuildKwilAssets packages gateway and indexer directories and binaries
func BuildKwilAssets(scope constructs.Construct, opts KwilAssetOptions) KwilAssets {
	gwZip := awss3assets.NewAsset(scope, jsii.String("KGWDir"), &awss3assets.AssetProps{
		Path: jsii.String(filepath.Join(opts.RootDir, "deployments/gateway/")),
	})
	ixZip := awss3assets.NewAsset(scope, jsii.String("IndexerDir"), &awss3assets.AssetProps{
		Path: jsii.String(filepath.Join(opts.RootDir, "deployments/indexer/")),
	})
	bin := utils.S3Object{
		Bucket: awss3.Bucket_FromBucketName(scope, jsii.String("BinaryBucket"), jsii.String(opts.BinariesBucketName)),
		Key:    jsii.String(opts.KGWBinaryKey),
	}
	return KwilAssets{
		Gateway: GatewayAssets{DirAsset: gwZip, Binary: bin},
		Indexer: IndexerAssets{DirAsset: ixZip},
	}
}
