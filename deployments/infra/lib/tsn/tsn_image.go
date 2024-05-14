package tsn

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

func NewTSNImageAsset(
	stack awscdk.Stack,
) awsecrassets.DockerImageAsset {
	// for some reason this is not working, it's not setting the repo correctly
	//repo := awsecr.NewRepository(stack, jsii.String("ECRRepository"), &awsecr.RepositoryProps{
	//	RepositoryName:     jsii.String(config.EcrRepoName(stack)),
	//	RemovalPolicy:      awscdk.RemovalPolicy_DESTROY,
	//	ImageTagMutability: awsecr.TagMutability_MUTABLE,
	//	ImageScanOnPush:    jsii.Bool(false),
	//	LifecycleRules: &[]*awsecr.LifecycleRule{
	//		{
	//			MaxImageCount: jsii.Number(10),
	//			RulePriority:  jsii.Number(1),
	//		},
	//	},
	//})

	cacheOpts := utils.GetBuildxCacheOpts()

	return awsecrassets.NewDockerImageAsset(stack, jsii.String("TsnImageAsset"), &awsecrassets.DockerImageAssetProps{
		CacheFrom: &[]*awsecrassets.DockerCacheOption{
			{
				Type: jsii.String(cacheOpts.CacheType),
				// the image name here must match from the compose file, then the cache should work
				// across different workflows
				Params: utils.UpdateParamsWithImageName(cacheOpts.CacheFromParams, "tsn-db"),
			},
		},
		CacheTo: &awsecrassets.DockerCacheOption{
			Type:   jsii.String(cacheOpts.CacheType),
			Params: utils.UpdateParamsWithImageName(cacheOpts.CacheToParams, "tsn-db"),
		},
		File:      jsii.String("deployments/Dockerfile"),
		Directory: jsii.String("../../"),
	})
}
