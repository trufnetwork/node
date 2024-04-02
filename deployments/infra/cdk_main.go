package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/truflation/tsn-db/infra/lib/domain_utils"
	"github.com/truflation/tsn-db/infra/lib/gateway_utils"
	"os"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"github.com/truflation/tsn-db/infra/lib/instance_utils"
)

type CdkStackProps struct {
	awscdk.StackProps
	cert awscertificatemanager.Certificate
}

func TsnDBCdkStack(scope constructs.Construct, id string, props *CdkStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, jsii.String(id), &sprops)

	awscdk.NewCfnOutput(stack, jsii.String("region"), &awscdk.CfnOutputProps{
		Value: stack.Region(),
	})

	instanceRole := awsiam.NewRole(stack, jsii.String("InstanceRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

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

	cacheType := "local"
	cacheFromParams := "src=/tmp/buildx-cache/#IMAGE_NAME"
	cacheToParams := "dest=/tmp/buildx-cache-new/#IMAGE_NAME"

	if os.Getenv("CI") == "true" {
		cacheType = "gha"
		cacheFromParams = "scope=truflation/tsn/#IMAGE_NAME"
		cacheToParams = "mode=max,scope=truflation/tsn/#IMAGE_NAME"
	}

	tsnImageAsset := awsecrassets.NewDockerImageAsset(stack, jsii.String("TsnImageAsset"), &awsecrassets.DockerImageAssetProps{
		CacheFrom: &[]*awsecrassets.DockerCacheOption{
			{
				Type: jsii.String(cacheType),
				// the image name here must match from the compose file, then the cache should work
				// across different workflows
				Params: UpdateParamsWithImageName(cacheFromParams, "tsn-db"),
			},
		},
		CacheTo: &awsecrassets.DockerCacheOption{
			Type:   jsii.String(cacheType),
			Params: UpdateParamsWithImageName(cacheToParams, "tsn-db"),
		},
		File:      jsii.String("deployments/Dockerfile"),
		Directory: jsii.String("../../"),
	})
	tsnImageAsset.Repository().GrantPull(instanceRole)

	pushDataImageAsset := awsecrassets.NewDockerImageAsset(stack, jsii.String("PushDataImageAsset"), &awsecrassets.DockerImageAssetProps{
		CacheFrom: &[]*awsecrassets.DockerCacheOption{
			{
				Type: jsii.String(cacheType),
				// the image name here must match from the compose file, then the cache should work
				// across different workflows
				Params: UpdateParamsWithImageName(cacheFromParams, "push-tsn-data"),
			},
		},
		CacheTo: &awsecrassets.DockerCacheOption{
			Type:   jsii.String(cacheType),
			Params: UpdateParamsWithImageName(cacheToParams, "push-tsn-data"),
		},
		File:      jsii.String("deployments/push-tsn-data.dockerfile"),
		Directory: jsii.String("../../"),
	})
	pushDataImageAsset.Repository().GrantPull(instanceRole)

	// Adding our docker compose file to the instance
	dockerComposeAsset := awss3assets.NewAsset(stack, jsii.String("TsnComposeAsset"), &awss3assets.AssetProps{
		Path: jsii.String("../../compose.yaml"),
	})
	dockerComposeAsset.GrantRead(instanceRole)

	// differently from tsn-db, the gateway docker images will be built by the instance, not in GH actions.
	kgwDirectoryAsset := awss3assets.NewAsset(stack, jsii.String("KgwComposeAsset"), &awss3assets.AssetProps{
		// gateway directory contains more than one file to configure the gateway, so we need to zip it
		Path: jsii.String("../gateway/"),
	})
	kgwDirectoryAsset.GrantRead(instanceRole)

	initElements := []awsec2.InitElement{
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ec2-user/docker-compose.yaml"), dockerComposeAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		}),
		awsec2.InitFile_FromExistingAsset(jsii.String("/home/ec2-user/kgw.zip"), kgwDirectoryAsset, &awsec2.InitFileOptions{
			Owner: jsii.String("ec2-user"),
		}),
	}

	// default vpc
	vpcInstance := awsec2.Vpc_FromLookup(stack, jsii.String("VPC"), &awsec2.VpcLookupOptions{
		IsDefault: jsii.Bool(true),
	})

	// Create instance using tsnImageAsset hash so that the instance is recreated when the image changes.
	newName := "TsnDBInstance" + *tsnImageAsset.AssetHash()

	bucketName := "kwil-binaries"
	kwilGatewayBucket := awss3.Bucket_FromBucketName(stack, jsii.String("KwilGatewayBucket"), jsii.String(bucketName))
	objPath := "gateway/kgw-v0.2.0.zip"
	kwilGatewayBucket.GrantRead(instanceRole, jsii.String(objPath))

	instance := instance_utils.CreateInstance(stack, instanceRole, newName, vpcInstance, &initElements)

	// Get the hosted zone.
	domain := config.Domain(stack)
	hostedZone := domain_utils.GetTSNHostedZone(stack)

	gateway_utils.CloudfrontForEc2Instance(stack, instance.InstancePublicDnsName(), domain, hostedZone, props.cert)
	//enable the instance to use the certificate
	instance_utils.AddTsnDbStartupScriptsToInstance(instance_utils.AddStartupScriptsOptions{
		Stack:              stack,
		Instance:           instance,
		TsnImageAsset:      tsnImageAsset,
		PushDataImageAsset: pushDataImageAsset,
	})
	gateway_utils.AddKwilGatewayStartupScriptsToInstance(gateway_utils.AddKwilGatewayStartupScriptsOptions{
		Instance: instance,
		Domain:   domain,
	})

	// Output info.
	awscdk.NewCfnOutput(stack, jsii.String("public-address"), &awscdk.CfnOutputProps{
		Value: instance.InstancePublicIp(),
	})

	return stack
}

// ConvertParamsToMap converts a string of comma-separated key-value pairs to a map.
// e.g.: "key1=value1,key2=value2" -> {"key1": "value1", "key2": "value2"}
func ConvertParamsToMap(paramsStr string) *map[string]*string {
	params := strings.Split(paramsStr, ",")
	paramsMap := make(map[string]*string)
	for _, param := range params {
		kv := strings.Split(param, "=")
		paramsMap[kv[0]] = jsii.String(kv[1])
	}
	return &paramsMap
}

// UpdateMapValues in every param, it replaces the target string with the value string.
func UpdateMapValues(params *map[string]*string, target string, value string) {
	for k, v := range *params {
		(*params)[k] = jsii.String(strings.Replace(*v, target, value, -1))
	}
}

func UpdateParamsWithImageName(paramsStr string, imageName string) *map[string]*string {
	params := ConvertParamsToMap(paramsStr)
	UpdateMapValues(params, "#IMAGE_NAME", imageName)
	return params
}

// CertStack creates a stack with an ACM certificate for the domain, fixed at us-east-1.
// This is necessary because CloudFront requires the certificate to be in us-east-1.
func CertStack(app constructs.Construct) awscertificatemanager.Certificate {
	env := env()
	env.Region = jsii.String("us-east-1")
	stackName := config.StackName(app) + "-Cert"
	stack := awscdk.NewStack(app, jsii.String(stackName), &awscdk.StackProps{
		Env:                   env,
		CrossRegionReferences: jsii.Bool(true),
	})
	domain := config.Domain(stack)
	hostedZone := domain_utils.GetTSNHostedZone(stack)
	return domain_utils.GetACMCertificate(stack, domain, &hostedZone)
}

func main() {
	app := awscdk.NewApp(nil)

	certificate := CertStack(app)

	TsnDBCdkStack(app, config.StackName(app), &CdkStackProps{
		awscdk.StackProps{
			Env:                   env(),
			CrossRegionReferences: jsii.Bool(true),
		},
		certificate,
	})

	app.Synth(nil)
}

// env determines the AWS environment (account+region) in which our stack is to
// be deployed. For more information see: https://docs.aws.amazon.com/cdk/latest/guide/environments.html
func env() *awscdk.Environment {
	account := os.Getenv("CDK_DEPLOY_ACCOUNT")
	region := os.Getenv("CDK_DEPLOY_REGION")

	if len(account) == 0 || len(region) == 0 {
		account = os.Getenv("CDK_DEFAULT_ACCOUNT")
		region = os.Getenv("CDK_DEFAULT_REGION")
	}

	return &awscdk.Environment{
		Account: jsii.String(account),
		Region:  jsii.String(region),
	}
}
