package kwil_cluster_test

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
	kwil_cluster "github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
	validator_set "github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	utils "github.com/trufnetwork/node/infra/lib/utils"
	"github.com/trufnetwork/node/infra/tests/testdata"
	testhelpers "github.com/trufnetwork/node/infra/tests/testhelpers"
)

func TestKwilClusterSynth_NoCert(t *testing.T) {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("TestStack"), &awscdk.StackProps{
		Env: &awscdk.Environment{
			Account: jsii.String("123456789012"),
			Region:  jsii.String("us-east-1"),
		},
	})

	// VPC lookup
	vpc := awsec2.NewVpc(stack, jsii.String("Vpc"), &awsec2.VpcProps{NatGateways: jsii.Number(0)})

	// HostedDomain for DNS
	hd := domaincfg.NewHostedDomain(stack, "HostedDomain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     domaincfg.StageDev,
			Sub:       "sub",
			DevPrefix: "test",
		},
		EdgeCertificate: false,
	})

	// Dummy assets for ValidatorSet
	dockerImage := awsecrassets.NewDockerImageAsset(stack, jsii.String("DockerImage"), &awsecrassets.DockerImageAssetProps{Directory: jsii.String(testdata.TestdataPath())})
	dockerCompose := awss3assets.NewAsset(stack, jsii.String("ComposeAsset"), &awss3assets.AssetProps{Path: jsii.String(testdata.TestdataPath() + "/README.md")})
	configImage := awss3assets.NewAsset(stack, jsii.String("ConfigImage"), &awss3assets.AssetProps{Path: jsii.String(testdata.TestdataPath() + "/README.md")})

	// Dummy Genesis Asset
	genesisAsset := awss3assets.NewAsset(stack, jsii.String("GenesisAsset"), &awss3assets.AssetProps{
		Path: jsii.String(testdata.TestdataPath() + "/genesis.json"), // Needs a dummy genesis file in testdata
	})

	// Dummy S3 object for KGW binary to avoid nil bucket panic
	dummyBucket := awss3.NewBucket(stack, jsii.String("DummyBinaryBucket"), &awss3.BucketProps{})
	dummyBinaryObj := utils.S3Object{Bucket: dummyBucket, Key: jsii.String("dummy-key")}

	// Single-node peer list
	testPeers := []peer2.TNPeer{
		{
			NodeId:         "nodeId0",
			Address:        jsii.String("peer0.test.sub"),
			NodeHexAddress: "hex0",
		},
	}

	// Create ValidatorSet
	vs := validator_set.NewValidatorSet(stack, "VS", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		Peers:        testPeers,
		GenesisAsset: genesisAsset,
		KeyPair:      nil,
		Assets: validator_set.TNAssets{
			DockerImage:   dockerImage,
			DockerCompose: dockerCompose,
			ConfigImage:   configImage,
		},
		InitElements: []awsec2.InitElement{},
	})

	// Prepare dummy Kwil Cluster Assets using stub assets to avoid nil panics
	kwilAssets := kwil_cluster.KwilAssets{
		Gateway: kwil_cluster.GatewayAssets{
			DirAsset: testhelpers.DummyAsset(stack, "DummyGatewayDir"),
			Binary:   dummyBinaryObj,
		},
		Indexer: kwil_cluster.IndexerAssets{
			DirAsset: testhelpers.DummyAsset(stack, "DummyIndexerDir"),
		},
	}

	_ = kwil_cluster.NewKwilCluster(stack, "KC", &kwil_cluster.KwilClusterProps{
		Vpc:                  vpc,
		HostedDomain:         hd,
		Cert:                 nil,
		CorsOrigins:          nil,
		SessionSecret:        nil,
		ChainId:              nil,
		Validators:           vs.Nodes,
		InitElements:         []awsec2.InitElement{},
		Assets:               kwilAssets,
		SelectedFrontingKind: fronting.KindAPI,
	})

	template := assertions.Template_FromStack(stack, nil)
	// Expect three launch templates (ValidatorSet node + KGW + Indexer)
	template.ResourceCountIs(jsii.String("AWS::EC2::LaunchTemplate"), jsii.Number(3))
	// Expect three EIPs (ValidatorSet node + KGW + Indexer)
	template.ResourceCountIs(jsii.String("AWS::EC2::EIP"), jsii.Number(3))
	// Expect three A records (ValidatorSet node + KGW + Indexer)
	template.ResourceCountIs(jsii.String("AWS::Route53::RecordSet"), jsii.Number(3))
}
