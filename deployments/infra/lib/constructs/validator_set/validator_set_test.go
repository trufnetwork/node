package validator_set_test

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	validator_set "github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	testutil "github.com/trufnetwork/node/infra/tests/testutil"
)

func TestValidatorSetSynth(t *testing.T) {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("TestStack"), &awscdk.StackProps{
		Env: &awscdk.Environment{
			Account: jsii.String("123456789012"),
			Region:  jsii.String("us-east-1"),
		},
	})

	// Create a VPC for testing
	vpc := awsec2.NewVpc(stack, jsii.String("Vpc"), &awsec2.VpcProps{
		NatGateways: jsii.Number(0),
	})

	// Create a HostedDomain for testing
	hd := domaincfg.NewHostedDomain(stack, "HostedDomain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     domaincfg.StageDev,
			DevPrefix: "test",
			Sub:       "sub",
		},
		EdgeCertificate: false,
	})

	// Dummy assets for image and config
	dockerImage := testutil.DummyDockerImageAsset(stack, "DockerImage", t)
	dockerCompose := awss3assets.NewAsset(stack, jsii.String("ComposeAsset"), &awss3assets.AssetProps{
		Path: jsii.String("testdata/README.md"),
	})
	configImage := awss3assets.NewAsset(stack, jsii.String("ConfigImage"), &awss3assets.AssetProps{
		Path: jsii.String("testdata/README.md"),
	})

	// Dummy Genesis Asset
	genesisAsset := awss3assets.NewAsset(stack, jsii.String("GenesisAsset"), &awss3assets.AssetProps{
		Path: jsii.String("testdata/genesis.json"), // Needs a dummy genesis file
	})

	// Single-node peer list
	testPeers := []peer2.TNPeer{
		{
			NodeId:         "nodeId0",
			Address:        jsii.String("peer0.test.sub"),
			NodeHexAddress: "hex0",
		},
	}

	_ = validator_set.NewValidatorSet(stack, "VS", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		Peers:        testPeers,
		GenesisAsset: genesisAsset,
		// Leave KeyPair nil to test default logic
		Assets: validator_set.TNAssets{
			DockerImage:   dockerImage,
			DockerCompose: dockerCompose,
			ConfigImage:   configImage,
		},
		InitElements: []awsec2.InitElement{},
	})

	// Assert on synthesized template
	template := assertions.Template_FromStack(stack, nil)
	// Expect one launch template (EC2 instances)
	template.ResourceCountIs(jsii.String("AWS::EC2::LaunchTemplate"), jsii.Number(1))
	// Expect one security group (explicitly created by ValidatorSet)
	template.ResourceCountIs(jsii.String("AWS::EC2::SecurityGroup"), jsii.Number(1))
	// Expect one EIP for the node
	template.ResourceCountIs(jsii.String("AWS::EC2::EIP"), jsii.Number(1))
	// Expect one DNS A record
	template.ResourceCountIs(jsii.String("AWS::Route53::RecordSet"), jsii.Number(1))
}
