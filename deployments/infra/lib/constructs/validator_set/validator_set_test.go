package validator_set_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"github.com/stretchr/testify/require"

	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	validator_set "github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	kwilnetwork "github.com/trufnetwork/node/infra/lib/kwil-network"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/secrets"
	"github.com/trufnetwork/node/infra/tests/testdata"
	testutil "github.com/trufnetwork/node/infra/tests/testutil"
)

// Placeholder public keys. They only need the right shape.
var testNodeKeys = []kwilnetwork.NodeKeys{
	{KeyType: "secp256k1", PublicKeyHex: "02" + strings.Repeat("11", 32)},
	{KeyType: "secp256k1", PublicKeyHex: "03" + strings.Repeat("22", 32)},
}

func TestValidatorSetSynth(t *testing.T) {
	app := awscdk.NewApp(&awscdk.AppProps{Context: &map[string]interface{}{"stage": "dev"}})
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
		Path: jsii.String(testdata.TestdataPath() + "/README.md"),
	})
	configImage := awss3assets.NewAsset(stack, jsii.String("ConfigImage"), &awss3assets.AssetProps{
		Path: jsii.String(testdata.TestdataPath() + "/README.md"),
	})

	// Dummy Genesis Asset
	genesisAsset := awss3assets.NewAsset(stack, jsii.String("GenesisAsset"), &awss3assets.AssetProps{
		Path: jsii.String(testdata.TestdataPath() + "/genesis.json"),
	})

	testPeers := make([]peer2.TNPeer, len(testNodeKeys))
	for i, key := range testNodeKeys {
		testPeers[i] = peer2.TNPeer{
			NodeId:         key.NodeId(),
			Address:        jsii.String(fmt.Sprintf("node-%d.test.sub", i+1)),
			NodeHexAddress: key.PublicKeyHex,
		}
	}

	_ = validator_set.NewValidatorSet(stack, "VS", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		Peers:        testPeers,
		NodeKeys:     testNodeKeys,
		GenesisAsset: genesisAsset,
		// Leave KeyPair nil to test default logic
		Assets: validator_set.TNAssets{
			DockerImage:   dockerImage,
			DockerCompose: dockerCompose,
			ConfigImage:   configImage,
		},
		InitElements: []awsec2.InitElement{},
		Secrets:      secrets.Store{Prefix: "/tsn/test"},
	})

	template := assertions.Template_FromStack(stack, nil)
	template.ResourceCountIs(jsii.String("AWS::EC2::LaunchTemplate"), jsii.Number(2))
	template.ResourceCountIs(jsii.String("AWS::EC2::SecurityGroup"), jsii.Number(1))
	template.ResourceCountIs(jsii.String("AWS::EC2::EIP"), jsii.Number(2))
	template.ResourceCountIs(jsii.String("AWS::Route53::RecordSet"), jsii.Number(2))
	// One role per node.
	template.ResourceCountIs(jsii.String("AWS::IAM::Role"), jsii.Number(2))

	// IMDSv2 only, unreachable from containers.
	template.AllResourcesProperties(jsii.String("AWS::EC2::LaunchTemplate"), map[string]interface{}{
		"LaunchTemplateData": map[string]interface{}{
			"MetadataOptions": map[string]interface{}{
				"HttpTokens":              "required",
				"HttpPutResponseHopLimit": 1,
				"HttpEndpoint":            "enabled",
			},
		},
	})

	raw, err := json.Marshal(template.ToJSON())
	require.NoError(t, err)
	// The node key is not downloaded by cfn-init, so it is not an asset.
	require.NotContains(t, string(raw), "init-tn-files/nodekey.json")

	launchTemplates := testutil.Resources(template, "AWS::EC2::LaunchTemplate")
	grants := testutil.PolicyGrants(template)
	for i := range testNodeKeys {
		param := fmt.Sprintf("/tsn/test/node/%d/nodekey", i)

		// Node i reads its key from SSM at boot.
		ltIDs := testutil.LogicalIDsContaining(launchTemplates, fmt.Sprintf("TNInstance%d", i))
		require.Len(t, ltIDs, 1)
		userData := testutil.LaunchTemplateUserData(launchTemplates[ltIDs[0]])
		require.Contains(t, userData, fmt.Sprintf(`NODE_KEY_PARAM="%s"`, param))
		require.Contains(t, userData, `aws ssm get-parameter --name "$NODE_KEY_PARAM" --with-decryption`)

		// Node i's role may read exactly its own parameter.
		roleIDs := testutil.LogicalIDsContaining(testutil.Resources(template, "AWS::IAM::Role"), fmt.Sprintf("NodeRole%d", i))
		require.Len(t, roleIDs, 1)
		var ssmResources []string
		for _, g := range grants {
			for _, action := range g.Actions {
				if strings.HasPrefix(action, "ssm:") && g.Role == roleIDs[0] {
					require.Equal(t, []string{"ssm:GetParameter"}, g.Actions)
					ssmResources = append(ssmResources, g.Resources...)
				}
			}
		}
		require.Equal(t, []string{"arn:${AWS::Partition}:ssm:us-east-1:123456789012:parameter" + param}, ssmResources)
	}

	// Asset reads are scoped to objects, never the whole assets bucket.
	for _, g := range grants {
		for _, res := range g.Resources {
			require.False(t, strings.HasSuffix(res, "-assets-123456789012-us-east-1/*"), "role %s can read the whole assets bucket", g.Role)
		}
	}
}
