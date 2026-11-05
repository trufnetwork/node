package kwil_cluster_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/jsii-runtime-go"
	"github.com/stretchr/testify/require"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
	kwil_cluster "github.com/trufnetwork/node/infra/lib/constructs/kwil_cluster"
	validator_set "github.com/trufnetwork/node/infra/lib/constructs/validator_set"
	kwilnetwork "github.com/trufnetwork/node/infra/lib/kwil-network"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/secrets"
	utils "github.com/trufnetwork/node/infra/lib/utils"
	"github.com/trufnetwork/node/infra/tests/testdata"
	testhelpers "github.com/trufnetwork/node/infra/tests/testhelpers"
	testutil "github.com/trufnetwork/node/infra/tests/testutil"
)

func TestKwilClusterSynth_NoCert(t *testing.T) {
	app := awscdk.NewApp(&awscdk.AppProps{Context: &map[string]interface{}{"stage": "dev"}})
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

	// Single-node peer list with a placeholder public key
	nodeKeys := []kwilnetwork.NodeKeys{{KeyType: "secp256k1", PublicKeyHex: "02" + strings.Repeat("11", 32)}}
	testPeers := []peer2.TNPeer{
		{
			NodeId:         nodeKeys[0].NodeId(),
			Address:        jsii.String("node-1.test.sub"),
			NodeHexAddress: nodeKeys[0].PublicKeyHex,
		},
	}
	store := secrets.Store{Prefix: "/tsn/test"}

	// Create ValidatorSet
	vs := validator_set.NewValidatorSet(stack, "VS", &validator_set.ValidatorSetProps{
		Vpc:          vpc,
		HostedDomain: hd,
		Peers:        testPeers,
		NodeKeys:     nodeKeys,
		GenesisAsset: genesisAsset,
		KeyPair:      nil,
		Assets: validator_set.TNAssets{
			DockerImage:   dockerImage,
			DockerCompose: dockerCompose,
			ConfigImage:   configImage,
		},
		InitElements: []awsec2.InitElement{},
		Secrets:      store,
	})

	// Prepare dummy Kwil Cluster Assets using stub assets to avoid nil panics
	kwilAssets := kwil_cluster.KwilAssets{
		Gateway: kwil_cluster.GatewayAssets{
			DirAsset: testhelpers.DummyAsset(stack, "DummyGatewayDir"),
			Binary:   dummyBinaryObj,
		},
		Indexer: kwil_cluster.IndexerAssets{
			DirAsset: testhelpers.DummyAsset(stack, "DummyIndexerDir"),
			Binary:   dummyBinaryObj,
		},
	}

	_ = kwil_cluster.NewKwilCluster(stack, "KC", &kwil_cluster.KwilClusterProps{
		Vpc:                  vpc,
		HostedDomain:         hd,
		Cert:                 nil,
		CorsOrigins:          jsii.String("*"),
		ChainId:              jsii.String("test-chain"),
		Validators:           vs.Nodes,
		InitElements:         []awsec2.InitElement{},
		Assets:               kwilAssets,
		SelectedFrontingKind: fronting.KindAPI,
		Secrets:              store,
	})

	template := assertions.Template_FromStack(stack, nil)
	// Expect three launch templates (ValidatorSet node + KGW + Indexer)
	template.ResourceCountIs(jsii.String("AWS::EC2::LaunchTemplate"), jsii.Number(3))
	// Expect three EIPs (ValidatorSet node + KGW + Indexer)
	template.ResourceCountIs(jsii.String("AWS::EC2::EIP"), jsii.Number(3))
	// Expect three A records (ValidatorSet node + KGW + Indexer)
	template.ResourceCountIs(jsii.String("AWS::Route53::RecordSet"), jsii.Number(3))

	// Every launch template: IMDSv2 only, hop limit 1.
	template.AllResourcesProperties(jsii.String("AWS::EC2::LaunchTemplate"), map[string]interface{}{
		"LaunchTemplateData": map[string]interface{}{
			"MetadataOptions": map[string]interface{}{
				"HttpTokens":              "required",
				"HttpPutResponseHopLimit": 1,
			},
		},
	})

	// The gateway reads the session secret from SSM at start. The unit file
	// carries no secret.
	launchTemplates := testutil.Resources(template, "AWS::EC2::LaunchTemplate")
	kgwIDs := testutil.LogicalIDsContaining(launchTemplates, "KGWLaunchTemplate")
	require.Len(t, kgwIDs, 1)
	userData := testutil.LaunchTemplateUserData(launchTemplates[kgwIDs[0]])
	require.Contains(t, userData, `PARAM_NAME="/tsn/test/kgw/session_secret"`)
	require.Contains(t, userData, "ExecStart=/usr/local/bin/start-kgw.sh")
	require.Contains(t, userData, "ExecStop=/bin/bash -c \"docker compose -p kgw down\"")
	require.NotContains(t, userData, `Environment="SESSION_SECRET`)

	// Only the gateway role reads the session secret, and only that parameter.
	roles := testutil.Resources(template, "AWS::IAM::Role")
	kgwRoles := testutil.LogicalIDsContaining(roles, "KGWInstanceRole")
	require.Len(t, kgwRoles, 1)
	sessionArn := "arn:${AWS::Partition}:ssm:us-east-1:123456789012:parameter/tsn/test/kgw/session_secret"
	var kgwSSM []string
	for _, g := range testutil.PolicyGrants(template) {
		for _, res := range g.Resources {
			if strings.Contains(res, "kgw/session_secret") {
				require.Equal(t, kgwRoles[0], g.Role)
			}
			// No role reads the whole assets bucket.
			require.False(t, strings.HasSuffix(res, "-assets-123456789012-us-east-1/*"), "role %s can read the whole assets bucket", g.Role)
		}
		if g.Role == kgwRoles[0] {
			for _, action := range g.Actions {
				if strings.HasPrefix(action, "ssm:") {
					require.Equal(t, []string{"ssm:GetParameter"}, g.Actions)
					kgwSSM = append(kgwSSM, g.Resources...)
				}
			}
		}
	}
	require.Equal(t, []string{sessionArn}, kgwSSM)
}
