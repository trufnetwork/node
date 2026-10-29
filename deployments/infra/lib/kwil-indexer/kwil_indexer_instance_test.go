package kwil_indexer_instance_test

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/jsii-runtime-go"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	kwil_indexer "github.com/trufnetwork/node/infra/lib/kwil-indexer"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/tn"
	"github.com/trufnetwork/node/infra/lib/utils"
	testhelpers "github.com/trufnetwork/node/infra/tests/testhelpers"
)

// The ports of the services set up on the indexer host (HTTPS for the v1 API,
// 8080 for the testnet indexer) belong on the indexer's own security group,
// next to ssh, whatever the fronting.
func TestIndexerSecurityGroupAllowsPublicPorts(t *testing.T) {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("TestStack"), &awscdk.StackProps{
		Env: &awscdk.Environment{
			Account: jsii.String("123456789012"),
			Region:  jsii.String("us-east-1"),
		},
	})

	vpc := awsec2.NewVpc(stack, jsii.String("Vpc"), &awsec2.VpcProps{NatGateways: jsii.Number(0)})
	hd := domaincfg.NewHostedDomain(stack, "HostedDomain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     domaincfg.StageDev,
			Sub:       "sub",
			DevPrefix: "test",
		},
		EdgeCertificate: false,
	})
	tnSG := awsec2.NewSecurityGroup(stack, jsii.String("TnSG"), &awsec2.SecurityGroupProps{Vpc: vpc})
	bucket := awss3.NewBucket(stack, jsii.String("BinaryBucket"), &awss3.BucketProps{})

	_ = kwil_indexer.NewIndexerInstance(stack, kwil_indexer.NewIndexerInstanceInput{
		Vpc: vpc,
		TNInstance: tn.TNInstance{
			SecurityGroup:  tnSG,
			PeerConnection: peer.TNPeer{Address: jsii.String("peer0.test.sub")},
		},
		IndexerDirAsset:    testhelpers.DummyAsset(stack, "IndexerDir"),
		IndexerBinaryAsset: utils.S3Object{Bucket: bucket, Key: jsii.String("indexer.zip")},
		HostedDomain:       hd,
	})

	template := assertions.Template_FromStack(stack, nil)
	for port, description := range map[int]string{
		443:  "Allow HTTPS to the v1 API.",
		8080: "Allow the testnet indexer API.",
	} {
		template.HasResourceProperties(jsii.String("AWS::EC2::SecurityGroup"), map[string]interface{}{
			"GroupDescription": "Kwil Indexer Security Group.",
			"SecurityGroupIngress": assertions.Match_ArrayWith(&[]interface{}{
				map[string]interface{}{
					"IpProtocol":  "tcp",
					"FromPort":    port,
					"ToPort":      port,
					"CidrIp":      "0.0.0.0/0",
					"Description": description,
				},
			}),
		})
	}
}
