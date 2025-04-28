package kwil_cluster

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awscloudfront"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	kwil_gateway "github.com/trufnetwork/node/infra/lib/kwil-gateway"
	kwil_indexer "github.com/trufnetwork/node/infra/lib/kwil-indexer"
	"github.com/trufnetwork/node/infra/lib/tn"
)

// KwilClusterProps holds inputs for creating a KwilCluster
// Cert is optional; if nil, CloudFront distribution will be skipped
// Validators should come from a previous ValidatorSet
// InitElements are user-data steps to apply to both instances

type KwilClusterProps struct {
	Vpc           awsec2.IVpc
	HostedDomain  *domaincfg.HostedDomain
	Cert          awscertificatemanager.Certificate // optional
	CorsOrigins   *string
	SessionSecret *string
	ChainId       *string
	Validators    []tn.TNInstance
	InitElements  []awsec2.InitElement
	Assets        KwilAssets
}

// KwilCluster is a reusable construct for Gateway and Indexer
type KwilCluster struct {
	constructs.Construct

	Gateway kwil_gateway.KGWInstance
	Indexer kwil_indexer.IndexerInstance
	Cdn     awscloudfront.Distribution // only if Cert != nil
}

// NewKwilCluster provisions the Kwil gateway, indexer, and optional CloudFront
func NewKwilCluster(scope constructs.Construct, id string, props *KwilClusterProps) *KwilCluster {
	node := constructs.NewConstruct(scope, jsii.String(id))
	kc := &KwilCluster{Construct: node}

	// create Gateway instance
	gw := kwil_gateway.NewKGWInstance(node, kwil_gateway.NewKGWInstanceInput{
		HostedDomain:   props.HostedDomain,
		Vpc:            props.Vpc,
		KGWDirAsset:    props.Assets.Gateway.DirAsset,
		KGWBinaryAsset: props.Assets.Gateway.Binary,
		Config: kwil_gateway.KGWConfig{
			Domain:           props.HostedDomain.DomainName,
			CorsAllowOrigins: props.CorsOrigins,
			SessionSecret:    props.SessionSecret,
			ChainId:          props.ChainId,
			Nodes:            props.Validators,
		},
		InitElements: props.InitElements,
	})
	kc.Gateway = gw

	// create Indexer instance (using first validator as dependency)
	idx := kwil_indexer.NewIndexerInstance(node, kwil_indexer.NewIndexerInstanceInput{
		Vpc:             props.Vpc,
		TNInstance:      props.Validators[0],
		IndexerDirAsset: props.Assets.Indexer.DirAsset,
		HostedDomain:    props.HostedDomain,
		InitElements:    props.InitElements,
	})
	kc.Indexer = idx

	// optional CloudFront for gateway/indexer
	if props.Cert != nil {
		cf := kwil_gateway.TNCloudfrontInstance(node, jsii.String("TNCloudfront"), kwil_gateway.TNCloudfrontConfig{
			DomainName:           props.HostedDomain.DomainName,
			KgwPublicDnsName:     gw.InstanceDnsName,
			IndexerPublicDnsName: idx.InstanceDnsName,
			HostedZone:           props.HostedDomain.Zone,
			Certificate:          props.Cert,
		})
		kc.Cdn = cf
	}

	return kc
}
