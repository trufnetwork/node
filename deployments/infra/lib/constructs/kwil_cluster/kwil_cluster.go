package kwil_cluster

import (
	"github.com/aws/aws-cdk-go/awscdk/v2/awscertificatemanager"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/constructs/fronting"
	kwil_gateway "github.com/trufnetwork/node/infra/lib/kwil-gateway"
	kwil_indexer "github.com/trufnetwork/node/infra/lib/kwil-indexer"
	"github.com/trufnetwork/node/infra/lib/tn"
	"github.com/trufnetwork/node/infra/lib/utils"
)

// KwilClusterProps holds inputs for creating a KwilCluster
// Cert is optional; if nil, CloudFront distribution will be skipped
// Validators should come from a previous ValidatorSet
// InitElements are user-data steps to apply to both instances

type KwilClusterProps struct {
	Vpc                  awsec2.IVpc
	HostedDomain         *domaincfg.HostedDomain
	Cert                 awscertificatemanager.Certificate // optional
	CorsOrigins          *string
	SessionSecret        *string
	ChainId              *string
	Validators           []tn.TNInstance
	InitElements         []awsec2.InitElement
	Assets               KwilAssets
	SelectedFrontingKind fronting.Kind
}

// KwilCluster is a reusable construct for Gateway and Indexer without CloudFront
type KwilCluster struct {
	constructs.Construct

	Gateway kwil_gateway.KGWInstance
	Indexer kwil_indexer.IndexerInstance
}

// NewKwilCluster provisions the Kwil gateway, indexer, and optional CloudFront
func NewKwilCluster(scope constructs.Construct, id string, props *KwilClusterProps) *KwilCluster {
	node := constructs.NewConstruct(scope, jsii.String(id))
	kc := &KwilCluster{Construct: node}

	// Instantiate selected fronting type to get rules
	selectedFrontingImpl := fronting.New(props.SelectedFrontingKind)
	ingressRules := selectedFrontingImpl.IngressRules()

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
			// default to 1, because it is behind a TLS termination point
			XffTrustProxyCount: jsii.String("1"),
		},
		InitElements: props.InitElements,
	})
	kc.Gateway = gw
	// Apply ingress rules to Gateway SG
	utils.ApplyIngressRules(gw.SecurityGroup, ingressRules)

	// create Indexer instance
	idx := kwil_indexer.NewIndexerInstance(node, kwil_indexer.NewIndexerInstanceInput{
		Vpc:                props.Vpc,
		TNInstance:         props.Validators[0],
		IndexerDirAsset:    props.Assets.Indexer.DirAsset,
		IndexerBinaryAsset: props.Assets.Indexer.Binary,
		HostedDomain:       props.HostedDomain,
		InitElements:       props.InitElements,
	})
	kc.Indexer = idx
	// Apply ingress rules to Indexer SG
	utils.ApplyIngressRules(idx.SecurityGroup, ingressRules)

	return kc
}
