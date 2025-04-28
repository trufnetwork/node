package validator_set

import (
	"fmt"

	awscdk "github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/config/domain"
	kwil_network "github.com/trufnetwork/node/infra/lib/kwil-network"
	peer2 "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/tn"
)

// ValidatorSetProps holds inputs for creating a ValidatorSet
// all fields are required unless otherwise documented
// TODO adjust fields as needed based on spec
// TODO include Stage if necessary

type ValidatorSetProps struct {
	Vpc          awsec2.IVpc
	HostedDomain *domain.HostedDomain
	NodesConfig  []kwil_network.KwilNetworkConfig
	KeyPair      awsec2.IKeyPair
	Assets       TNAssets
	InitElements []awsec2.InitElement
}

// ValidatorSet is a reusable construct for TN validator nodes
type ValidatorSet struct {
	constructs.Construct

	Nodes         []tn.TNInstance
	Role          awsiam.IRole
	SecurityGroup awsec2.SecurityGroup
}

// NewValidatorSet provisions validator instances, EIPs, and DNS records
func NewValidatorSet(scope constructs.Construct, id string, props *ValidatorSetProps) *ValidatorSet {
	node := constructs.NewConstruct(scope, jsii.String(id))
	vs := &ValidatorSet{Construct: node}

	// handle KeyPair reuse or create default from context
	if props.KeyPair == nil {
		keyPairName := config.KeyPairName(node)
		if len(keyPairName) == 0 {
			panic("KeyPairName is empty")
		}
		props.KeyPair = awsec2.KeyPair_FromKeyPairName(node, jsii.String("DefaultKeyPair"), jsii.String(keyPairName))
	}

	// create IAM role for TN instances
	role := awsiam.NewRole(node, jsii.String("ValidatorSetRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	// create security group for TN instances
	sg := tn.NewTNSecurityGroup(node, tn.NewTNSecurityGroupInput{Vpc: props.Vpc})

	// collect peer connections
	n := len(props.NodesConfig)
	allPeers := make([]peer2.TNPeer, n)
	for i, cfg := range props.NodesConfig {
		allPeers[i] = cfg.Connection
	}

	// provision TN instances and record EIP and DNS
	instances := make([]tn.TNInstance, n)
	for i := 0; i < n; i++ {
		cfg := props.NodesConfig[i]
		inst := newNode(node, i, role, sg, props, cfg.Connection, allPeers)

		// allocate Elastic IP
		eip := awsec2.NewCfnEIP(node, jsii.String(fmt.Sprintf("PeerEIP-%d", i)), &awsec2.CfnEIPProps{})
		eip.Tags().SetTag(jsii.String("Name"), jsii.String(fmt.Sprintf("%s-PeerEIP-%d", *awscdk.Aws_STACK_NAME(), i)), jsii.Number(10), jsii.Bool(true))
		inst.ElasticIp = eip

		// create DNS A record
		awsroute53.NewARecord(node, jsii.String(fmt.Sprintf("PeerARecord-%d", i)), &awsroute53.ARecordProps{
			Zone:       props.HostedDomain.Zone,
			RecordName: cfg.Connection.Address,
			Target:     awsroute53.RecordTarget_FromIpAddresses(eip.AttrPublicIp()),
		})

		instances[i] = inst
	}

	// set outputs
	vs.Nodes = instances
	vs.Role = role
	vs.SecurityGroup = sg

	return vs
}
