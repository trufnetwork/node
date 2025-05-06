package validator_set

import (
	"fmt"

	awscdk "github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	"github.com/trufnetwork/node/infra/config"
	"github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
	kwilnetwork "github.com/trufnetwork/node/infra/lib/kwil-network"
	peer "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/tn"
)

// ValidatorSetProps holds inputs for creating a ValidatorSet
// all fields are required unless otherwise documented

type ValidatorSetProps struct {
	Vpc          awsec2.IVpc
	HostedDomain *domain.HostedDomain
	Peers        []peer.TNPeer
	NodeKeys     []kwilnetwork.NodeKeys
	GenesisAsset awss3assets.Asset
	KeyPair      awsec2.IKeyPair
	Assets       TNAssets
	InitElements []awsec2.InitElement
	CDKParams    config.CDKParams
}

// ValidatorSet is a reusable construct for TN validator nodes
type ValidatorSet struct {
	constructs.Construct

	Nodes         []tn.TNInstance
	Role          awsiam.IRole
	SecurityGroup awsec2.SecurityGroup
	GenesisAsset  awss3assets.Asset
}

// NewValidatorSet provisions validator instances, EIPs, and DNS records
func NewValidatorSet(scope constructs.Construct, id string, props *ValidatorSetProps) *ValidatorSet {
	nodeConstruct := constructs.NewConstruct(scope, jsii.String(id))
	vs := &ValidatorSet{Construct: nodeConstruct}

	// Use the Peers list directly
	allPeers := props.Peers // Use the provided peer list
	n := len(allPeers)
	cdklogger.LogInfo(nodeConstruct, "", "Initializing ValidatorSet for %d peers.", n)

	// handle KeyPair reuse or create default from context
	if props.KeyPair == nil {
		keyPairName := config.KeyPairName(nodeConstruct)
		if len(keyPairName) == 0 {
			panic("KeyPairName is empty")
		}
		props.KeyPair = awsec2.KeyPair_FromKeyPairName(nodeConstruct, jsii.String("DefaultKeyPair"), jsii.String(keyPairName))
	}

	// create IAM role for TN instances
	role := awsiam.NewRole(nodeConstruct, jsii.String("ValidatorSetRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("ec2.amazonaws.com"), nil),
	})

	// Grant pull access to the TN Docker image repository
	props.Assets.DockerImage.Repository().GrantPull(role)

	// Grant read access to the genesis asset bucket
	props.GenesisAsset.Bucket().GrantRead(role, nil)

	// create security group for TN instances
	sg := tn.NewTNSecurityGroup(nodeConstruct, tn.NewTNSecurityGroupInput{Vpc: props.Vpc})

	// provision TN instances and record EIP and DNS
	instances := make([]tn.TNInstance, n)
	for i := 0; i < n; i++ {
		peerInfo := allPeers[i]      // Get current peer
		nodeKey := props.NodeKeys[i] // Get corresponding node key data

		// The genesis asset details will be passed to tn_instance.go -> tn_startup_scripts.go
		inst := newNode(nodeConstruct, NewNodeInput{
			Index:         i,
			Role:          role,
			SG:            sg,
			Props:         props,
			Connection:    peerInfo,              // Pass public peer info
			PrivateKeyHex: nodeKey.PrivateKeyHex, // Pass private key hex
			KeyType:       nodeKey.KeyType,       // Pass key type
			AllPeers:      allPeers,
			GenisisAsset:  props.GenesisAsset,
		})

		// allocate Elastic IP
		eipConstructID := fmt.Sprintf("PeerEIP-%d", i)
		eip := awsec2.NewCfnEIP(nodeConstruct, jsii.String(eipConstructID), &awsec2.CfnEIPProps{})
		cdklogger.LogInfo(nodeConstruct, eipConstructID, "Allocated Elastic IP for Validator Node-%d: %s (Ref Token)", i, *eip.Ref())
		eip.Tags().SetTag(jsii.String("Name"), jsii.String(fmt.Sprintf("%s-PeerEIP-%d", *awscdk.Aws_STACK_NAME(), i)), jsii.Number(10), jsii.Bool(true))
		inst.ElasticIp = eip

		// create DNS A record
		awsroute53.NewARecord(nodeConstruct, jsii.String(fmt.Sprintf("PeerARecord-%d", i)), &awsroute53.ARecordProps{
			Zone:       props.HostedDomain.Zone,
			RecordName: peerInfo.Address, // Use peerInfo directly
			Target:     awsroute53.RecordTarget_FromIpAddresses(eip.AttrPublicIp()),
		})

		instances[i] = inst
	}

	// set outputs
	vs.Nodes = instances
	vs.Role = role
	vs.SecurityGroup = sg
	vs.GenesisAsset = props.GenesisAsset // Add genesis asset to outputs

	return vs
}
