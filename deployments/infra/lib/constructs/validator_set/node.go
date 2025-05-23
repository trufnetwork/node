package validator_set

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsroute53"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"

	nodeconfig "github.com/trufnetwork/node/infra/config/node"
	"github.com/trufnetwork/node/infra/lib/cdklogger"
	fronting "github.com/trufnetwork/node/infra/lib/constructs/fronting"
	kwil_network "github.com/trufnetwork/node/infra/lib/kwil-network"
	kwilnetworkpeer "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/tn"
	"github.com/trufnetwork/node/infra/lib/utils"
)

const kwildConfigTemplateFile = "kwild-config.tmpl"
const kwildConfigDir = "config/node"
const kwildConfigFilename = "config.toml"
const kwildGenesisPath = "/root/.kwild/genesis.json"

// nodeKeyJson represents the structure of the nodekey.json file kwild expects.
type nodeKeyJson struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

// populateAndRenderValues gathers config data, populates the Values struct, and renders the TOML template.
func populateAndRenderValues(scope constructs.Construct, index int, props *ValidatorSetProps, connection kwilnetworkpeer.TNPeer, allPeers []kwilnetworkpeer.TNPeer, allNodeKeys []kwil_network.NodeKeys) *bytes.Buffer {
	// --- 1. Gather dynamic data ---
	// Build bootnodes list: <hex_pubkey>#<key_type>@<hostname:port>
	bootnodes := make([]string, 0, len(allPeers)-1)
	for i, p := range allPeers {
		if i == index {
			continue // Skip self
		}
		// Ensure we have a corresponding key (should always be true if lengths match)
		if i >= len(allNodeKeys) {
			panic(fmt.Sprintf("Index %d out of bounds for allNodeKeys (length %d) while building bootnodes", i, len(allNodeKeys)))
		}
		peerKeyType := allNodeKeys[i].KeyType
		// Construct hostname:port string directly using known values
		hostnamePort := fmt.Sprintf("%s:%d", *p.Address, kwilnetworkpeer.TnP2pPort)
		// Use the peer's KeyType and constructed hostnamePort string
		bootnodes = append(bootnodes, fmt.Sprintf("%s#%s@%s", p.NodeHexAddress, peerKeyType, hostnamePort))
	}

	// Get node-specific external address (using hostname:port)
	externalAddr := fmt.Sprintf("%s:%d", *connection.Address, kwilnetworkpeer.TnP2pPort)

	// Fetch other dynamic config if needed (e.g., DB creds from Secrets Manager)
	// dbUser := ...
	// dbPass := ...

	// --- 2. Populate Values struct starting with defaults ---
	values := nodeconfig.NewDefaultValues()

	// Override necessary fields with dynamic/node-specific values
	values.Genesis.Path = kwildGenesisPath            // Set the standard path
	values.P2P.Bootnodes = bootnodes                  // Set calculated bootnodes
	values.P2P.External = externalAddr                // Set node-specific external address (hostname:port)
	values.P2P.ListenPort = kwilnetworkpeer.TnP2pPort // Ensure correct P2P port (though default matches)
	values.RPC.Port = kwilnetworkpeer.TnRPCPort       // Ensure correct RPC port (though default matches)
	values.DB.Port = kwilnetworkpeer.TnPostgresPort   // Ensure correct DB port (though default matches)

	// Example: Override DB credentials if fetched dynamically
	// values.DB.User = dbUser
	// values.DB.Pass = dbPass

	// --- 3. Render Template ---
	// Use GetProjectRootDir which should reliably find the repo root containing deployments/
	rootDir := utils.GetProjectRootDir()
	templatePath := filepath.Join(rootDir, "deployments", "infra", kwildConfigDir, kwildConfigTemplateFile)

	tmpl, err := template.New(filepath.Base(templatePath)).
		Funcs(sprig.TxtFuncMap()).
		ParseFiles(templatePath)
	if err != nil {
		panic(fmt.Errorf("failed to parse template %s: %w", templatePath, err))
	}

	var renderedConfig bytes.Buffer
	err = tmpl.Execute(&renderedConfig, values)
	if err != nil {
		panic(fmt.Errorf("failed to execute template %s with values %+v: %w", templatePath, values, err))
	}

	return &renderedConfig
}

type NewNodeInput struct {
	Index         int
	Role          awsiam.IRole
	SG            awsec2.SecurityGroup
	Props         *ValidatorSetProps
	Connection    kwilnetworkpeer.TNPeer
	PrivateKeyHex string
	KeyType       string
	AllPeers      []kwilnetworkpeer.TNPeer
	GenisisAsset  awss3assets.Asset
}

// newNode builds a single TNInstance using the shared role and security group
func newNode(
	scope constructs.Construct,
	input NewNodeInput,
) tn.TNInstance {
	// Populate values and render the config template
	renderedConfig := populateAndRenderValues(scope, input.Index, input.Props, input.Connection, input.AllPeers, input.Props.NodeKeys)

	assetConstructID := fmt.Sprintf("KwildRenderedConfigAsset-%d", input.Index)
	// Create an S3 asset from the rendered TOML content
	nodeConfigAsset := awss3assets.NewAsset(scope, jsii.String(assetConstructID), &awss3assets.AssetProps{
		Path: utils.WriteToTempFile(scope, fmt.Sprintf("rendered-config-%d.toml", input.Index), renderedConfig.Bytes()),
	})

	cdklogger.LogInfo(scope, assetConstructID, "Created S3 asset for Node-%d rendered kwild config. AssetPath (token): %s", input.Index, *nodeConfigAsset.S3ObjectUrl())

	// Grant the EC2 instance role read access to the asset bucket
	nodeConfigAsset.Bucket().GrantRead(input.Role, nil)

	// 1. Prepare the data structure for nodekey.json
	nodeKeyData := nodeKeyJson{
		Key:  input.PrivateKeyHex,
		Type: input.KeyType,
	}

	// 2. Marshal the data to JSON bytes
	nodeKeyJsonBytes, err := json.Marshal(nodeKeyData)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal nodekey.json for node %d: %v", input.Index, err))
	}

	// 3. Write the JSON bytes to a temporary file
	nodeKeyTempFile := utils.WriteToTempFile(scope, fmt.Sprintf("nodekey-%d.json", input.Index), nodeKeyJsonBytes)

	// 4. Create an S3 asset from the temporary JSON file
	nodeKeyJsonAsset := awss3assets.NewAsset(scope, jsii.String(fmt.Sprintf("NodeKeyJsonAsset-%d", input.Index)), &awss3assets.AssetProps{
		Path: nodeKeyTempFile,
	})

	// 5. Grant the EC2 instance role read access to the nodekey.json asset bucket
	nodeKeyJsonAsset.Bucket().GrantRead(input.Role, nil) // Use the same role as other assets

	// Build the TNInstance, passing the *rendered* config asset details
	return tn.NewTNInstance(scope, tn.NewTNInstanceInput{
		Index:                input.Index,
		Id:                   strconv.Itoa(input.Index),
		Role:                 input.Role,
		Vpc:                  input.Props.Vpc,
		SecurityGroup:        input.SG,
		TNDockerComposeAsset: input.Props.Assets.DockerCompose,
		TNDockerImageAsset:   input.Props.Assets.DockerImage,
		// Pass the Asset for rendered config and genesis file
		RenderedConfigAsset: nodeConfigAsset,
		GenesisAsset:        input.GenisisAsset,
		NodeKeyJsonAsset:    nodeKeyJsonAsset,
		TNConfigImageAsset:  input.Props.Assets.ConfigImage,
		InitElements:        input.Props.InitElements,
		PeerConnection:      input.Connection,
		AllPeerConnections:  input.AllPeers,
		KeyPair:             input.Props.KeyPair,
	})
}

// NodeTarget provides an implementation of the fronting.DnsTarget interface
// specifically for a validator node, using its Elastic IP address as the target.
type NodeTarget struct {
	// IpAddress holds the Elastic IP address associated with the node instance.
	IpAddress *string
	// PrimaryAddr stores the node's primary, internal FQDN (e.g., node-1.dev.infra.truf.network).
	PrimaryAddr *string
}

// RecordTarget implements the fronting.DnsTarget interface.
// It returns an awsroute53.RecordTarget configured for an IP address list,
// containing only the node's Elastic IP address.
func (nt *NodeTarget) RecordTarget() awsroute53.RecordTarget {
	if nt.IpAddress == nil || *nt.IpAddress == "" {
		// This indicates an internal error or missing EIP association.
		panic("NodeTarget IpAddress is nil or empty; cannot create RecordTarget for IP.")
	}
	// RecordTarget_FromIpAddresses accepts one or more IP addresses.
	return awsroute53.RecordTarget_FromIpAddresses(nt.IpAddress)
}

// PrimaryFQDN implements the fronting.DnsTarget interface.
// It returns the node's primary, internal FQDN.
func (nt *NodeTarget) PrimaryFQDN() *string {
	return nt.PrimaryAddr
}

// Ensures NodeTarget satisfies the DnsTarget interface at compile time.
var _ fronting.DnsTarget = (*NodeTarget)(nil)
