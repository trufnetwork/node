package kwil_network

import (
	"encoding/json"
	"fmt"
	"os"

	awscdk "github.com/aws/aws-cdk-go/awscdk/v2"
	awss3assets "github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	constructs "github.com/aws/constructs-go/constructs/v10"
	jsii "github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/config"
	domaincfg "github.com/trufnetwork/node/infra/config/domain"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
)

type NetworkConfigInput struct {
	KwilAutoNetworkConfigAssetInput
	ConfigPath string
}

type NetworkConfigOutput struct {
	NodeConfigPaths []string
}

type KwilAutoNetworkConfigAssetInput struct {
	NumberOfNodes int
	// If provided, the private keys will be used to extract the node info
	PrivateKeys     []string
	DbOwner         string
	GenesisFilePath string
	Params          config.CDKParams
}

type KwilNetworkConfig struct {
	Asset      awss3assets.Asset
	Connection peer.TNPeer
}

// KwilNetworkConfigAssetsFromNumberOfNodes generates peer information and the genesis file asset.
// It no longer generates individual node config files, as that's handled by templating.
func KwilNetworkConfigAssetsFromNumberOfNodes(scope constructs.Construct, input KwilAutoNetworkConfigAssetInput) ([]peer.TNPeer, []NodeKeys, awss3assets.Asset) {
	// Initialize CDK parameters and DomainConfig
	stage := config.GetStage(scope)
	devPrefix := config.GetDevPrefix(scope)
	stack, ok := scope.(awscdk.Stack)
	if !ok {
		panic(fmt.Sprintf("KwilNetworkConfigAssetsFromNumberOfNodes: expected scope to be awscdk.Stack, got %T", scope))
	}
	hd := domaincfg.NewHostedDomain(stack, "NetworkDomain", &domaincfg.HostedDomainProps{
		Spec: domaincfg.Spec{
			Stage:     stage,
			Sub:       "",
			DevPrefix: devPrefix,
		},
	})
	baseDomain := *hd.DomainName

	env := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	// --- Determine number of nodes ---
	numNodes := input.NumberOfNodes
	useProvidedKeys := len(input.PrivateKeys) > 0
	if useProvidedKeys {
		if numNodes > 0 && numNodes != len(input.PrivateKeys) {
			// If both NumberOfNodes and PrivateKeys are provided, their lengths must match
			panic(fmt.Sprintf("NumberOfNodes (%d) and the number of provided PrivateKeys (%d) must match if both are specified", numNodes, len(input.PrivateKeys)))
		}
		numNodes = len(input.PrivateKeys) // Set numNodes based on provided keys
		if numNodes == 0 {
			panic("PrivateKeys slice was provided but is empty")
		}
	} else if numNodes <= 0 {
		// If not using provided keys, NumberOfNodes must be positive
		panic("NumberOfNodes must be positive if PrivateKeys are not provided")
	}

	// Generate or Extract Node Keys and Peer Info
	nodeKeys := make([]NodeKeys, numNodes)
	peers := make([]peer.TNPeer, numNodes)
	for i := 0; i < numNodes; i++ {
		if useProvidedKeys {
			// Use provided private key to extract node info
			nodeKeys[i] = ExtractKeys(scope, input.PrivateKeys[i])
		} else {
			// Generate new keys
			nodeKeys[i] = GenerateNodeKeys(scope)
		}
		// Create peer info (same logic for both cases)
		peers[i] = peer.TNPeer{
			NodeId:         nodeKeys[i].NodeId,
			Address:        jsii.String(fmt.Sprintf("node-%d.%s", i+1, baseDomain)),
			NodeHexAddress: nodeKeys[i].PublicKeyHex,
		}
	}

	var genesisAsset awss3assets.Asset

	// Either generate a genesis file or use the provided one
	if input.GenesisFilePath != "" {
		// Verify the chain_id in the provided genesis file
		err := verifyGenesisChainID(input.GenesisFilePath, env.ChainId)
		if err != nil {
			// If verification fails, panic
			panic(fmt.Sprintf("genesis file verification failed: %v", err))
		}

		// Chain ID matches, proceed to create the asset
		genesisAsset = awss3assets.NewAsset(scope, jsii.String("GenesisFileAsset"), &awss3assets.AssetProps{
			Path: jsii.String(input.GenesisFilePath), // Path to the provided genesis.json
		})
	} else if input.DbOwner != "" {
		genesisFilePath := GenerateGenesisFile(scope, GenerateGenesisFileInput{
			ChainId:         env.ChainId,
			PeerConnections: peers, // Pass peers to include validators in genesis
			DbOwner:         input.DbOwner,
			NodeKeys:        nodeKeys, // Pass the generated nodeKeys
		})

		// Create Genesis Asset
		genesisAsset = awss3assets.NewAsset(scope, jsii.String("GenesisFileAsset"), &awss3assets.AssetProps{
			Path: jsii.String(genesisFilePath), // Path to the generated genesis.json
		})
	} else {
		panic("DbOwner or GenesisFilePath must be provided")
	}

	// Return the list of peers, the corresponding node keys, and the single genesis asset
	return peers, nodeKeys, genesisAsset
}

// verifyGenesisChainID reads a genesis file, parses it, and verifies its chain_id against the expected one.
func verifyGenesisChainID(genesisFilePath string, expectedChainID string) error {
	// Read the provided genesis file
	genesisFileContent, err := os.ReadFile(genesisFilePath)
	if err != nil {
		return fmt.Errorf("failed to read provided genesis file %s: %w", genesisFilePath, err)
	}

	// Unmarshal into a generic map
	var genesisData map[string]interface{}
	err = json.Unmarshal(genesisFileContent, &genesisData)
	if err != nil {
		return fmt.Errorf("failed to unmarshal provided genesis file %s: %w", genesisFilePath, err)
	}

	// Verify chain_id field existence
	chainIdFromFileRaw, ok := genesisData["chain_id"]
	if !ok {
		return fmt.Errorf("provided genesis file %s is missing 'chain_id' field", genesisFilePath)
	}

	// Verify chain_id field type
	chainIdFromFile, ok := chainIdFromFileRaw.(string)
	if !ok {
		return fmt.Errorf("provided genesis file %s has 'chain_id' field with unexpected type: %T", genesisFilePath, chainIdFromFileRaw)
	}

	// Compare chain_ids
	if chainIdFromFile != expectedChainID {
		return fmt.Errorf("provided genesis file %s has chain_id '%s' which does not match expected chain_id '%s'", genesisFilePath, chainIdFromFile, expectedChainID)
	}

	// Chain ID matches
	return nil
}
