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
	"github.com/trufnetwork/node/infra/lib/cdklogger"
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
	// NumberOfNodes, when set, must equal len(NodeKeys).
	NumberOfNodes int
	// NodeKeys are the validators' public keys in node order (entry 0 is node-1).
	NodeKeys        []NodeKeys
	DbOwner         string
	GenesisFilePath string
	Params          config.CDKParams
	BaseDomainFqdn  *string
}

type KwilNetworkConfig struct {
	Asset      awss3assets.Asset
	Connection peer.TNPeer
}

// KwilNetworkConfigAssetsFromNumberOfNodes builds peer information from the
// nodes' public keys and the genesis file asset. Node config files are
// rendered elsewhere, and private keys are never read here.
func KwilNetworkConfigAssetsFromNumberOfNodes(scope constructs.Construct, input KwilAutoNetworkConfigAssetInput) ([]peer.TNPeer, []NodeKeys, awss3assets.Asset) {
	// Initialize CDK parameters and DomainConfig
	var baseDomain string
	if input.BaseDomainFqdn != nil && *input.BaseDomainFqdn != "" {
		baseDomain = *input.BaseDomainFqdn
	} else {
		// Fallback to old behavior if BaseDomainFqdn is not provided (for backward compatibility or other stacks)
		stage := config.GetStage(scope)
		devPrefix := config.GetDevPrefix(scope)
		stack, ok := scope.(awscdk.Stack)
		if !ok {
			panic(fmt.Sprintf("KwilNetworkConfigAssetsFromNumberOfNodes: expected scope to be awscdk.Stack, got %T", scope))
		}
		hd := domaincfg.NewHostedDomain(stack, "NetworkDomainInternalLookup", &domaincfg.HostedDomainProps{
			Spec: domaincfg.Spec{
				Stage:     stage,
				Sub:       "",
				DevPrefix: devPrefix,
			},
		})
		baseDomain = *hd.DomainName
		cdklogger.LogWarning(scope, "", "KwilNetworkConfigAssetsFromNumberOfNodes is using an internal HostedDomain lookup. Consider passing BaseDomainFqdn for consistency.")
	}

	env := config.GetEnvironmentVariables[config.MainEnvironmentVariables](scope)

	// --- Determine number of nodes ---
	numNodes := len(input.NodeKeys)
	if numNodes == 0 {
		panic("at least one node public key is required")
	}
	if input.NumberOfNodes > 0 && input.NumberOfNodes != numNodes {
		panic(fmt.Sprintf("numOfNodes (%d) does not match the number of node public keys (%d)", input.NumberOfNodes, numNodes))
	}

	configGenConstructID := "KwilNetworkConfigGen" // For logging context

	// Peer info from the public keys. Hostnames follow the node index.
	peers := make([]peer.TNPeer, numNodes)
	for i, key := range input.NodeKeys {
		peers[i] = peer.TNPeer{
			NodeId:         key.NodeId(),
			Address:        jsii.String(fmt.Sprintf("node-%d.%s", i+1, baseDomain)),
			NodeHexAddress: key.PublicKeyHex,
		}
	}
	cdklogger.LogInfo(scope, configGenConstructID, "[NetCfg 1/2] Built peer info for %d nodes from their public keys. Base Domain: %s.", numNodes, baseDomain)

	var genesisAsset awss3assets.Asset
	genesisSource := ""

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
		genesisSource = fmt.Sprintf("from provided file: %s", input.GenesisFilePath)
	} else if input.DbOwner != "" {
		genesisSource = "dynamically generated"
		genesisFilePath := GenerateGenesisFile(scope, GenerateGenesisFileInput{
			ChainId:         env.ChainId,
			PeerConnections: peers, // Pass peers to include validators in genesis
			DbOwner:         input.DbOwner,
			NodeKeys:        input.NodeKeys,
		})

		// Create Genesis Asset
		genesisAsset = awss3assets.NewAsset(scope, jsii.String("GenesisFileAsset"), &awss3assets.AssetProps{
			Path: jsii.String(genesisFilePath), // Path to the generated genesis.json
		})
	} else {
		panic("DbOwner or GenesisFilePath must be provided")
	}

	assetPathToken := "[Not Available]"
	if genesisAsset.S3ObjectUrl() != nil {
		assetPathToken = *genesisAsset.S3ObjectUrl()
	}
	cdklogger.LogInfo(scope, configGenConstructID, "[NetCfg 2/2] Generating/Validating Genesis file (%s). ChainID: %s, DBOwner: %s. Output Asset (token): %s.", genesisSource, env.ChainId, input.DbOwner, assetPathToken)

	// Return the list of peers, the corresponding node keys, and the single genesis asset
	return peers, input.NodeKeys, genesisAsset
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
