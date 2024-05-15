package kwil_network

import (
	"github.com/BurntSushi/toml"
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"os/exec"
	"strconv"
	"strings"
)

type NetworkConfigInput struct {
	KwilNetworkConfigAssetInput
	ConfigPath string
}

type NetworkConfigOutput struct {
	NodeConfigPaths []string
}

// GenerateNetworkConfig generates network configuration for a kwil network node. I.e.:
// kwil-admin setup testnet -v $NUMBER_OF_NODES --chain-id $CHAIN_ID -o $CONFIG_PATH
// this will generate a config directory for each node in the network indexed from 0
func GenerateNetworkConfig(input NetworkConfigInput) NetworkConfigOutput {
	nNodes := input.NumberOfNodes

	envVars := config.GetEnvironmentVariables()

	cmd := exec.Command(envVars.KwilAdminBinPath, "setup", "testnet",
		"-v", strconv.Itoa(nNodes),
		"--chain-id", envVars.ChainId,
		"-o", input.ConfigPath,
	)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}

	// create a list of node config paths
	// i.e. [<out_dir>/node0, <out_dir>/node1, ...]
	nodeConfigPaths := make([]string, nNodes)
	for i := 0; i < nNodes; i++ {
		nodeConfigPaths[i] = input.ConfigPath + "/node" + strconv.Itoa(i)
	}

	return NetworkConfigOutput{
		NodeConfigPaths: nodeConfigPaths,
	}
}

type KwilNetworkConfigAssetInput struct {
	NumberOfNodes int
}

type KwilNetworkConfig struct {
	Asset awss3assets.Asset
	Id    string
}

// NewKwilNetworkConfigAssets generates configuration S3 asset for a network kwil node
// It may be used as a init file mounted into EC2 instances
func NewKwilNetworkConfigAssets(scope constructs.Construct, input KwilNetworkConfigAssetInput) []KwilNetworkConfig {
	// create a temporary directory to store the generated network configuration
	tempDir := awscdk.FileSystem_Mkdtemp(jsii.String("kw-net-conf"))
	out := GenerateNetworkConfig(NetworkConfigInput{
		ConfigPath:                  *tempDir,
		KwilNetworkConfigAssetInput: input,
	})

	// create an S3 asset for each node config
	assets := make([]KwilNetworkConfig, len(out.NodeConfigPaths))
	for i, nodeConfigPath := range out.NodeConfigPaths {
		// read this node id from ${nodeConfigPath}/config.toml
		peerId := ExtractPeerIdFromConfigFile(nodeConfigPath + "/config.toml")

		assets[i].Asset = awss3assets.NewAsset(scope, jsii.String("KwilNetworkConfigAsset-"+strconv.Itoa(i)), &awss3assets.AssetProps{
			Path: jsii.String(nodeConfigPath),
		})
		assets[i].Id = peerId
	}

	return assets
}

type ConfigFileFields struct {
	Chain struct {
		P2P struct {
			// e.g. "tcp://0.0.0.0:26655"
			ListenAddr string `toml:"listen_addr"`
			// e.g. "0fe9dd945c659c5a062de731cdc4f26f1f7b492b@172.10.100.2:26656,f3b2b34fa7bddd972ee3875476322e023be7791e@172.10.100.3:26655"
			PersistentPeers string `toml:"persistent_peers"`
		} `toml:"p2p"`
	} `toml:"chain"`
}

// ExtractPeerIdFromConfigFile extracts the peer id from the config file
// It tries matching the port from the listen address with the port from the persistent peers to get the ID
func ExtractPeerIdFromConfigFile(filePath string) string {
	// read the config file
	config := ConfigFileFields{}
	// read the config file
	_, err := toml.DecodeFile(filePath, &config)
	if err != nil {
		panic(err)
	}

	// extract the port from the listen address, by splitting the string by ':'
	port := config.Chain.P2P.ListenAddr[strings.LastIndex(config.Chain.P2P.ListenAddr, ":")+1:]

	// extract the peer id from the persistent peers field, by locating the id with the same port
	peerIds := strings.Split(config.Chain.P2P.PersistentPeers, ",")

	// find the peer id with the same port
	var peerId string
	for _, peerIdWithAddr := range peerIds {
		if strings.HasSuffix(peerIdWithAddr, port) {
			peerId = strings.Split(peerIdWithAddr, "@")[0]
			break
		}
	}

	if peerId == "" {
		panic("peer id not found")
	}

	// return the peer id
	return peerId
}
