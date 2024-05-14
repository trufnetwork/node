package kwil_network

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/config"
	"os/exec"
	"strconv"
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

	// check if the kwil-admin binary is a token
	// if it is, resolve it
	kwilAdminBinPath := config.GetEnvironmentVariables().KwilAdminBinPath

	cmd := exec.Command(kwilAdminBinPath, "setup", "testnet",
		"-v", strconv.Itoa(nNodes),
		"--chain-id", *input.ChainId,
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
	ChainId       *string
}

// NewKwilNetworkConfigAssets generates configuration S3 asset for a network kwil node
// It may be used as a init file mounted into EC2 instances
func NewKwilNetworkConfigAssets(scope constructs.Construct, input KwilNetworkConfigAssetInput) []awss3assets.Asset {
	// create a temporary directory to store the generated network configuration
	tempDir := awscdk.FileSystem_Mkdtemp(jsii.String("kw-net-conf"))
	out := GenerateNetworkConfig(NetworkConfigInput{
		ConfigPath:                  *tempDir,
		KwilNetworkConfigAssetInput: input,
	})

	// create an S3 asset for each node config
	assets := make([]awss3assets.Asset, len(out.NodeConfigPaths))
	for i, nodeConfigPath := range out.NodeConfigPaths {
		assets[i] = awss3assets.NewAsset(scope, jsii.String("KwilNetworkConfigAsset-"+strconv.Itoa(i)), &awss3assets.AssetProps{
			Path: jsii.String(nodeConfigPath),
		})
	}

	return assets
}
