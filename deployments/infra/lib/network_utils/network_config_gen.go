package network_utils

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"os/exec"
	"strconv"
	"strings"
)

type NetworkConfigInput struct {
	ChainId          string
	ConfigPath       string
	Hostnames        []string
	KwilAdminBinPath string
}

type NetworkConfigOutput struct {
	NodeConfigPaths []string
}

// GenerateNetworkConfig generates network configuration for a kwil network node. I.e.:
// kwil-admin setup testnet -v $NUMBER_OF_NODES --chain-id $CHAIN_ID -o $CONFIG_PATH --hostnames $HOSTNAMES
// this will generate a config directory for each node in the network indexed from 0
func GenerateNetworkConfig(input NetworkConfigInput) NetworkConfigOutput {
	nNodes := len(input.Hostnames)
	cmd := exec.Command(input.KwilAdminBinPath, "setup", "testnet",
		"-v", strconv.Itoa(nNodes),
		"--chain-id", input.ChainId,
		"-o", input.ConfigPath,
		"--hostnames", strings.Join(input.Hostnames, " "),
	)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}

	// create a list of node config paths
	// i.e. [<out_dir>/node0, <out_dir>/node1, ...]
	nodeConfigPaths := make([]string, nNodes)
	for i := 0; i < nNodes; i++ {
		nodeConfigPaths[i] = input.ConfigPath + "/" + strconv.Itoa(i)
	}

	return NetworkConfigOutput{
		NodeConfigPaths: nodeConfigPaths,
	}
}

type KwilNetworkConfigAssetInput struct {
	ChainId          string
	Hostnames        []string
	KwilAdminBinPath string
}

// NewKwilNetworkConfigAssets generates configuration S3 asset for a network kwil node
// It may be used as a init file mounted into EC2 instances
func NewKwilNetworkConfigAssets(scope constructs.Construct, input KwilNetworkConfigAssetInput) []awss3assets.Asset {
	// create a temporary directory to store the generated network configuration
	tempDir := awscdk.FileSystem_Mkdtemp(jsii.String("kw-net-conf"))
	out := GenerateNetworkConfig(NetworkConfigInput{
		ChainId:          input.ChainId,
		ConfigPath:       *tempDir,
		Hostnames:        input.Hostnames,
		KwilAdminBinPath: input.KwilAdminBinPath,
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
