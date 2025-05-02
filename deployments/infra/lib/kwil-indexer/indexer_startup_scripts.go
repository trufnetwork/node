package kwil_indexer_instance

import (
	"fmt"
	"strconv"

	"github.com/aws/jsii-runtime-go"
	"github.com/trufnetwork/node/infra/lib/kwil-network/peer"
	"github.com/trufnetwork/node/infra/lib/tn"
	"github.com/trufnetwork/node/infra/lib/utils"
)

type IndexerEnvConfig struct {
	NodeRpcEndpoint   *string `env:"NODE_RPC_ENDPOINT"`
	PostgresVolume    *string `env:"POSTGRES_VOLUME"`
	IndexerBinaryPath *string `env:"INDEXER_BINARY_PATH"`
}

type AddKwilIndexerStartupScriptsOptions struct {
	TNInstance           tn.TNInstance
	indexerZippedDirPath *string
	indexerBinaryZipPath *string
}

func AddKwilIndexerStartupScripts(options AddKwilIndexerStartupScriptsOptions) *string {
	tnInstance := options.TNInstance
	indexerBinaryDir := "/home/ec2-user/indexer"
	indexerBinaryPath := fmt.Sprintf("%s/kwil-indexer", indexerBinaryDir)

	// Create the environment variables for the indexer compose file
	indexerEnvConfig := IndexerEnvConfig{
		// note: the tn p2p port (usually 26656) will be automatically crawled by the indexer
		NodeRpcEndpoint: jsii.String(fmt.Sprintf(
			"http://%s:%s",
			// public ip so the external elastic ip is used to allow the indexer to connect to the TN node
			*tnInstance.PeerConnection.Address,
			strconv.Itoa(peer.TnRPCPort),
		)),
		PostgresVolume:    jsii.String("/data/postgres"),
		IndexerBinaryPath: jsii.String(indexerBinaryPath),
	}

	script := "#!/bin/bash\nset -e\nset -x\n\n"
	installScript, err := utils.InstallDockerScript()
	if err != nil {
		panic(err)
	}
	script += installScript + "\n"
	configureScript, err := utils.ConfigureDocker(utils.ConfigureDockerInput{
		DataRoot: jsii.String("/data/docker"),
		// when we want to enable docker metrics on the hostr
		// MetricsAddr: jsii.String("127.0.0.1:9323"),
	})
	if err != nil {
		panic(err)
	}
	script += configureScript + "\n"
	script += utils.UnzipFileScript(*options.indexerZippedDirPath, "/home/ec2-user/indexer") + "\n"
	// Define the target directory for the binary

	script += fmt.Sprintf("mkdir -p %s\n", indexerBinaryDir)
	// Unzip the binary zip into the target directory
	script += utils.UnzipFileScript(*options.indexerBinaryZipPath, indexerBinaryDir) + "\n"
	// Ensure the binary (assuming name kwil-indexer) is executable
	script += fmt.Sprintf("chmod +x %s\n", indexerBinaryPath)

	// Assume indexer-compose.yaml will mount indexerBinaryDir/kwil-indexer to the correct path inside the container
	script += utils.CreateSystemdServiceScript(
		"kwil-indexer",
		"Kwil Indexer Compose",
		"/bin/bash -c \"docker compose -f /home/ec2-user/indexer/indexer-compose.yaml up -d\"",
		"/bin/bash -c \"docker compose -f /home/ec2-user/indexer/indexer-compose.yaml down\"",
		utils.GetDictFromStruct(indexerEnvConfig),
	)

	return jsii.String(script)
}
