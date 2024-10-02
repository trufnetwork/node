package kwil_indexer_instance

import (
	"fmt"
	"strconv"

	"github.com/aws/jsii-runtime-go"
	"github.com/truflation/tsn-db/infra/lib/kwil-network/peer"
	"github.com/truflation/tsn-db/infra/lib/tsn"
	"github.com/truflation/tsn-db/infra/lib/utils"
)

type IndexerEnvConfig struct {
	NodeCometBftEndpoint *string `env:"NODE_COMETBFT_ENDPOINT"`
	KwilPgConn           *string `env:"KWIL_PG_CONN"`
	PostgresVolume       *string `env:"POSTGRES_VOLUME"`
}

type AddKwilIndexerStartupScriptsOptions struct {
	TSNInstance          tsn.TSNInstance
	indexerZippedDirPath *string
}

func AddKwilIndexerStartupScripts(options AddKwilIndexerStartupScriptsOptions) *string {
	tsnInstance := options.TSNInstance

	// Create the environment variables for the indexer compose file
	indexerEnvConfig := IndexerEnvConfig{
		// note: the tsn p2p port (usually 26656) will be automatically crawled by the indexer
		NodeCometBftEndpoint: jsii.String(fmt.Sprintf(
			"http://%s:%s",
			// public ip so the external elastic ip is used to allow the indexer to connect to the TSN node
			*tsnInstance.PeerConnection.Address,
			strconv.Itoa(peer.TsnCometBFTRPCPort),
		)),
		// postgresql://kwild@<ip>:<psqlport>/kwild?sslmode=disable
		KwilPgConn: jsii.String(fmt.Sprintf(
			"postgresql://kwild@%s:%s/kwild?sslmode=disable",
			// public ip so the external elastic ip is used to allow the indexer to connect to the TSN node
			*tsnInstance.PeerConnection.Address,
			strconv.Itoa(peer.TSNPostgresPort),
		)),
		PostgresVolume: jsii.String("/data/postgres"),
	}

	script := "#!/bin/bash\nset -e\nset -x\n\n"
	script += utils.InstallDockerScript() + "\n"
	script += utils.ConfigureDocker(utils.ConfigureDockerInput{
		DataRoot: jsii.String("/data/docker"),
		// when we want to enable docker metrics on the hostr
		// MetricsAddr: jsii.String("127.0.0.1:9323"),
	}) + "\n"
	script += utils.UnzipFileScript(*options.indexerZippedDirPath, "/home/ec2-user/indexer") + "\n"
	script += utils.CreateSystemdServiceScript(
		"kwil-indexer",
		"Kwil Indexer Compose",
		"/bin/bash -c \"docker compose -f /home/ec2-user/indexer/indexer-compose.yaml up -d\"",
		"/bin/bash -c \"docker compose -f /home/ec2-user/indexer/indexer-compose.yaml down\"",
		utils.GetDictFromStruct(indexerEnvConfig),
	)

	return jsii.String(script)
}
