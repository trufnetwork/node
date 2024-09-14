package kwil_indexer_instance

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsec2"
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
	IndexerInstance      awsec2.Instance
	indexerZippedDirPath *string
}

func AddKwilIndexerStartupScriptsToInstance(options AddKwilIndexerStartupScriptsOptions) {
	tsnInstance := options.TSNInstance

	// Create the environment variables for the gateway compose file
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

	setupScript := `#!/bin/bash
set -e
set -x
`

	setupScript += utils.InstallDockerScript() + "\n"
	setupScript += utils.ConfigureDockerDataRoot("/data/docker") + "\n"

	setupScript += `# Extract the indexer files
unzip ` + *options.indexerZippedDirPath + ` -d /home/ec2-user/indexer

cat <<EOF > /etc/systemd/system/kwil-indexer.service
[Unit]
Description=Kwil Indexer Compose
Restart=on-failure

[Service]
type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -c "docker compose -f /home/ec2-user/indexer/indexer-compose.yaml up -d"
ExecStop=/bin/bash -c "docker compose -f /home/ec2-user/indexer/indexer-compose.yaml down"
` + utils.GetEnvStringsForService(utils.GetDictFromStruct(indexerEnvConfig)) + `


[Install]
WantedBy=multi-user.target

EOF

systemctl daemon-reload
systemctl enable kwil-indexer.service
systemctl start kwil-indexer.service
`

	options.IndexerInstance.AddUserData(jsii.String(setupScript))
}
