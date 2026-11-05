//go:generate go test . -update
package kwil_network_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/require"

	constructs "github.com/aws/constructs-go/constructs/v10"
	kwilnet "github.com/trufnetwork/node/infra/lib/kwil-network"
	peer "github.com/trufnetwork/node/infra/lib/kwil-network/peer"
)

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// findKwild makes sure the real kwild CLI is present and returns its path.
// The test is skipped automatically on machines where kwild is not installed.
func findKwild(t *testing.T) string {
	if p := os.Getenv("KWILD_CLI_PATH"); p != "" {
		return p
	}
	p, err := exec.LookPath("kwild")
	if err != nil {
		t.Skip("kwild binary not found in PATH and KWILD_CLI_PATH not set – skipping integration test")
	}
	return p
}

// minimalConstruct is the lightest Construct that still works with
// config.NewCDKParams inside GenerateGenesisFile.
func minimalConstruct() constructs.Construct {
	app := awscdk.NewApp(nil)
	stack := awscdk.NewStack(app, jsii.String("Dummy"), nil)
	return stack
}

// stripVolatiles keeps only the fields this code sets (chain id, db owner,
// validators and leader), so the golden file stays stable across kwild
// versions.
func stripVolatiles(raw []byte) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}

	out := map[string]any{
		"chain_id":   doc["chain_id"],
		"db_owner":   doc["db_owner"],
		"leader":     doc["leader"],
		"validators": doc["validators"],
	}

	return json.MarshalIndent(out, "", "  ")
}

// -----------------------------------------------------------------------------
// Test case
// -----------------------------------------------------------------------------

func TestGenerateGenesisFile_Snapshot(t *testing.T) {
	kwildPath := findKwild(t)
	// Placeholder values that config.GetEnvironmentVariables requires.
	t.Setenv("KWILD_CLI_PATH", kwildPath)
	t.Setenv("CDK_DOCKER", "docker")
	t.Setenv("CHAIN_ID", "unit-test-chain")

	// ----------- Arrange -----------------------------------------------------
	scope := minimalConstruct()

	// Placeholder public keys.
	nodeKeys := []kwilnet.NodeKeys{
		{KeyType: "secp256k1", PublicKeyHex: "02" + strings.Repeat("aa", 32)},
		{KeyType: "secp256k1", PublicKeyHex: "03" + strings.Repeat("bb", 32)},
	}
	peers := []peer.TNPeer{
		{Address: jsii.String("node-1.example.com"), NodeHexAddress: nodeKeys[0].PublicKeyHex, NodeId: nodeKeys[0].NodeId()},
		{Address: jsii.String("node-2.example.com"), NodeHexAddress: nodeKeys[1].PublicKeyHex, NodeId: nodeKeys[1].NodeId()},
	}
	chainID := "unit-test-chain"

	// ----------- Act ---------------------------------------------------------
	outPath := kwilnet.GenerateGenesisFile(scope, kwilnet.GenerateGenesisFileInput{
		PeerConnections: peers,
		ChainId:         chainID,
		DbOwner:         "0x0000000000000000000000000000000000000000",
		NodeKeys:        nodeKeys,
	})

	// kwild setup init writes a node key next to genesis.json. It must be gone.
	_, err := os.Stat(filepath.Join(filepath.Dir(outPath), "nodekey.json"))
	require.True(t, os.IsNotExist(err), "the unused node key from kwild setup init must be removed")

	raw, err := os.ReadFile(outPath)
	require.NoError(t, err, "generated genesis file must be readable")

	// ----------- Assert (goldie) --------------------------------------------
	clean, err := stripVolatiles(raw)
	require.NoError(t, err)

	goldie.New(t,
		goldie.WithFixtureDir("testdata"),
		goldie.WithNameSuffix(".golden"),
	).Assert(t, "genesis", clean)
}
