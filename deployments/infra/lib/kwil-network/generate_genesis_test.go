//go:generate go test . -update
package kwil_network_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"sort"
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

// stripVolatiles removes or normalises fields that change on every run
// (e.g. timestamps).  We only keep the parts we care about – chain-id and
// validators – so the golden file stays stable but still proves correctness.
func stripVolatiles(raw []byte) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}

	out := map[string]any{
		"chain_id":   doc["chain_id"],
		"validators": doc["validators"],
		"db_owner":   doc["db_owner"],
	}

	// sort validators by name so snapshot diff is deterministic
	if vs, ok := out["validators"].([]any); ok {
		sort.Slice(vs, func(i, j int) bool {
			a := vs[i].(map[string]any)["name"].(string)
			b := vs[j].(map[string]any)["name"].(string)
			return a < b
		})
	}

	return json.MarshalIndent(out, "", "  ")
}

// -----------------------------------------------------------------------------
// Test case
// -----------------------------------------------------------------------------

func TestGenerateGenesisFile_Snapshot(t *testing.T) {
	kwildPath := findKwild(t)
	t.Setenv("KWILD_CLI_PATH", kwildPath) // used by config.GetEnvironmentVariables

	// ----------- Arrange -----------------------------------------------------
	scope := minimalConstruct()

	peers := []peer.TNPeer{
		{Address: jsii.String("1.2.3.4"), NodeHexAddress: "deadbeef", NodeId: "node0"},
		{Address: jsii.String("5.6.7.8"), NodeHexAddress: "cafebabe", NodeId: "node1"},
	}
	chainID := "unit-test-chain"

	// ----------- Act ---------------------------------------------------------
	outPath := kwilnet.GenerateGenesisFile(scope, kwilnet.GenerateGenesisFileInput{
		PeerConnections: peers,
		ChainId:         chainID,
		DbOwner:         "0x0000000000000000000000000000000000000000",
	})

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
