//go:build kwiltest

package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

// Stand-ins with the names of the nuon price feed actions that exist on
// mainnet but not in this repository: the three setters and one getter.
const nuonActionsSQL = `
CREATE OR REPLACE ACTION nuon_set_base_constant($value NUMERIC(36, 18)) PUBLIC {
  $v := $value;
};

CREATE OR REPLACE ACTION nuon_set_divisor_constant($value NUMERIC(36, 18)) PUBLIC {
  $v := $value;
};

CREATE OR REPLACE ACTION nuon_set_source_stream($data_provider TEXT, $stream_id TEXT) PUBLIC {
  $p := $data_provider;
};

CREATE OR REPLACE ACTION nuon_get_base_constant() PUBLIC VIEW RETURNS (value NUMERIC(36, 18)) {
  RETURN 1.000000000000000000::NUMERIC(36, 18);
};
`

// TestNuonSettersDropped applies the current migrations on top of a network
// that holds the nuon actions, as a mainnet deploy would.
func TestNuonSettersDropped(t *testing.T) {
	seeds := append([]string{nuonActionsSQL}, migrations.GetSeedScriptStatements()...)

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "NUON_SETTERS01_Dropped",
		SeedStatements: seeds,
		FunctionTests: []kwilTesting.TestFunc{
			testNuonSettersDropped(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testNuonSettersDropped(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		for _, name := range []string{"nuon_set_base_constant", "nuon_set_divisor_constant", "nuon_set_source_stream"} {
			require.False(t, nuonActionExists(ctx, t, platform, name), "%s should be dropped by the migrations", name)
		}

		// The getter is kept.
		require.True(t, nuonActionExists(ctx, t, platform, "nuon_get_base_constant"))

		return nil
	}
}

// nuonActionExists reports whether an action of that name exists in the main namespace.
func nuonActionExists(ctx context.Context, t *testing.T, platform *kwilTesting.Platform, name string) bool {
	found := false
	err := platform.Engine.ExecuteWithoutEngineCtx(ctx, platform.DB,
		`{info}SELECT name FROM actions WHERE namespace = 'main' AND name = $name`,
		map[string]any{"name": name},
		func(row *common.Row) error {
			found = true
			return nil
		})
	require.NoError(t, err)
	return found
}
