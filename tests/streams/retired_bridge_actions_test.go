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

// Stand-ins with the names and parameters of the bridge actions that #1162
// took out of erc20-bridge/001-actions.sql, as a network migrated before that
// change still holds them.
const retiredBridgeActionsSQL = `
CREATE OR REPLACE ACTION mainnet_admin_bridge_tokens($amount TEXT) PUBLIC {
  $num_amount := $amount::NUMERIC(78, 0);
};

CREATE OR REPLACE ACTION sepolia_admin_bridge_tokens($amount TEXT) PUBLIC {
  $num_amount := $amount::NUMERIC(78, 0);
};
`

// TestRetiredBridgeActionsDropped applies the current migrations on top of a
// network that still holds the retired bridge actions, as a deploy would.
func TestRetiredBridgeActionsDropped(t *testing.T) {
	seeds := append([]string{retiredBridgeActionsSQL}, migrations.GetSeedScriptStatements()...)

	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "RETIRED_BRIDGE_ACTIONS01_Dropped",
		SeedStatements: seeds,
		FunctionTests: []kwilTesting.TestFunc{
			testRetiredBridgeActionsDropped(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

func testRetiredBridgeActionsDropped(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		for _, name := range []string{"mainnet_admin_bridge_tokens", "sepolia_admin_bridge_tokens"} {
			require.False(t, mainActionExists(ctx, t, platform, name), "%s should be dropped by the migrations", name)
		}

		// The lookup does find actions the migrations keep.
		require.True(t, mainActionExists(ctx, t, platform, "change_bid"))

		return nil
	}
}

// mainActionExists reports whether an action of that name exists in the main namespace.
func mainActionExists(ctx context.Context, t *testing.T, platform *kwilTesting.Platform, name string) bool {
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
