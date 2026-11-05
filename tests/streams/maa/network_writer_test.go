//go:build kwiltest

package maa

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// An agent wallet runs allow-listed actions for its agent key, so it never holds the
// system:network_writer role: grant_roles refuses to grant the role to a joined agent
// wallet, and maa_join refuses to register a wallet at an address that already holds it.

const (
	nwManagerHex = "0x3333333333333333333333333333333333333333" // member of system:network_writers_manager
	nwWriterHex  = "0x4444444444444444444444444444444444444444" // a plain wallet
)

func TestMAANetworkWriter(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "MAA_NetworkWriter",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testMAANetworkWriterGrantRefused(t),
			testMAANetworkWriterJoinRefused(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// isNetworkWriter reports whether wallet is a member of system:network_writer.
func isNetworkWriter(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, caller util.EthereumAddress, wallet string) bool {
	t.Helper()
	var member bool
	require.NoError(t, callAs(ctx, platform, caller, "are_members_of", []any{"system", "network_writer", []string{wallet}},
		func(row *common.Row) error { member = row.Values[1].(bool); return nil }))
	return member
}

// testMAANetworkWriterGrantRefused: a network_writers_manager cannot grant the role to a
// joined agent wallet, in any letter case and even beside a plain wallet; granting a plain
// wallet works as before.
func testMAANetworkWriterGrantRefused(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		manager := util.Unsafe_NewEthereumAddressFromString(nwManagerHex)
		platform.Deployer = restricted.Bytes()

		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaHex := "0x" + hex.EncodeToString(maa)

		require.NoError(t, setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writers_manager", nwManagerHex))

		// The agent wallet in upper case: grant_roles lowercases it, and still refuses.
		err := callAs(ctx, platform, manager, "grant_roles",
			[]any{"system", "network_writer", []string{"0x" + strings.ToUpper(hex.EncodeToString(maa))}}, nil)
		require.ErrorContains(t, err, "cannot be granted to an agent wallet")
		require.False(t, isNetworkWriter(t, ctx, platform, manager, maaHex), "the agent wallet must not hold network_writer")

		// Beside a plain wallet, the whole grant is refused: neither wallet gets the role.
		err = callAs(ctx, platform, manager, "grant_roles",
			[]any{"system", "network_writer", []string{nwWriterHex, maaHex}}, nil)
		require.ErrorContains(t, err, "cannot be granted to an agent wallet")
		require.False(t, isNetworkWriter(t, ctx, platform, manager, maaHex), "the agent wallet must not hold network_writer")
		require.False(t, isNetworkWriter(t, ctx, platform, manager, nwWriterHex), "a refused grant must grant nothing")

		// A plain wallet is granted as before.
		require.NoError(t, callAs(ctx, platform, manager, "grant_roles",
			[]any{"system", "network_writer", []string{nwWriterHex}}, nil))
		require.True(t, isNetworkWriter(t, ctx, platform, manager, nwWriterHex), "a plain wallet must still be granted")
		return nil
	}
}

// testMAANetworkWriterJoinRefused: the wallet address is known before the join, so the role
// could already sit on it. maa_join refuses to register the wallet until the role is revoked.
func testMAANetworkWriterJoinRefused(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		manager := util.Unsafe_NewEthereumAddressFromString(nwManagerHex)
		platform.Deployer = restricted.Bytes()

		// Golden vector A: this rule joined by `unrestricted` derives goldenMAAHex.
		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maaHex := "0x" + goldenMAAHex
		maaBytes, err := hex.DecodeString(goldenMAAHex)
		require.NoError(t, err)

		// The role sits on the address before anyone joins.
		require.NoError(t, setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writer", maaHex))

		err = callAs(ctx, platform, unrestricted, "maa_join", []any{ruleID}, nil)
		require.ErrorContains(t, err, "holds system:network_writer")
		var known bool
		require.NoError(t, callAs(ctx, platform, restricted, "maa_is_known", []any{maaBytes},
			func(row *common.Row) error { known = row.Values[0].(bool); return nil }))
		require.False(t, known, "a refused join must not register the agent wallet")

		// Once the role is revoked, the same join goes through and derives the same wallet.
		require.NoError(t, setup.AddMemberToRoleBypass(ctx, platform, "system", "network_writers_manager", nwManagerHex))
		require.NoError(t, callAs(ctx, platform, manager, "revoke_roles",
			[]any{"system", "network_writer", []string{maaHex}}, nil))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		require.Equal(t, maaBytes, maa)
		return nil
	}
}
