//go:build kwiltest

package maa

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	erc20bridge "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/sdk-go/core/util"
)

// This file is the end-to-end integration of the agent-wallet withdrawal, now possible
// because node's kwil-db pin carries the full MAA stack (the MAAExec route, the
// TxContext.MAARestricted flag, and the erc20 token boundary that enforces it).
//
// Driving the literal maaExecRoute.Execute would need a full TxApp (signer, accounts,
// validators, gas/nonce) that the kwiltest Platform does not stand up, and the route type is
// unexported. So the test reproduces exactly the child execution context the route builds
// for its inner call (kwil-db node/txapp/maa_route.go:186-189): @caller rewritten to the MAA
// address, and TxContext.MAARestricted = (role == "restricted"). With that, it exercises the
// REAL pieces the route integrates — the 048 store getters that resolve the role, the 049
// withdrawal actions that move the money, and the #3 erc20 MAARestricted boundary on REAL
// balances. The route's own gate / role-resolution / caller-rewrite branch logic is unit
// tested upstream in kwil-db (node/txapp/maa_route_test.go).

func TestMAAWithdrawRouteE2E(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:           "MAA_Withdraw_RouteE2E",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testMAAWithdrawE2E(t),
		},
	}, testutils.GetTestOptionsWithCache())
}

// callAsMAA invokes an action exactly as the maa_exec route's inner call does: @caller is the
// MAA address and TxContext.MAARestricted carries the signer's role (true for the restricted
// agent, false for the unrestricted owner). The flag threads by pointer to any call depth, so
// the erc20 token boundary sees it inside the nested transfer/bridge primitives.
func callAsMAA(ctx context.Context, platform *kwilTesting.Platform, maa util.EthereumAddress, restricted bool, action string, args []any) error {
	tx := &common.TxContext{
		Ctx:           ctx,
		BlockContext:  &common.BlockContext{Height: 1},
		Signer:        maa.Bytes(),
		Caller:        maa.Address(),
		TxID:          platform.Txid(),
		MAARestricted: restricted,
	}
	res, err := platform.Engine.Call(&common.EngineContext{TxContext: tx}, platform.DB, "", action, args, func(*common.Row) error { return nil })
	if err != nil {
		return err
	}
	return res.Error
}

func testMAAWithdrawE2E(t *testing.T) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		restricted := util.Unsafe_NewEthereumAddressFromString(restrictedHex)
		unrestricted := util.Unsafe_NewEthereumAddressFromString(unrestrictedHex)
		platform.Deployer = restricted.Bytes()

		// A rule whose allow-list is ONLY order-book actions — maa_withdraw is deliberately
		// NOT allow-listed, mirroring the real design: the owner's exit is a route privilege,
		// not an allow-list grant. 250 bps commission; 100 USDC funded into the MAA.
		require.NoError(t, erc20bridge.ForTestingInitializeExtension(ctx, platform))
		ruleID := createDefaultRule(t, ctx, platform, restricted, 250, repeat(0xab, 32))
		maa := joinRule(t, ctx, platform, unrestricted, ruleID)
		maaAddr := addrFromBytes(maa)
		orderedsync.ForTestingReset()
		fundAddress(t, ctx, platform, maaAddr.Address(), "100000000000000000000", 1)

		// 1) Role resolution source of truth: the route reads maa_get_instance to decide the
		//    signer's role. Assert it maps the MAA to both component keys (the data the
		//    route's raw-byte role compare uses).
		var instRestricted, instUnrestricted string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_instance", []any{maa},
			func(row *common.Row) error {
				instRestricted = row.Values[2].(string)
				instUnrestricted = row.Values[3].(string)
				return nil
			}))
		require.Equal(t, restrictedHex, instRestricted, "the route resolves the agent (restricted) from the store")
		require.Equal(t, unrestrictedHex, instUnrestricted, "the route resolves the owner (unrestricted) from the store")

		// 2) maa_withdraw is NOT in the allow-list — so the owner reaching it below is the
		//    route's owner-exit bypass, not an allow-list grant.
		var allowed []string
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_allowed_actions", []any{ruleID},
			func(row *common.Row) error { allowed = append(allowed, row.Values[1].(string)); return nil }))
		require.NotContains(t, allowed, "maa_withdraw", "the exit must NOT be allow-listed (it is a route privilege)")

		// 3) RESTRICTED agent path. The route rejects this at its gate before re-entry, but
		//    this proves the HARD backstop: even if the agent reached maa_withdraw with
		//    MAARestricted set, the erc20 token boundary blocks the (commission) transfer leg
		//    and nothing moves. The guard fires before any state mutation, so balances are
		//    untouched without needing a rollback.
		err := callAsMAA(ctx, platform, maaAddr, true /* restricted */, "maa_withdraw",
			[]any{wdBridge, dec(t, "100000000000000000000")})
		require.Error(t, err, "a restricted agent must not be able to withdraw")
		require.ErrorContains(t, err, "restricted agent (MAA) execution",
			"the withdrawal must be stopped by the erc20 token boundary, not some incidental error")
		require.Equal(t, "100000000000000000000", balance(t, ctx, platform, maaAddr.Address()), "the agent's blocked withdrawal must move nothing")
		require.Equal(t, "0", balance(t, ctx, platform, restrictedHex), "no commission was paid on the blocked attempt")

		// 4) UNRESTRICTED owner path. The same inner call with MAARestricted=false (as the
		//    route sets it for the owner) runs end to end: commission -> agent, payout ->
		//    owner, MAA drained, audit event recorded.
		err = callAsMAA(ctx, platform, maaAddr, false /* unrestricted owner */, "maa_withdraw",
			[]any{wdBridge, dec(t, "100000000000000000000")})
		require.NoError(t, err, "the owner's withdrawal must succeed end to end")
		require.Equal(t, "2500000000000000000", balance(t, ctx, platform, restrictedHex), "agent earns the 2.5% commission")
		require.Equal(t, "97500000000000000000", balance(t, ctx, platform, unrestrictedHex), "owner receives the remainder")
		require.Equal(t, "0", balance(t, ctx, platform, maaAddr.Address()), "the agent wallet is drained")

		var sawWithdraw bool
		require.NoError(t, callAs(ctx, platform, restricted, "maa_get_events", []any{ruleID, int64(100), int64(0)},
			func(row *common.Row) error {
				if row.Values[2].(string) == "WITHDRAW" {
					sawWithdraw = true
				}
				return nil
			}))
		require.True(t, sawWithdraw, "the successful withdrawal records a WITHDRAW audit event")
		return nil
	}
}
